/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.util.history;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.worker.TaskHistory;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * History directory structure
 *   tajo.query-history.path: hdfs
 *   tajo.task-history.path: local or hdfs
 *
 *   <tajo.history.query.dir>/<yyyyMMdd>/query-list/query-list-<HHmmss>.hist (TajoMaster's query list, hourly rolling)
 *                                      /query-detail/<QUERY_ID>/query.hist    (QueryMaster's query detail)
 *                                                               /<EB_ID>.hist  (QueryMaster's stage detail)
 *   <tajo.history.task.dir>/<yyyyMMdd>/tasks/<WORKER_HOST>_<WORKER_PORT>/<WORKER_HOST>_<WORKER_PORT>_<HH>_<seq>.hist
 * History files are kept for "tajo.history.expiry-time-day" (default value is 7 days)
 */
public class HistoryWriter extends AbstractService {
  private static final Log LOG = LogFactory.getLog(HistoryWriter.class);
  public static final String QUERY_LIST = "query-list";
  public static final String QUERY_DETAIL = "query-detail";
  public static final String HISTORY_FILE_POSTFIX = ".hist";

  private final LinkedBlockingQueue<WriterFuture<WriterHolder>>
      historyQueue = new LinkedBlockingQueue<WriterFuture<WriterHolder>>();
  // key: yyyyMMddHH
  private Map<String, WriterHolder> taskWriters = new HashMap<String, WriterHolder>();

  // For TajoMaster's query list
  private WriterHolder querySummaryWriter = null;

  private WriterThread writerThread;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private Path historyParentPath;
  private Path taskHistoryParentPath;
  private String processName;
  private TajoConf tajoConf;
  private HistoryCleaner historyCleaner;
  private boolean isMaster;
  private short queryReplication;
  private short taskReplication;

  public HistoryWriter(String processName, boolean isMaster) {
    super(HistoryWriter.class.getName() + ":" + processName);
    this.processName = processName.replaceAll(":", "_").toLowerCase();
    this.isMaster = isMaster;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("conf should be a TajoConf type.");
    }
    tajoConf = (TajoConf)conf;
    historyParentPath = tajoConf.getQueryHistoryDir(tajoConf);
    taskHistoryParentPath = tajoConf.getTaskHistoryDir(tajoConf);
    writerThread = new WriterThread();
    historyCleaner = new HistoryCleaner(tajoConf, isMaster);
    queryReplication = (short) tajoConf.getIntVar(TajoConf.ConfVars.HISTORY_QUERY_REPLICATION);
    taskReplication = (short) tajoConf.getIntVar(TajoConf.ConfVars.HISTORY_TASK_REPLICATION);
    super.serviceInit(conf);
  }

  @Override
  public void serviceStop() throws Exception {
    if(stopped.getAndSet(true)){
      return;
    }

    for (WriterHolder eachWriter : taskWriters.values()) {
      IOUtils.cleanup(LOG, eachWriter);
    }

    taskWriters.clear();
    writerThread.interrupt();

    IOUtils.cleanup(LOG, querySummaryWriter);

    if (historyCleaner != null) {
      historyCleaner.doStop();
    }
    super.serviceStop();
  }

  @Override
  public void serviceStart() throws Exception {
    writerThread.start();
    historyCleaner.start();
  }

  /* asynchronously append to history file */
  public WriterFuture<WriterHolder> appendHistory(History history) {
    WriterFuture<WriterHolder> future = new WriterFuture<WriterHolder>(history);
    historyQueue.add(future);
    return future;
  }

  /* asynchronously flush to history file */
  public WriterFuture<WriterHolder> appendAndFlush(History history) {
    WriterFuture<WriterHolder> future = new WriterFuture<WriterHolder>(history) {
      public void done(WriterHolder holder) {
        try {
          if (holder != null) holder.flush();
          super.done(holder);
        } catch (IOException e) {
          super.failed(e);
        }
      }
    };
    historyQueue.add(future);
    synchronized (writerThread) {
      writerThread.notifyAll();
    }
    return future;
  }

  /* synchronously flush to history file */
  public synchronized void appendAndSync(History history)
      throws TimeoutException, InterruptedException, IOException {

    WriterFuture<WriterHolder> future = appendAndFlush(history);

    future.get(5, TimeUnit.SECONDS);
    if(!future.isSucceed()){
      throw new IOException(future.getError());
    }
  }

  /* Flushing the buffer */
  public void flushTaskHistories() {
    if (historyQueue.size() > 0) {
      synchronized (writerThread) {
        writerThread.needTaskFlush.set(true);
        writerThread.notifyAll();
      }
    } else {
      writerThread.flushTaskHistories();
    }
  }

  public static FileSystem getNonCrcFileSystem(Path path, Configuration conf) throws IOException {
    // https://issues.apache.org/jira/browse/HADOOP-7844
    // If FileSystem is a local and CheckSumFileSystem, flushing doesn't touch file length.
    // So HistoryReader can't read until closing the file.
    FileSystem fs = path.getFileSystem(conf);
    if (path.toUri().getScheme().equals("file")) {
      fs.setWriteChecksum(false);
    }

    return fs;
  }

  public static Path getQueryHistoryFilePath(Path historyParentPath, String queryId, long startTime) {
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

    Path datePath = new Path(historyParentPath, df.format(startTime) + "/" + QUERY_DETAIL);
    return new Path(datePath, queryId + "/query" + HISTORY_FILE_POSTFIX);
  }

  public static Path getQueryHistoryFilePath(Path historyParentPath, String queryId) {
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

    Path datePath;
    try {
      String[] tokens = queryId.split("_");
      //q_1412483083972_0005 = q_<timestamp>_<seq>
      if (tokens.length == 3) {
        datePath = new Path(historyParentPath, df.format(tokens[1]) + "/" + QUERY_DETAIL);
      } else {
        datePath = new Path(historyParentPath, df.format(new Date(System.currentTimeMillis())) + "/" + QUERY_DETAIL);
      }
    } catch (Exception e) {
      datePath = new Path(historyParentPath, df.format(new Date(System.currentTimeMillis())) + "/" + QUERY_DETAIL);
    }
    return new Path(datePath, queryId + "/query" + HISTORY_FILE_POSTFIX);
  }

  class WriterThread extends Thread {
    private AtomicBoolean needTaskFlush = new AtomicBoolean(false);

    public void run() {
      LOG.info("HistoryWriter_" + processName + " started.");
      SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
      while (!stopped.get() && !Thread.interrupted()) {
        List<WriterFuture<WriterHolder>> histories = Lists.newArrayList();

        try {
          drainHistory(histories, 100, 1000);
        } catch (InterruptedException e) {
          if (stopped.get()) break;
        }

        try {
          if (!histories.isEmpty()) {
            writeHistory(histories);
          } else {
            continue;
          }
        } catch (Throwable e) {
          LOG.error(e.getMessage(), e);
        }

        //clean up history file

        // closing previous writer
        synchronized (taskWriters) {
          Calendar cal = Calendar.getInstance();
          cal.add(Calendar.HOUR_OF_DAY, -2);
          String closeTargetTime = df.format(cal.getTime());
          List<String> closingTargets = new ArrayList<String>();

          for (String eachWriterTime : taskWriters.keySet()) {
            if (eachWriterTime.compareTo(closeTargetTime) <= 0) {
              closingTargets.add(eachWriterTime);
            }
          }

          for (String eachWriterTime : closingTargets) {
            WriterHolder writerHolder;
            writerHolder = taskWriters.remove(eachWriterTime);
            if (writerHolder != null) {
              LOG.info("Closing task history file: " + writerHolder.path);
              IOUtils.cleanup(LOG, writerHolder);
            }
          }
        }
      }
      LOG.info("HistoryWriter_" + processName + " stopped.");
    }

    private int drainHistory(Collection<WriterFuture<WriterHolder>> buffer, int numElements,
                             long timeoutMillis) throws InterruptedException {

      long deadline = System.currentTimeMillis() + timeoutMillis;
      int added = 0;
      while (added < numElements) {
        added += historyQueue.drainTo(buffer, numElements - added);
        if (added < numElements) { // not enough elements immediately available; will have to wait
          if (deadline <= System.currentTimeMillis()) {
            break;
          } else {
            synchronized (writerThread) {
              writerThread.wait(deadline - System.currentTimeMillis());
              if (deadline > System.currentTimeMillis()) {
                added += historyQueue.drainTo(buffer, numElements - added);
                break;
              }
            }
          }
        }
      }
      return added;
    }

    private List<WriterFuture<WriterHolder>> writeHistory(List<WriterFuture<WriterHolder>> histories) {

      if (histories.isEmpty()) {
        return histories;
      }

      for (WriterFuture<WriterHolder> future : histories) {
        History history = future.getHistory();
        switch (history.getHistoryType()) {
          case TASK:
            try {
              future.done(writeTaskHistory((TaskHistory) history));
            } catch (Throwable e) {
              LOG.error("Error while saving task history: " +
                  ((TaskHistory) history).getTaskAttemptId() + ":" + e.getMessage(), e);
              future.failed(e);
            }
            break;
          case QUERY:
            try {
              writeQueryHistory((QueryHistory) history);
              future.done(null);
            } catch (Throwable e) {
              LOG.error("Error while saving query history: " +
                  ((QueryHistory) history).getQueryId() + ":" + e.getMessage(), e);
              future.failed(e);
            }
            break;
          case QUERY_SUMMARY:
            try {
              future.done(writeQuerySummary((QueryInfo) history));
            } catch (Throwable e) {
              LOG.error("Error while saving query summary: " +
                  ((QueryInfo) history).getQueryId() + ":" + e.getMessage(), e);
              future.failed(e);
            }
            break;
          default:
            LOG.warn("Wrong history type: " + history.getHistoryType());
        }
      }

      if(needTaskFlush.getAndSet(false)){
        flushTaskHistories();
      }
      return histories;
    }

    private void writeQueryHistory(QueryHistory queryHistory) throws Exception {
      // QueryMaster's query detail history (json format)
      // <tajo.query-history.path>/<yyyyMMdd>/query-detail/<QUERY_ID>/query.hist

      // QueryMaster's stage detail history(proto binary format)
      // <tajo.query-history.path>/<yyyyMMdd>/query-detail/<QUERY_ID>/<EB_ID>.hist

      Path queryHistoryFile = getQueryHistoryFilePath(historyParentPath, queryHistory.getQueryId());
      FileSystem fs = getNonCrcFileSystem(queryHistoryFile, tajoConf);

      if (!fs.exists(queryHistoryFile.getParent())) {
        if (!fs.mkdirs(queryHistoryFile.getParent())) {
          LOG.error("Can't make QueryHistory dir: " + queryHistoryFile.getParent());
          return;
        }
      }

      FSDataOutputStream out = null;
      try {
        LOG.info("Saving query summary: " + queryHistoryFile);
        out = fs.create(queryHistoryFile, queryReplication);
        out.write(queryHistory.toJson().getBytes(Bytes.UTF8_CHARSET));
      } finally {
        IOUtils.cleanup(LOG, out);
      }

      if (queryHistory.getStageHistories() != null) {
        for (StageHistory stageHistory : queryHistory.getStageHistories()) {
          Path path = new Path(queryHistoryFile.getParent(), stageHistory.getExecutionBlockId() + HISTORY_FILE_POSTFIX);
          out = null;
          try {
            out = fs.create(path, queryReplication);
            out.write(stageHistory.toTasksJson().getBytes(Bytes.UTF8_CHARSET));
            LOG.info("Saving query unit: " + path);
          } finally {
            IOUtils.cleanup(LOG, out);
          }
        }
      }
    }

    private WriterHolder writeQuerySummary(QueryInfo queryInfo) throws Exception {
      if(stopped.get()) return null;

        // writing to HDFS and rolling hourly
      if (querySummaryWriter == null) {
        querySummaryWriter = new WriterHolder();
        rollingQuerySummaryWriter();
      } else {
        if (querySummaryWriter.out == null) {
          rollingQuerySummaryWriter();
        } else if (System.currentTimeMillis() - querySummaryWriter.lastWritingTime >= 60 * 60 * 1000) {
          LOG.info("Close query history file: " + querySummaryWriter.path);
          IOUtils.cleanup(LOG, querySummaryWriter);
          rollingQuerySummaryWriter();
        }
      }
      byte[] jsonBytes = ("\n" + queryInfo.toJson() + "\n").getBytes(Bytes.UTF8_CHARSET);
      try {
        querySummaryWriter.out.writeInt(jsonBytes.length);
        querySummaryWriter.out.write(jsonBytes);
      } catch (IOException ie) {
        IOUtils.cleanup(LOG, querySummaryWriter);
        querySummaryWriter.out = null;
        throw ie;
      }
      return querySummaryWriter;
    }

    private void rollingQuerySummaryWriter() throws Exception {
      // finding largest file sequence
      SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
      String currentDateTime = df.format(new Date(System.currentTimeMillis()));

      Path datePath = new Path(historyParentPath, currentDateTime.substring(0, 8) + "/" + QUERY_LIST);
      FileSystem fs = getNonCrcFileSystem(datePath, tajoConf);
      if (!fs.exists(datePath)) {
        if (!fs.mkdirs(datePath)) {
          LOG.error("Can't make QueryList history dir: " + datePath.getParent());
          return;
        }
      }

      Path historyFile = new Path(datePath, QUERY_LIST + "-" + currentDateTime.substring(8, 14) + HISTORY_FILE_POSTFIX);
      querySummaryWriter.path = historyFile;
      querySummaryWriter.lastWritingTime = System.currentTimeMillis();
      LOG.info("Create query history file: " + historyFile);
      querySummaryWriter.out = fs.create(historyFile, queryReplication);
    }

    private void flushTaskHistories() {
      synchronized (taskWriters) {
        for (WriterHolder holder : taskWriters.values()) {
          try {
            holder.flush();
          } catch (IOException e) {
            LOG.warn(e, e);
          }
        }
      }
    }

    private WriterHolder writeTaskHistory(TaskHistory taskHistory) throws Exception {
      SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");

      String taskStartTime = df.format(new Date(taskHistory.getStartTime()));

      // taskWriters variable has three Writer(currentTime-2, currentTime-1, currentTime)
      // because Task history writer is rolled by hourly
      WriterHolder writerHolder = taskWriters.get(taskStartTime);
      if (writerHolder == null) {
        writerHolder = new WriterHolder();
        writerHolder.out = createTaskHistoryFile(taskStartTime, writerHolder);
        taskWriters.put(taskStartTime, writerHolder);
      }
      writerHolder.lastWritingTime = System.currentTimeMillis();

      if (writerHolder.out != null) {
        try {
          byte[] taskHistoryBytes = taskHistory.getProto().toByteArray();
          writerHolder.out.writeInt(taskHistoryBytes.length);
          writerHolder.out.write(taskHistoryBytes);
        } catch (IOException ie) {
          taskWriters.remove(taskStartTime);
          IOUtils.cleanup(LOG, writerHolder);
          throw ie;
        }
      }
      return writerHolder;
    }

    private FSDataOutputStream createTaskHistoryFile(String taskStartTime, WriterHolder writerHolder) throws IOException {
      FileSystem fs = getNonCrcFileSystem(taskHistoryParentPath, tajoConf);
      Path path = getQueryTaskHistoryPath(fs, taskHistoryParentPath, processName, taskStartTime);
      if (!fs.exists(path)) {
        if (!fs.mkdirs(path.getParent())) {
          LOG.error("Can't make Query history directory: " + path);
          return null;
        }
      }
      writerHolder.path = path;
      return fs.create(path, false, 4096, taskReplication, fs.getDefaultBlockSize(path));
    }
  }

  public static Path getQueryTaskHistoryPath(FileSystem fs, Path parent,
                                              String processName, String taskStartTime) throws IOException {
    // <tajo.task-history.path>/<yyyyMMdd>/tasks/<WORKER_HOST>_<WORKER_PORT>/<WORKER_HOST>_<WORKER_PORT>_<HH>_<seq>.hist

    // finding largest sequence path
    Path fileParent = new Path(parent, taskStartTime.substring(0, 8) + "/tasks/" + processName);

    String hour = taskStartTime.substring(8, 10);
    int maxSeq = -1;

    if (!fs.exists(fileParent)) {
      maxSeq++;
      return new Path(fileParent, processName + "_" + hour + "_" + maxSeq + HISTORY_FILE_POSTFIX);
    }

    if (!fs.isDirectory(fileParent)) {
      throw new IOException("Task history path is not directory: " + fileParent);
    }
    FileStatus[] files = fs.listStatus(fileParent);
    if (files != null) {
      for (FileStatus eachFile: files) {
        String[] nameTokens = eachFile.getPath().getName().split("_");
        if (nameTokens.length != 4) {
          continue;
        }

        if (nameTokens[2].equals(hour)) {
          int prefixIndex = nameTokens[3].indexOf(".");
          if (prefixIndex > 0) {
            try {
              int fileSeq = Integer.parseInt(nameTokens[3].substring(0, prefixIndex));
              if (fileSeq > maxSeq) {
                maxSeq = fileSeq;
              }
            } catch (NumberFormatException e) {
            }
          }
        }
      }
    }

    maxSeq++;
    return new Path(fileParent, processName + "_" + hour + "_" + maxSeq + HISTORY_FILE_POSTFIX);
  }

  static class WriterHolder implements Closeable {
    long lastWritingTime;
    Path path;
    FSDataOutputStream out;

    @Override
    public void close() throws IOException {
      if (out != null) out.close();
    }

    /*
     * Sync buffered data to DataNodes or disks (flush to disk devices).
     */
    private void flush() throws IOException {
      if (out != null) out.hsync();
    }
  }

  static class WriterFuture<T> implements Future<T> {
    private boolean done = false;
    private T result;
    private History history;
    private Throwable error;
    private CountDownLatch latch = new CountDownLatch(1);

    public WriterFuture(History history) {
      this.history = history;
    }

    private History getHistory() {
      return history;
    }

    public void done(T t) {
      this.result = t;
      this.done = true;
      this.latch.countDown();
    }

    public void failed(Throwable e) {
      this.error = e;
      done(null);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      // TODO - to be implemented
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
      // TODO - to be implemented
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
      return done;
    }

    public boolean isSucceed() {
      return error == null;
    }

    public Throwable getError() {
      return error;
    }

    @Override
    public T get() throws InterruptedException {
      this.latch.await();
      return result;
    }

    @Override
    public T get(long timeout, TimeUnit unit)
        throws InterruptedException, TimeoutException {
      if (latch.await(timeout, unit)) {
        return result;
      } else {
        throw new TimeoutException();
      }
    }
  }
}
