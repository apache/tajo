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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.worker.TaskHistory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
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

  private final LinkedBlockingQueue<History> historyQueue = new LinkedBlockingQueue<History>();
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

  public HistoryWriter(String processName, boolean isMaster) {
    super(HistoryWriter.class.getName() + ":" + processName);
    this.processName = processName.replaceAll(":", "_").toLowerCase();
    this.isMaster = isMaster;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    tajoConf = (TajoConf)conf;
    historyParentPath = tajoConf.getQueryHistoryDir(tajoConf);
    taskHistoryParentPath = tajoConf.getTaskHistoryDir(tajoConf);
    writerThread = new WriterThread();
    historyCleaner = new HistoryCleaner(tajoConf, isMaster);
    super.serviceInit(conf);
  }

  @Override
  public void serviceStop() throws Exception {
    for (WriterHolder eachWriter : taskWriters.values()) {
      if (eachWriter.out != null) {
        try {
          eachWriter.out.close();
        } catch (Exception err) {
          LOG.error(err.getMessage(), err);
        }
      }
    }
    taskWriters.clear();
    stopped.set(true);
    writerThread.interrupt();

    if (querySummaryWriter != null && querySummaryWriter.out != null) {
      try {
        querySummaryWriter.out.close();
      } catch (Exception err) {
        LOG.error(err.getMessage(), err);
      }
    }

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

  public void appendHistory(History history) {
    synchronized (historyQueue) {
      historyQueue.add(history);
      historyQueue.notifyAll();
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

    Path datePath = null;
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
    public void run() {
      LOG.info("HistoryWriter_"+ processName + " started.");
      SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
      while (!stopped.get()) {
        List<History> histories = new ArrayList<History>();
        synchronized (historyQueue) {
          historyQueue.drainTo(histories);
          if (histories.isEmpty()) {
            try {
              historyQueue.wait(60 * 1000);
            } catch (InterruptedException e) {
              if (stopped.get()) {
                break;
              }
            }
          }
        }
        if (stopped.get()) {
          break;
        }
        try {
          if (!histories.isEmpty()) {
            writeHistory(histories);
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }

        //clean up history file

        // closing previous writer
        synchronized (taskWriters) {
          Calendar cal = Calendar.getInstance();
          cal.add(Calendar.HOUR_OF_DAY, -2);
          String closeTargetTime = df.format(cal.getTime());
          List<String> closingTargets = new ArrayList<String>();
          synchronized (taskWriters) {
            for (String eachWriterTime : taskWriters.keySet()) {
              if (eachWriterTime.compareTo(closeTargetTime) <= 0) {
                closingTargets.add(eachWriterTime);
              }
            }
          }

          for (String eachWriterTime : closingTargets) {
            WriterHolder writerHolder = null;
            synchronized (taskWriters) {
              writerHolder = taskWriters.remove(eachWriterTime);
            }
            if (writerHolder != null) {
              LOG.info("Closing task history file: " + writerHolder.path);
              if (writerHolder.out != null) {
                try {
                  writerHolder.out.close();
                } catch (IOException e) {
                  LOG.error(e.getMessage(), e);
                }
              }
            }
          }
        }
      }
      LOG.info("HistoryWriter_"+ processName + " stopped.");
    }

    public void writeHistory(List<History> histories) {
      if (histories.isEmpty()) {
        return;
      }
      for (History eachHistory : histories) {
        switch(eachHistory.getHistoryType()) {
          case TASK:
            try {
              writeTaskHistory((TaskHistory) eachHistory);
            } catch (Exception e) {
              LOG.error("Error while saving task history: " +
                  ((TaskHistory) eachHistory).getTaskAttemptId() + ":" + e.getMessage(), e);
            }
            break;
          case QUERY:
            try {
              writeQueryHistory((QueryHistory) eachHistory);
            } catch (Exception e) {
              LOG.error("Error while saving query history: " +
                  ((QueryHistory) eachHistory).getQueryId() + ":" + e.getMessage(), e);
            }
            break;
          case QUERY_SUMMARY:
            try {
              writeQuerySummary((QueryInfo) eachHistory);
            } catch (Exception e) {
              LOG.error("Error while saving query summary: " +
                  ((QueryInfo) eachHistory).getQueryId() + ":" + e.getMessage(), e);
            }
            break;
          default:
            LOG.warn("Wrong history type: " + eachHistory.getHistoryType());
        }
      }
    }

    private synchronized void writeQueryHistory(QueryHistory queryHistory) throws Exception {
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
        out = fs.create(queryHistoryFile);
        out.write(queryHistory.toJson().getBytes());
      } finally {
        if (out != null) {
          try {
            out.close();
          } catch (Exception err) {
            LOG.error(err.getMessage(), err);
          }
        }
      }

      if (queryHistory.getStageHistories() != null) {
        for (StageHistory stageHistory : queryHistory.getStageHistories()) {
          Path path = new Path(queryHistoryFile.getParent(), stageHistory.getExecutionBlockId() + HISTORY_FILE_POSTFIX);
          out = null;
          try {
            out = fs.create(path);
            out.write(stageHistory.toTasksJson().getBytes());
            LOG.info("Saving query unit: " + path);
          } finally {
            if (out != null) {
              try {
                out.close();
              } catch (Exception err) {
                LOG.error(err.getMessage(), err);
              }
            }
          }
        }
      }
    }

    private synchronized void writeQuerySummary(QueryInfo queryInfo) throws Exception {
      if(stopped.get()) return;

        // writing to HDFS and rolling hourly
      if (querySummaryWriter == null) {
        querySummaryWriter = new WriterHolder();
        rollingQuerySummaryWriter();
      } else {
        if (querySummaryWriter.out == null) {
          rollingQuerySummaryWriter();
        } else if (System.currentTimeMillis() - querySummaryWriter.lastWritingTime >= 60 * 60 * 1000) {
          if (querySummaryWriter.out != null) {
            LOG.info("Close query history file: " + querySummaryWriter.path);
            querySummaryWriter.out.close();
          }
          rollingQuerySummaryWriter();
        }
      }
      byte[] jsonBytes = ("\n" + queryInfo.toJson() + "\n").getBytes();

      querySummaryWriter.out.writeInt(jsonBytes.length);
      querySummaryWriter.out.write(jsonBytes);
      querySummaryWriter.out.hflush();
    }

    private synchronized void rollingQuerySummaryWriter() throws Exception {
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
      querySummaryWriter.out = fs.create(historyFile);
    }

    private synchronized void writeTaskHistory(TaskHistory taskHistory) throws Exception {
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
        byte[] taskHistoryBytes = taskHistory.getProto().toByteArray();
        writerHolder.out.writeInt(taskHistoryBytes.length);
        writerHolder.out.write(taskHistoryBytes);
        writerHolder.out.flush();
      }
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
      return fs.create(path, false);
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

  class WriterHolder {
    long lastWritingTime;
    Path path;
    FSDataOutputStream out;
  }
}
