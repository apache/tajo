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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.querymaster.QueryInfo;
import org.apache.tajo.worker.TajoWorker.WorkerContext;
import org.apache.tajo.worker.TaskHistory;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * History directory structure
 *   tajo.query-history.path: hdfs
 *   tajo.task-history.path: local or hdfs
 *
 *   <tajo.query-history.path>/<yyyyMMdd>/query-list/query-list-<sequence>.hist (TajoMaster's query list, hourly rolling)
 *                                       /query-detail/<QUERY_ID>/query.hist    (QueryMaster's query detail)
 *                                                               /<EB_ID>.hist  (QueryMaster's subquery detail)
 *   <tajo.task-history.path>/<yyyyMMdd>/tasks/<HH>/<WORKER_HOST>_<WORKER_PORT>.hist
 * History files are kept for "tajo.query-history.preserve.hours" (default value is 68 hours-1 week.)
 */
public class HistoryWriter extends AbstractService {
  private static final Log LOG = LogFactory.getLog(HistoryWriter.class);
  private static final String QUERY_LIST = "query-list";
  private static final String QUERY_DETAIL = "query-detail";
  private static final String HISTORY_FILE_POSTFIX = ".hist";
  private static final String QUERY_DELIMITER = "TAJO-QUERY---------------------------" + System.currentTimeMillis();

  private final LinkedBlockingQueue<History> historyQueue = new LinkedBlockingQueue<History>();
  private String writerName;

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

  public HistoryWriter(String processName) {
    super(HistoryWriter.class.getName() + ":" + processName);
    this.processName = processName;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    tajoConf = (TajoConf)conf;
    historyParentPath = tajoConf.getQueryHistoryDir(tajoConf);
    taskHistoryParentPath = tajoConf.getTaskHistoryDir(tajoConf);
    writerThread = new WriterThread();
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
    super.serviceStop();
  }

  @Override
  public void serviceStart() throws Exception {
    writerThread.start();
  }

  public void appendHistory(History history) {
    synchronized (historyQueue) {
      historyQueue.add(history);
      historyQueue.notifyAll();
    }
  }

  class WriterThread extends Thread {
    public void run() {
      LOG.info("HistoryWriter_"+ writerName + " started.");
      SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
      while (!stopped.get()) {
        List<History> histories = new ArrayList<History>();
        synchronized (historyQueue) {
          try {
            historyQueue.wait(60 * 1000);
          } catch (InterruptedException e) {
            if (stopped.get()) {
              break;
            }
          }
          historyQueue.drainTo(histories);
        }

        try {
          writeHistory(histories);
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
      LOG.info("HistoryWriter_"+ writerName + " stopped.");
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
                  ((TaskHistory) eachHistory).getQueryUnitAttemptId() + ":" + e.getMessage(), e);
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
          default:
            LOG.warn("Wrong history type: " + eachHistory.getHistoryType());
        }
      }
    }


    private synchronized void writeQueryHistory(QueryHistory queryHistory) throws Exception {
      // QueryMaster's query detail history (json format)
      // <tajo.query-history.path>/<yyyyMMdd>/query-detail/<QUERY_ID>/query.hist

      // QueryMaster's subquery detail history(proto binary format)
      // <tajo.query-history.path>/<yyyyMMdd>/query-detail/<QUERY_ID>/<EB_ID>.hist

      SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
      Path datePath = new Path(historyParentPath, df.format(new Date(System.currentTimeMillis())) + "/" + QUERY_DETAIL);
      Path queryHistoryFile = new Path(datePath, queryHistory.getQueryId() + "/query" + HISTORY_FILE_POSTFIX);
      FileSystem fs = queryHistoryFile.getFileSystem(tajoConf);

      if (!fs.exists(queryHistoryFile.getParent())) {
        if (!fs.mkdirs(queryHistoryFile.getParent())) {
          LOG.error("Can't make QueryHistory dir: " + queryHistoryFile.getParent());
          return;
        }
      }

      FSDataOutputStream out = null;
      try {
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

      if (queryHistory.getSubQueryHistories() != null) {
        for (SubQueryHistory subQueryHistory : queryHistory.getSubQueryHistories()) {
          Path path = new Path(queryHistoryFile.getParent(), subQueryHistory.getExecutionBlockId() + HISTORY_FILE_POSTFIX);
          out = null;
          try {
            out = fs.create(path);
            out.write(subQueryHistory.toJson().getBytes());
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
      querySummaryWriter.out.write((queryInfo.toJson() + "\n" + QUERY_DELIMITER + "\n").getBytes());
      querySummaryWriter.out.hflush();
    }

    private synchronized void rollingQuerySummaryWriter() throws Exception {
      // finding largest file sequence
      SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
      Path datePath = new Path(historyParentPath, df.format(new Date(System.currentTimeMillis())) + "/" + QUERY_LIST);
      FileSystem fs = datePath.getFileSystem(tajoConf);
      if (!fs.exists(datePath)) {
        if (!fs.mkdirs(datePath)) {
          LOG.error("Can't make QueryList history dir: " + datePath.getParent());
          return;
        }
      }

      FileStatus[] files = fs.listStatus(datePath);
      int maxSeq = -1;
      if (files != null) {
        for (FileStatus eachFile: files) {
          //query-list-<sequence>.hist
          String[] nameTokens = eachFile.getPath().getName().split("-");
          if (nameTokens.length == 3) {
            try {
              int fileSeq = Integer.parseInt(nameTokens[2]);
              if (fileSeq > maxSeq) {
                maxSeq = fileSeq;
              }
            } catch (NumberFormatException e) {
            }
          }
        }
      }

      maxSeq++;
      Path historyFile = new Path(datePath, QUERY_LIST + "-" + maxSeq + HISTORY_FILE_POSTFIX);
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
        writerHolder.out.write(taskHistoryBytes.length);
        writerHolder.out.write(taskHistoryBytes);
        writerHolder.out.hflush();
      }
    }

    private FSDataOutputStream createTaskHistoryFile(String taskStartTime, WriterHolder writerHolder) throws IOException {
      Path path = getQueryTsaskHistoryPath(taskHistoryParentPath, processName, taskStartTime);
      FileSystem fs = path.getFileSystem(tajoConf);
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

  public static Path getQueryTsaskHistoryPath(Path parent, String processName, String taskStartTime) {
    //<tajo.task-history.path>/<yyyyMMdd>/tasks/<HH>/<WORKER_HOST>:<WORKER_PORT>.hist
    return new Path(parent, taskStartTime.substring(0, 8) + "/tasks/" +
        taskStartTime.substring(8, 10) + "/" + processName + HISTORY_FILE_POSTFIX);
  }

  class WriterHolder {
    long lastWritingTime;
    Path path;
    FSDataOutputStream out;
  }
}
