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
import com.google.common.collect.Sets;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.ResourceProtos.TaskHistoryProto;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.util.Bytes;

import java.io.EOFException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HistoryReader {
  private static final Log LOG = LogFactory.getLog(HistoryReader.class);

  public static final int DEFAULT_PAGE_SIZE = 100;
  public static final int DEFAULT_TASK_PAGE_SIZE = 2000;
  private String processName;
  private TajoConf tajoConf;
  private Path historyParentPath;
  private Path taskHistoryParentPath;

  public HistoryReader(String processName, TajoConf tajoConf) throws IOException {
    this.processName = processName.replaceAll(":", "_").toLowerCase();
    this.tajoConf = tajoConf;

    historyParentPath = tajoConf.getQueryHistoryDir(tajoConf);
    taskHistoryParentPath = tajoConf.getTaskHistoryDir(tajoConf);
  }

  @Deprecated
  public List<QueryInfo> getQueriesInHistory() throws IOException {
    return getQueriesInHistory(-1, Integer.MAX_VALUE);
  }

  /**
   * Get desc ordered query histories in persistent storage
   * @param page index of page
   * @param size size of page
   */
  public List<QueryInfo> getQueriesInHistory(int page, int size) throws IOException {
    return findQueryInfoInStorage(page, size, null);
  }

  private synchronized List<QueryInfo> findQueryInfoInStorage(int page, int size, @Nullable QueryId queryId)
      throws IOException {
    List<QueryInfo> result = Lists.newLinkedList();

    FileSystem fs = HistoryWriter.getNonCrcFileSystem(historyParentPath, tajoConf);
    try {
      if (!fs.exists(historyParentPath)) {
        return result;
      }
    } catch (Throwable e) {
      return result;
    }

    FileStatus[] files = fs.listStatus(historyParentPath);
    if (files == null || files.length == 0) {
      return result;
    }

    Set<QueryInfo> queryInfos = Sets.newTreeSet(Collections.reverseOrder());
    int startIndex = page < 1 ? page : ((page - 1) * size) + 1;
    int currentIndex = 0;
    int skipSize = 0;

    ArrayUtils.reverse(files);
    for (FileStatus eachDateFile : files) {
      Path queryListPath = new Path(eachDateFile.getPath(), HistoryWriter.QUERY_LIST);
      if (eachDateFile.isFile() || !fs.exists(queryListPath)) {
        continue;
      }

      FileStatus[] dateFiles = fs.listStatus(queryListPath);
      if (dateFiles == null || dateFiles.length == 0) {
        continue;
      }

      ArrayUtils.reverse(dateFiles);
      for (FileStatus eachFile : dateFiles) {
        Path path = eachFile.getPath();
        if (eachFile.isDirectory() || !path.getName().endsWith(HistoryWriter.HISTORY_FILE_POSTFIX)) {
          continue;
        }

        FSDataInputStream in = null;

        List<String> jsonList = Lists.newArrayList();
        try {
          in = fs.open(path);

          //If history file does not close, FileStatus.getLen() are not being updated
          //So, this code block should check the EOFException
          while (true) {
            int length = in.readInt();

            byte[] buf = new byte[length];
            in.readFully(buf, 0, length);

            jsonList.add(new String(buf, 0, length, Bytes.UTF8_CHARSET));
            currentIndex++;
          }
        } catch (EOFException eof) {
        } catch (Throwable e) {
          LOG.warn("Reading error:" + path + ", " + e.getMessage());
        } finally {
          IOUtils.cleanup(LOG, in);
        }

        //skip previous page
        if (startIndex > currentIndex) {
          skipSize += jsonList.size();
        } else {
          for (String json : jsonList) {
            QueryInfo queryInfo = QueryInfo.fromJson(json);
            if (queryId != null) {
              if (queryInfo.getQueryId().equals(queryId)) {
                result.add(queryInfo);
                return result;
              }
            } else {
              queryInfos.add(queryInfo);
            }
          }
        }

        if (currentIndex - (startIndex - 1) >= size) {
          result.addAll(queryInfos);
          int fromIndex = (startIndex - 1) - skipSize;
          return result.subList(fromIndex, fromIndex + size);
        }
      }
    }

    result.addAll(queryInfos);
    return result;
  }

  public QueryInfo getQueryByQueryId(QueryId queryId) throws IOException {
    List<QueryInfo> queryInfoList = findQueryInfoInStorage(-1, Integer.MAX_VALUE, queryId);
    if (queryInfoList.size() > 0) {
      return queryInfoList.get(0);
    } else {
      return null;
    }
  }

  private Path getQueryHistoryFilePath(String queryId, long startTime) throws IOException {
    if (startTime == 0) {
      String[] tokens = queryId.split("_");
      if (tokens.length == 3) {
        startTime = Long.parseLong(tokens[1]);
      } else {
        startTime = System.currentTimeMillis();
      }
    }
    Path queryHistoryPath = HistoryWriter.getQueryHistoryFilePath(historyParentPath, queryId, startTime);
    FileSystem fs = HistoryWriter.getNonCrcFileSystem(queryHistoryPath, tajoConf);

    if (!fs.exists(queryHistoryPath)) {
      LOG.info("No query history file: " + queryHistoryPath);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(startTime);
      cal.add(Calendar.DAY_OF_MONTH, -1);
      queryHistoryPath = HistoryWriter.getQueryHistoryFilePath(historyParentPath, queryId, startTime);
      if (!fs.exists(queryHistoryPath)) {
        LOG.info("No query history file: " + queryHistoryPath);
        cal.setTimeInMillis(startTime);
        cal.add(Calendar.DAY_OF_MONTH, 1);
        queryHistoryPath = HistoryWriter.getQueryHistoryFilePath(historyParentPath, queryId, startTime);
      }
      if (!fs.exists(queryHistoryPath)) {
        LOG.info("No query history file: " + queryHistoryPath);
        return null;
      }
    }
    return queryHistoryPath;
  }

  public QueryHistory getQueryHistory(String queryId) throws IOException {
    return  getQueryHistory(queryId, 0);
  }

  public QueryHistory getQueryHistory(String queryId, long startTime) throws IOException {
    Path queryHistoryFile = getQueryHistoryFilePath(queryId, startTime);
    if (queryHistoryFile == null) {
      return null;
    }
    FileSystem fs = HistoryWriter.getNonCrcFileSystem(queryHistoryFile, tajoConf);

    FileStatus fileStatus = fs.getFileStatus(queryHistoryFile);
    if (fileStatus.getLen() > 10 * 1024 * 1024) {
      throw new IOException("QueryHistory file is too big: " +
          queryHistoryFile + ", " + fileStatus.getLen() + " bytes");
    }
    FSDataInputStream in = null;
    try {
      in = fs.open(queryHistoryFile);
      byte[] buf = new byte[(int)fileStatus.getLen()];

      in.readFully(buf, 0, buf.length);

      return QueryHistory.fromJson(new String(buf, Bytes.UTF8_CHARSET));
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  public List<TaskHistory> getTaskHistory(String queryId, String ebId) throws IOException {
    return getTaskHistory(queryId, ebId, 0);
  }

  public List<TaskHistory> getTaskHistory(String queryId, String ebId, long startTime) throws IOException {
    Path queryHistoryFile = getQueryHistoryFilePath(queryId, startTime);
    if (queryHistoryFile == null) {
      return new ArrayList<>();
    }
    Path detailFile = new Path(queryHistoryFile.getParent(), ebId + HistoryWriter.HISTORY_FILE_POSTFIX);
    FileSystem fs = HistoryWriter.getNonCrcFileSystem(detailFile, tajoConf);

    if (!fs.exists(detailFile)) {
      return new ArrayList<>();
    }

    FileStatus fileStatus = fs.getFileStatus(detailFile);
    if (fileStatus.getLen() > 100 * 1024 * 1024) {    // 100MB
      throw new IOException("TaskHistory file is too big: " + detailFile + ", " + fileStatus.getLen() + " bytes");
    }

    FSDataInputStream in = null;
    try {
      in = fs.open(detailFile);
      byte[] buf = new byte[(int)fileStatus.getLen()];

      in.readFully(buf, 0, buf.length);

      return StageHistory.fromJsonTasks(new String(buf, Bytes.UTF8_CHARSET));
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  public org.apache.tajo.worker.TaskHistory getTaskHistory(String taskAttemptId, long startTime) throws IOException {
    FileSystem fs = HistoryWriter.getNonCrcFileSystem(taskHistoryParentPath, tajoConf);

    SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");

    Calendar cal = Calendar.getInstance();
    cal.setTime(new Date(startTime));

    //current, current-1, current+1 hour
    String[] targetHistoryFileDates = new String[3];
    targetHistoryFileDates[0] = df.format(cal.getTime());

    cal.add(Calendar.HOUR_OF_DAY, -1);
    targetHistoryFileDates[1] = df.format(cal.getTime());

    cal.setTime(new Date(startTime));
    cal.add(Calendar.HOUR_OF_DAY, 1);
    targetHistoryFileDates[2] = df.format(cal.getTime());

    for (String historyFileDate : targetHistoryFileDates) {
      Path fileParent = new Path(taskHistoryParentPath, historyFileDate.substring(0, 8) + "/tasks/" + processName);
      String hour = historyFileDate.substring(8, 10);

      if (!fs.exists(fileParent)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Task history parent not exists:" + fileParent);
        }
        continue;
      }

      FileStatus[] files = fs.listStatus(fileParent);
      if (files == null || files.length == 0) {
        return null;
      }

      String filePrefix = processName + "_" + hour + "_";

      for (FileStatus eachFile : files) {
        if (eachFile.getPath().getName().indexOf(filePrefix) != 0) {
          continue;
        }

        FSDataInputStream in = null;
        TaskHistoryProto.Builder builder = TaskHistoryProto.newBuilder();
        try {
          FileStatus status = fs.getFileStatus(eachFile.getPath());
          LOG.info("Finding TaskHistory from " + status.getLen() + "," + eachFile.getPath());

          in = fs.open(eachFile.getPath());
          while (true) {
            int len = in.readInt();
            byte[] buf = new byte[len];
            in.readFully(buf, 0, len);

            builder.clear();
            TaskHistoryProto taskHistoryProto = builder.mergeFrom(buf).build();
            TaskAttemptId attemptId = new TaskAttemptId(taskHistoryProto.getTaskAttemptId());
            if (attemptId.toString().equals(taskAttemptId)) {
              return new org.apache.tajo.worker.TaskHistory(taskHistoryProto);
            }
          }
        } catch (EOFException e) {
        } finally {
          if (in != null) {
            in.close();
          }
        }
      }
    }
    return null;
  }
}
