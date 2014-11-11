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

import com.google.common.io.Files;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.TajoProtos.TaskAttemptState;
import org.apache.tajo.catalog.proto.CatalogProtos.TableStatsProto;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.querymaster.QueryInfo;
import org.apache.tajo.util.TajoIdUtils;
import org.apache.tajo.worker.TaskHistory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class TestHistoryWriterReader extends QueryTestCaseBase {
  public static final String HISTORY_DIR = "/tmp/tajo-test-history";
  TajoConf tajoConf;
  @Before
  public void setUp() throws Exception {
    tajoConf = new TajoConf(testingCluster.getConfiguration());
    tajoConf.setVar(ConfVars.HISTORY_QUERY_DIR, HISTORY_DIR);
  }

  @After
  public void tearDown() throws Exception {
    Path path = TajoConf.getQueryHistoryDir(tajoConf);
    FileSystem fs = path.getFileSystem(tajoConf);
    fs.delete(path, true);
  }

  @Test
  public void testQueryInfoReadAndWrite() throws Exception {
    HistoryWriter writer = new HistoryWriter("127.0.0.1:28090", true);
    try {
      writer.init(tajoConf);
      writer.start();

      long startTime = System.currentTimeMillis();
      QueryInfo queryInfo1 = new QueryInfo(QueryIdFactory.newQueryId(startTime, 1));
      queryInfo1.setStartTime(startTime);
      queryInfo1.setProgress(1.0f);
      queryInfo1.setQueryState(QueryState.QUERY_SUCCEEDED);

      QueryInfo queryInfo2 = new QueryInfo(QueryIdFactory.newQueryId(startTime, 2));
      queryInfo2.setStartTime(startTime);
      queryInfo2.setProgress(0.5f);
      queryInfo2.setQueryState(QueryState.QUERY_FAILED);

      writer.appendHistory(queryInfo1);
      writer.appendHistory(queryInfo2);

      // HistoryWriter writes asynchronous.
      Thread.sleep(5 * 1000);

      SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
      Path path = new Path(tajoConf.getVar(ConfVars.HISTORY_QUERY_DIR));

      FileSystem fs = path.getFileSystem(tajoConf);
      Path parentPath = new Path(path, df.format(startTime) + "/query-list");
      FileStatus[] histFiles = fs.listStatus(parentPath);
      assertNotNull(histFiles);
      assertEquals(1, histFiles.length);
      assertTrue(histFiles[0].isFile());
      assertTrue(histFiles[0].getPath().getName().endsWith(".hist"));

      HistoryReader reader = new HistoryReader("127.0.0.1:28090", tajoConf);
      List<QueryInfo> queryInfos = reader.getQueries(null);
      assertNotNull(queryInfos);
      assertEquals(2, queryInfos.size());

      QueryInfo foundQueryInfo = queryInfos.get(0);
      assertEquals(queryInfo2.getQueryId(), foundQueryInfo.getQueryId());
      assertEquals(queryInfo2.getQueryState(), foundQueryInfo.getQueryState());
      assertEquals(queryInfo2.getProgress(), foundQueryInfo.getProgress(), 0);

      foundQueryInfo = queryInfos.get(1);
      assertEquals(queryInfo1.getQueryId(), foundQueryInfo.getQueryId());
      assertEquals(queryInfo1.getQueryState(), foundQueryInfo.getQueryState());
      assertEquals(queryInfo1.getProgress(), foundQueryInfo.getProgress(), 0);
    } finally {
      writer.stop();
    }
  }

  @Test
  public void testQueryHistoryReadAndWrite() throws Exception {
    HistoryWriter writer = new HistoryWriter("127.0.0.1:28090", true);
    writer.init(tajoConf);
    writer.start();

    try {
      long startTime = System.currentTimeMillis();

      QueryHistory queryHistory = new QueryHistory();
      QueryId queryId = QueryIdFactory.newQueryId(startTime, 1);
      queryHistory.setQueryId(queryId.toString());
      queryHistory.setLogicalPlan("LogicalPlan");
      List<SubQueryHistory> subQueries = new ArrayList<SubQueryHistory>();
      for (int i = 0; i < 3; i++) {
        ExecutionBlockId ebId = QueryIdFactory.newExecutionBlockId(queryId, i);
        SubQueryHistory subQueryHistory = new SubQueryHistory();
        subQueryHistory.setExecutionBlockId(ebId.toString());
        subQueryHistory.setStartTime(startTime + i);

        List<QueryUnitHistory> queryUnitHistories = new ArrayList<QueryUnitHistory>();
        for (int j = 0; j < 5; j++) {
          QueryUnitHistory queryUnitHistory = new QueryUnitHistory();
          queryUnitHistory.setId(QueryIdFactory.newQueryUnitAttemptId(QueryIdFactory.newQueryUnitId(ebId), 1).toString());
          queryUnitHistories.add(queryUnitHistory);
        }
        subQueryHistory.setQueryUnits(queryUnitHistories);
        subQueries.add(subQueryHistory);
      }
      queryHistory.setSubQueryHistories(subQueries);

      writer.appendHistory(queryHistory);

      // HistoryWriter writes asynchronous.
      Thread.sleep(5 * 1000);

      SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
      Path path = new Path(tajoConf.getVar(ConfVars.HISTORY_QUERY_DIR));

      FileSystem fs = path.getFileSystem(tajoConf);

      assertTrue(fs.exists(new Path(path,
          df.format(startTime) + "/query-detail/" + queryId.toString() + "/query.hist")));
      for (int i = 0; i < 3; i++) {
        String ebId = QueryIdFactory.newExecutionBlockId(queryId, i).toString();
        assertTrue(fs.exists(new Path(path,
            df.format(startTime) + "/query-detail/" + queryId.toString() + "/" + ebId + ".hist")));
      }

      HistoryReader reader = new HistoryReader("127.0.0.1:28090", tajoConf);
      QueryHistory foundQueryHistory = reader.getQueryHistory(queryId.toString());
      assertNotNull(foundQueryHistory);
      assertEquals(queryId.toString(), foundQueryHistory.getQueryId());
      assertEquals(3, foundQueryHistory.getSubQueryHistories().size());

      for (int i = 0; i < 3; i++) {
        String ebId = QueryIdFactory.newExecutionBlockId(queryId, i).toString();
        SubQueryHistory subQueryHistory = foundQueryHistory.getSubQueryHistories().get(i);
        assertEquals(ebId, subQueryHistory.getExecutionBlockId());
        assertEquals(startTime + i, subQueryHistory.getStartTime());

        // QueryUnitHistory is stored in the other file.
        assertNull(subQueryHistory.getQueryUnits());

        List<QueryUnitHistory> queryUnits = reader.getQueryUnitHistory(queryId.toString(), ebId);
        assertNotNull(queryUnits);
        assertEquals(5, queryUnits.size());

        for (int j = 0; j < 5; j++) {
          QueryUnitHistory queryUnitHistory = queryUnits.get(j);
          assertEquals(subQueries.get(i).getQueryUnits().get(j).getId(), queryUnitHistory.getId());
        }
      }
    } finally {
      writer.stop();
    }
  }

  @Test
  public void testTaskHistoryReadAndWrite() throws Exception {
    TajoConf tajoConf = new TajoConf();
    File historyParentDir = Files.createTempDir();
    historyParentDir.deleteOnExit();
    tajoConf.setVar(ConfVars.HISTORY_TASK_DIR, "file://" + historyParentDir.getCanonicalPath());

    HistoryWriter writer = new HistoryWriter("127.0.0.1:28090", false);
    writer.init(tajoConf);
    writer.start();

    try {
      // Write TaskHistory
      TableStatsProto tableStats = TableStatsProto.newBuilder()
          .setNumRows(10)
          .setNumBytes(100)
          .build();
      long startTime = System.currentTimeMillis() - 2000;
      QueryUnitAttemptId id1 = TajoIdUtils.parseQueryUnitAttemptId("ta_1412326813565_0001_000001_000001_00");
      TaskHistory taskHistory1 = new TaskHistory(
          id1, TaskAttemptState.TA_SUCCEEDED, 1.0f, startTime, System.currentTimeMillis(), tableStats);
      writer.appendHistory(taskHistory1);

      QueryUnitAttemptId id2 = TajoIdUtils.parseQueryUnitAttemptId("ta_1412326813565_0001_000001_000002_00");
      TaskHistory taskHistory2 = new TaskHistory(
          id2, TaskAttemptState.TA_SUCCEEDED, 1.0f, startTime, System.currentTimeMillis() - 500, tableStats);
      writer.appendHistory(taskHistory2);

      // HistoryWriter writes asynchronous.
      Thread.sleep(5 * 1000);

      SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
      String startDate = df.format(new Date(startTime));
      Path taskParentPath = new Path(tajoConf.getVar(ConfVars.HISTORY_TASK_DIR),
          startDate.substring(0, 8) + "/tasks/127.0.0.1_28090");

      FileSystem fs = taskParentPath.getFileSystem(tajoConf);
      assertTrue(fs.exists(taskParentPath));

      HistoryReader reader = new HistoryReader("127.0.0.1:28090", tajoConf);
      TaskHistory foundTaskHistory = reader.getTaskHistory(id1.toString(), startTime);
      assertNotNull(foundTaskHistory);
      assertEquals(id1, foundTaskHistory.getQueryUnitAttemptId());
      assertEquals(taskHistory1, foundTaskHistory);

      foundTaskHistory = reader.getTaskHistory(id2.toString(), startTime);
      assertNotNull(foundTaskHistory);
      assertEquals(id2, foundTaskHistory.getQueryUnitAttemptId());
      assertEquals(taskHistory2, foundTaskHistory);

      foundTaskHistory = reader.getTaskHistory("ta_1412326813565_0001_000001_000003_00", startTime);
      assertNull(foundTaskHistory);
    } finally {
      writer.stop();
    }
  }
}
