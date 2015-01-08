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

package org.apache.tajo.worker;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.service.Service;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.QueryInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.*;

public class TestHistory {
  private static TajoTestingCluster cluster;
  private static TajoMaster master;
  private static  TajoConf conf;
  private static TajoClient client;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new TajoTestingCluster();
    cluster.startMiniClusterInLocal(1);
    master = cluster.getMaster();
    conf = cluster.getConfiguration();
    client = new TajoClientImpl(cluster.getConfiguration());
    File file = TPCH.getDataFile("lineitem");
    client.executeQueryAndGetResult("create external table default.lineitem (l_orderkey int, l_partkey int) "
        + "using text location 'file://" + file.getAbsolutePath() + "'");
    assertTrue(client.existTable("default.lineitem"));
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (client != null) client.close();
    if (cluster != null) cluster.shutdownMiniCluster();
  }

  @Test
  public final void testTaskRunnerHistory() throws IOException, ServiceException, InterruptedException {
    int beforeFinishedQueriesCount = master.getContext().getQueryJobManager().getFinishedQueries().size();
    client.executeQueryAndGetResult("select count(*) from lineitem");

    Collection<QueryInfo> finishedQueries = master.getContext().getQueryJobManager().getFinishedQueries();
    assertTrue(finishedQueries.size() > beforeFinishedQueriesCount);

    TajoWorker worker = cluster.getTajoWorkers().get(0);
    TaskRunnerManager taskRunnerManager = worker.getWorkerContext().getTaskRunnerManager();
    assertNotNull(taskRunnerManager);


    Collection<TaskRunnerHistory> histories = taskRunnerManager.getExecutionBlockHistories();
    assertTrue(histories.size() > 0);

    TaskRunnerHistory history = histories.iterator().next();
    assertEquals(Service.STATE.STOPPED, history.getState());
    TaskRunnerHistory fromProto = new TaskRunnerHistory(history.getProto());
    assertEquals(history.getExecutionBlockId(), fromProto.getExecutionBlockId());
    assertEquals(history.getFinishTime(), fromProto.getFinishTime());
    assertEquals(history.getStartTime(), fromProto.getStartTime());
    assertEquals(history.getState(), fromProto.getState());
    assertEquals(history.getContainerId(), fromProto.getContainerId());
    assertEquals(history.getProto().getTaskHistoriesCount(), fromProto.getProto().getTaskHistoriesCount());
  }

  @Test
  public final void testTaskHistory() throws IOException, ServiceException, InterruptedException {
    int beforeFinishedQueriesCount = master.getContext().getQueryJobManager().getFinishedQueries().size();
    client.executeQueryAndGetResult("select count(*) from lineitem");

    Collection<QueryInfo> finishedQueries = master.getContext().getQueryJobManager().getFinishedQueries();
    assertTrue(finishedQueries.size() > beforeFinishedQueriesCount);

    TajoWorker worker = cluster.getTajoWorkers().get(0);
    TaskRunnerManager taskRunnerManager = worker.getWorkerContext().getTaskRunnerManager();
    assertNotNull(taskRunnerManager);


    Collection<TaskRunnerHistory> histories = taskRunnerManager.getExecutionBlockHistories();
    assertTrue(histories.size() > 0);

    TaskRunnerHistory history = histories.iterator().next();

    assertTrue(history.size() > 0);
    assertEquals(Service.STATE.STOPPED, history.getState());

    Map.Entry<TaskAttemptId, TaskHistory> entry =
        history.getTaskHistoryMap().entrySet().iterator().next();

    TaskAttemptId taskAttemptId = entry.getKey();
    TaskHistory taskHistory = entry.getValue();

    assertEquals(TajoProtos.TaskAttemptState.TA_SUCCEEDED, taskHistory.getState());
    assertEquals(taskAttemptId, taskHistory.getTaskAttemptId());
  }
}
