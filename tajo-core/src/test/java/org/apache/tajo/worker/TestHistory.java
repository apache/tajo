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
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.*;

public class TestHistory {
  private TajoTestingCluster cluster;
  private TajoMaster master;
  private TajoConf conf;
  private TajoClient client;

  @Before
  public void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    master = cluster.getMaster();
    conf = cluster.getConfiguration();
    client = new TajoClient(conf);
  }

  @After
  public void tearDown() {
    client.close();
  }


  @Test
  public final void testTaskRunnerHistory() throws IOException, ServiceException, InterruptedException {
    int beforeFinishedQueriesCount = master.getContext().getQueryJobManager().getFinishedQueries().size();
    client.executeQueryAndGetResult("select sleep(1) from lineitem");

    Collection<QueryInProgress> finishedQueries = master.getContext().getQueryJobManager().getFinishedQueries();
    assertTrue(finishedQueries.size() > beforeFinishedQueriesCount);

    TajoWorker worker = cluster.getTajoWorkers().get(0);
    TaskRunnerManager taskRunnerManager = worker.getWorkerContext().getTaskRunnerManager();
    assertNotNull(taskRunnerManager);


    Collection<TaskRunnerHistory> histories = taskRunnerManager.getExecutionBlockHistories();
    assertTrue(histories.size() > 0);

    TaskRunnerHistory history = histories.iterator().next();
    assertEquals(Service.STATE.STOPPED, history.getState());

    assertEquals(history, new TaskRunnerHistory(history.getProto()));
  }

  @Test
  public final void testTaskHistory() throws IOException, ServiceException, InterruptedException {
    int beforeFinishedQueriesCount = master.getContext().getQueryJobManager().getFinishedQueries().size();
    client.executeQueryAndGetResult("select sleep(1) from lineitem");

    Collection<QueryInProgress> finishedQueries = master.getContext().getQueryJobManager().getFinishedQueries();
    assertTrue(finishedQueries.size() > beforeFinishedQueriesCount);

    TajoWorker worker = cluster.getTajoWorkers().get(0);
    TaskRunnerManager taskRunnerManager = worker.getWorkerContext().getTaskRunnerManager();
    assertNotNull(taskRunnerManager);


    Collection<TaskRunnerHistory> histories = taskRunnerManager.getExecutionBlockHistories();
    assertTrue(histories.size() > 0);

    TaskRunnerHistory history = histories.iterator().next();

    assertTrue(history.size() > 0);
    assertEquals(Service.STATE.STOPPED, history.getState());

    Map.Entry<QueryUnitAttemptId, TaskHistory> entry =
        history.getTaskHistoryMap().entrySet().iterator().next();

    QueryUnitAttemptId queryUnitAttemptId = entry.getKey();
    TaskHistory taskHistory = entry.getValue();

    assertEquals(TajoProtos.TaskAttemptState.TA_SUCCEEDED, taskHistory.getState());
    assertEquals(queryUnitAttemptId, taskHistory.getQueryUnitAttemptId());
  }
}
