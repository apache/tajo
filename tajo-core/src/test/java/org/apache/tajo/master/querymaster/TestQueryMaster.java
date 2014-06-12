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

package org.apache.tajo.master.querymaster;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse;
import org.apache.tajo.master.rm.Worker;
import org.apache.tajo.worker.TajoWorker;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestQueryMaster extends QueryTestCaseBase {
  public TestQueryMaster() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public void testQuerySessionTimeout() throws Exception {
    // If a query's session is expired, that query should be killed.

    TajoClient tajoClient = null;
    try {
      // 5 secs
      testingCluster.setAllTajoDaemonConfValue(ConfVars.QUERY_SESSION_TIMEOUT.varname, "5");

      conf = testBase.getTestingCluster().getConfiguration();
      tajoClient = new TajoClient(conf);

      // Run test query
      // Sleep for 10 secs.
      SubmitQueryResponse queryResponse = tajoClient.executeQuery("select sleep(2) from lineitem");

      //doesn't heartbeat
      tajoClient.close();
      tajoClient = null;

      Thread.sleep(10 * 1000);

      QueryId queryId = new QueryId(queryResponse.getQueryId());
      QueryInProgress qip = testingCluster.getMaster().getContext().getQueryJobManager().getFinishedQuery(queryId);
      assertNotNull(qip);

      boolean foundQueryMaster = false;
      for(TajoWorker eachWorker: testingCluster.getTajoWorkers()) {
        QueryMasterTask queryMasterTask =
            eachWorker.getWorkerContext().getQueryMaster().getQueryMasterTask(queryId, true);

        if (!foundQueryMaster) {
          foundQueryMaster = queryMasterTask != null;
        }

        if (foundQueryMaster) {
          assertEquals(QueryState.QUERY_KILLED, queryMasterTask.getQuery().getState());
          assertNull("Shuold be removed from running QueryMasterTask.",
              eachWorker.getWorkerContext().getQueryMaster().getQueryMasterTask(queryId, false));
        }
      }

      assertTrue(foundQueryMaster);

      Thread.sleep(10 * 1000);

      for(Worker eachWorker: testingCluster.getMaster().getContext().getResourceManager().getWorkers().values()) {
        assertEquals(0, eachWorker.getResource().getNumRunningTasks());
      }
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.QUERY_SESSION_TIMEOUT.varname,
          ConfVars.QUERY_SESSION_TIMEOUT.defaultVal);
      if (tajoClient != null) {
        tajoClient.close();
      }
    }

  }
}
