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

package org.apache.tajo.querymaster;

import org.apache.tajo.*;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.master.QueryManager;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(IntegrationTest.class)
public class TestQueryState {
  private static TajoTestingCluster cluster;
  private static TajoClient client;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    client = cluster.newTajoClient();
  }

  @Test(timeout = 10000)
  public void testSucceededState() throws Exception {
    String queryStr = "select l_orderkey from lineitem group by l_orderkey order by l_orderkey";
    /*
    =======================================================
    Block Id: eb_1429886996479_0001_000001 [LEAF] HASH_SHUFFLE
    Block Id: eb_1429886996479_0001_000002 [INTERMEDIATE] RANGE_SHUFFLE
    Block Id: eb_1429886996479_0001_000003 [ROOT] NONE_SHUFFLE
    Block Id: eb_1429886996479_0001_000004 [TERMINAL]
    =======================================================

    The order of execution:

    1: eb_1429886996479_0001_000001
    2: eb_1429886996479_0001_000002
    3: eb_1429886996479_0001_000003
    4: eb_1429886996479_0001_000004
    */

    ClientProtos.SubmitQueryResponse res = client.executeQuery(queryStr);
    QueryId queryId = new QueryId(res.getQueryId());
    cluster.waitForQuerySubmitted(queryId);

    QueryMasterTask qmt = cluster.getQueryMasterTask(queryId);
    Query query = qmt.getQuery();

    // wait for query complete
    cluster.waitForQueryState(query, TajoProtos.QueryState.QUERY_SUCCEEDED, 100);

    assertEquals(TajoProtos.QueryState.QUERY_SUCCEEDED, qmt.getState());

    assertEquals(TajoProtos.QueryState.QUERY_SUCCEEDED, query.getSynchronizedState());
    assertEquals(TajoProtos.QueryState.QUERY_SUCCEEDED, query.getState());

    assertFalse(query.getStages().isEmpty());
    for (Stage stage : query.getStages()) {
      assertEquals(StageState.SUCCEEDED, stage.getSynchronizedState());
      assertEquals(StageState.SUCCEEDED, stage.getState());
    }

    /* wait for heartbeat from QueryMaster */
    QueryManager queryManager = cluster.getMaster().getContext().getQueryJobManager();
    for (; ; ) {
      if (queryManager.getFinishedQuery(queryId) != null) break;
      else Thread.sleep(100);
    }

    /* get status from TajoMaster */
    assertEquals(TajoProtos.QueryState.QUERY_SUCCEEDED, client.getQueryStatus(queryId).getState());
  }
}
