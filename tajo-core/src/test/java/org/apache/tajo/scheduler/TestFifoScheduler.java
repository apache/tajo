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

package org.apache.tajo.scheduler;

import org.apache.tajo.*;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestFifoScheduler {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;
  private static TajoClient client;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    conf = cluster.getConfiguration();
    client = new TajoClient(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.close();
  }

  @Test
  public final void testKillScheduledQuery() throws Exception {
    ClientProtos.SubmitQueryResponse res = client.executeQuery("select sleep(1) from lineitem");
    ClientProtos.SubmitQueryResponse res2 = client.executeQuery("select sleep(1) from lineitem");
    QueryId queryId = new QueryId(res.getQueryId());
    QueryId queryId2 = new QueryId(res2.getQueryId());

    cluster.waitForQueryRunning(queryId);
    client.killQuery(queryId2);
    assertEquals(TajoProtos.QueryState.QUERY_KILLED, client.getQueryStatus(queryId2).getState());

    client.killQuery(queryId); // cleanup
  }

  @Test
  public final void testForwardedQuery() throws Exception {
    ClientProtos.SubmitQueryResponse res = client.executeQuery("select sleep(1) from lineitem");
    ClientProtos.SubmitQueryResponse res2 = client.executeQuery("select * from lineitem limit 1");
    assertTrue(res.getIsForwarded());
    assertFalse(res2.getIsForwarded());

    QueryId queryId = new QueryId(res.getQueryId());
    QueryId queryId2 = new QueryId(res2.getQueryId());
    cluster.waitForQueryRunning(queryId);

    assertEquals(TajoProtos.QueryState.QUERY_SUCCEEDED, client.getQueryStatus(queryId2).getState());
    ResultSet resSet = TajoClient.createResultSet(client, res2);
    assertNotNull(resSet);

    client.killQuery(queryId); //cleanup
  }

  @Test
  public final void testScheduledQuery() throws Exception {
    ClientProtos.SubmitQueryResponse res = client.executeQuery("select sleep(1) from lineitem");
    ClientProtos.SubmitQueryResponse res2 = client.executeQuery("select sleep(1) from lineitem");
    ClientProtos.SubmitQueryResponse res3 = client.executeQuery("select sleep(1) from lineitem");
    ClientProtos.SubmitQueryResponse res4 = client.executeQuery("select sleep(1) from lineitem");

    QueryId queryId = new QueryId(res.getQueryId());
    QueryId queryId2 = new QueryId(res2.getQueryId());
    QueryId queryId3 = new QueryId(res3.getQueryId());
    QueryId queryId4 = new QueryId(res4.getQueryId());

    cluster.waitForQueryRunning(queryId);

    assertTrue(TajoClient.isInRunningState(client.getQueryStatus(queryId).getState()));

    assertEquals(TajoProtos.QueryState.QUERY_MASTER_INIT, client.getQueryStatus(queryId2).getState());
    assertEquals(TajoProtos.QueryState.QUERY_MASTER_INIT, client.getQueryStatus(queryId3).getState());
    assertEquals(TajoProtos.QueryState.QUERY_MASTER_INIT, client.getQueryStatus(queryId4).getState());

    client.killQuery(queryId2);
    client.killQuery(queryId3);
    client.killQuery(queryId4);
    client.killQuery(queryId);
  }
}
