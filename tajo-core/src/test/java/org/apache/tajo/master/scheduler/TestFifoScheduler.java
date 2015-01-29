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

package org.apache.tajo.master.scheduler;

import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.sql.ResultSet;

import static org.junit.Assert.*;

public class TestFifoScheduler {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;
  private static TajoClient client;
  private static String query =
      "select l_orderkey, l_partkey from lineitem group by l_orderkey, l_partkey order by l_orderkey";

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new TajoTestingCluster();
    cluster.startMiniClusterInLocal(1);
    conf = cluster.getConfiguration();
    client = new TajoClientImpl(cluster.getConfiguration());
    File file = TPCH.getDataFile("lineitem");
    client.executeQueryAndGetResult("create external table default.lineitem (l_orderkey int, l_partkey int) "
        + "using text location 'file://" + file.getAbsolutePath() + "'");
    assertTrue(client.existTable("default.lineitem"));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (client != null) client.close();
    if (cluster != null) cluster.shutdownMiniCluster();
  }

  @Test
  public final void testKillScheduledQuery() throws Exception {
    ClientProtos.SubmitQueryResponse res = client.executeQuery(query);
    ClientProtos.SubmitQueryResponse res2 = client.executeQuery(query);
    QueryId queryId = new QueryId(res.getQueryId());
    QueryId queryId2 = new QueryId(res2.getQueryId());

    cluster.waitForQuerySubmitted(queryId);
    client.killQuery(queryId2);
    assertEquals(TajoProtos.QueryState.QUERY_KILLED, client.getQueryStatus(queryId2).getState());
  }

  @Test
  public final void testForwardedQuery() throws Exception {
    ClientProtos.SubmitQueryResponse res = client.executeQuery(query);
    ClientProtos.SubmitQueryResponse res2 = client.executeQuery("select * from lineitem limit 1");
    assertTrue(res.getIsForwarded());
    assertFalse(res2.getIsForwarded());

    QueryId queryId = new QueryId(res.getQueryId());
    QueryId queryId2 = new QueryId(res2.getQueryId());
    cluster.waitForQuerySubmitted(queryId);

    assertEquals(TajoProtos.QueryState.QUERY_SUCCEEDED, client.getQueryStatus(queryId2).getState());
    ResultSet resSet = TajoClientUtil.createResultSet(conf, client, res2);
    assertNotNull(resSet);
  }

  @Test
  public final void testScheduledQuery() throws Exception {
    ClientProtos.SubmitQueryResponse res = client.executeQuery("select sleep(1) from lineitem");
    ClientProtos.SubmitQueryResponse res2 = client.executeQuery(query);
    ClientProtos.SubmitQueryResponse res3 = client.executeQuery(query);
    ClientProtos.SubmitQueryResponse res4 = client.executeQuery(query);

    QueryId queryId = new QueryId(res.getQueryId());
    QueryId queryId2 = new QueryId(res2.getQueryId());
    QueryId queryId3 = new QueryId(res3.getQueryId());
    QueryId queryId4 = new QueryId(res4.getQueryId());

    cluster.waitForQuerySubmitted(queryId);

    assertFalse(TajoClientUtil.isQueryComplete(client.getQueryStatus(queryId).getState()));

    assertEquals(TajoProtos.QueryState.QUERY_MASTER_INIT, client.getQueryStatus(queryId2).getState());
    assertEquals(TajoProtos.QueryState.QUERY_MASTER_INIT, client.getQueryStatus(queryId3).getState());
    assertEquals(TajoProtos.QueryState.QUERY_MASTER_INIT, client.getQueryStatus(queryId4).getState());

    client.killQuery(queryId4);
    client.killQuery(queryId3);
    client.killQuery(queryId2);
  }
}
