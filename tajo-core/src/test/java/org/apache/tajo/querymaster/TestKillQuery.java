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

import com.google.common.collect.Lists;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.master.event.QueryEvent;
import org.apache.tajo.master.event.QueryEventType;
import org.apache.tajo.master.event.StageEvent;
import org.apache.tajo.master.event.StageEventType;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.session.Session;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class TestKillQuery {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;
  private static TajoClient client;
  private static String queryStr = "select t1.l_orderkey, t1.l_partkey, t2.c_custkey " +
      "from lineitem t1 join customer t2 " +
      "on t1.l_orderkey = t2.c_custkey order by t1.l_orderkey";

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

    file = TPCH.getDataFile("customer");
    client.executeQueryAndGetResult("create external table default.customer (c_custkey int, c_name text) "
        + "using text location 'file://" + file.getAbsolutePath() + "'");
    assertTrue(client.existTable("default.customer"));
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (client != null) client.close();
    if (cluster != null) cluster.shutdownMiniCluster();
  }

  @Test
  public final void testKillQueryFromInitState() throws Exception {
    SQLAnalyzer analyzer = new SQLAnalyzer();
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(conf);
    Session session = LocalTajoTestingUtility.createDummySession();
    CatalogService catalog = cluster.getMaster().getCatalog();

    LogicalPlanner planner = new LogicalPlanner(catalog);
    LogicalOptimizer optimizer = new LogicalOptimizer(conf);
    Expr expr =  analyzer.parse(queryStr);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);

    optimizer.optimize(plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    QueryContext queryContext = new QueryContext(conf);
    MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
    GlobalPlanner globalPlanner = new GlobalPlanner(conf, catalog);
    globalPlanner.build(masterPlan);

    QueryMaster qm = cluster.getTajoWorkers().get(0).getWorkerContext().getQueryMaster();
    QueryMasterTask queryMasterTask = new QueryMasterTask(qm.getContext(),
        queryId, session, defaultContext, expr.toJson());

    queryMasterTask.init(conf);
    queryMasterTask.getQueryTaskContext().getDispatcher().start();
    queryMasterTask.startQuery();

    try{
      cluster.waitForQueryState(queryMasterTask.getQuery(), TajoProtos.QueryState.QUERY_RUNNING, 2);
    } finally {
      assertEquals(TajoProtos.QueryState.QUERY_RUNNING, queryMasterTask.getQuery().getSynchronizedState());
    }

    Stage stage = queryMasterTask.getQuery().getStages().iterator().next();
    assertNotNull(stage);

    try{
      cluster.waitForStageState(stage, StageState.INITED, 2);
    } finally {
      assertEquals(StageState.INITED, stage.getSynchronizedState());
    }

    // fire kill event
    Query q = queryMasterTask.getQuery();
    q.handle(new QueryEvent(queryId, QueryEventType.KILL));

    try {
      cluster.waitForQueryState(queryMasterTask.getQuery(), TajoProtos.QueryState.QUERY_KILLED, 50);
      assertEquals(TajoProtos.QueryState.QUERY_KILLED, queryMasterTask.getQuery().getSynchronizedState());
    } catch (Exception e) {
      e.printStackTrace();
      if (stage != null) {
        System.err.println(String.format("Stage: [%s] (Total: %d, Complete: %d, Success: %d, Killed: %d, Failed: %d)",
            stage.getId().toString(),
            stage.getTotalScheduledObjectsCount(),
            stage.getCompletedTaskCount(),
            stage.getSucceededObjectCount(),
            stage.getKilledObjectCount(),
            stage.getFailedObjectCount()));
      }
      throw e;
    } finally {
      queryMasterTask.stop();
    }
  }

  @Test
  public final void testIgnoreStageStateFromKilled() throws Exception {

    ClientProtos.SubmitQueryResponse res = client.executeQuery(queryStr);
    QueryId queryId = new QueryId(res.getQueryId());
    cluster.waitForQuerySubmitted(queryId);

    QueryMasterTask qmt = cluster.getQueryMasterTask(queryId);
    Query query = qmt.getQuery();

    // wait for a stage created
    cluster.waitForQueryState(query, TajoProtos.QueryState.QUERY_RUNNING, 10);
    query.handle(new QueryEvent(queryId, QueryEventType.KILL));

    try{
      cluster.waitForQueryState(query, TajoProtos.QueryState.QUERY_KILLED, 50);
    } finally {
      assertEquals(TajoProtos.QueryState.QUERY_KILLED, query.getSynchronizedState());
    }

    List<Stage> stages = Lists.newArrayList(query.getStages());
    Stage lastStage = stages.get(stages.size() - 1);

    assertEquals(StageState.KILLED, lastStage.getSynchronizedState());

    lastStage.getStateMachine().doTransition(StageEventType.SQ_START,
        new StageEvent(lastStage.getId(), StageEventType.SQ_START));

    lastStage.getStateMachine().doTransition(StageEventType.SQ_KILL,
        new StageEvent(lastStage.getId(), StageEventType.SQ_KILL));

    lastStage.getStateMachine().doTransition(StageEventType.SQ_CONTAINER_ALLOCATED,
        new StageEvent(lastStage.getId(), StageEventType.SQ_CONTAINER_ALLOCATED));

    lastStage.getStateMachine().doTransition(StageEventType.SQ_SHUFFLE_REPORT,
        new StageEvent(lastStage.getId(), StageEventType.SQ_SHUFFLE_REPORT));

    lastStage.getStateMachine().doTransition(StageEventType.SQ_STAGE_COMPLETED,
        new StageEvent(lastStage.getId(), StageEventType.SQ_STAGE_COMPLETED));

    lastStage.getStateMachine().doTransition(StageEventType.SQ_FAILED,
        new StageEvent(lastStage.getId(), StageEventType.SQ_FAILED));
  }
}
