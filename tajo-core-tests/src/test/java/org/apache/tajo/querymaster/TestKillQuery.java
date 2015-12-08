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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.tajo.*;
import org.apache.tajo.ResourceProtos.ExecutionBlockContextResponse;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.engine.query.TaskRequestImpl;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.event.QueryEvent;
import org.apache.tajo.master.event.QueryEventType;
import org.apache.tajo.master.event.StageEvent;
import org.apache.tajo.master.event.StageEventType;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.session.Session;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.worker.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
    client = cluster.newTajoClient();
    client.executeQueryAndGetResult("create external table default.lineitem (l_orderkey int, l_partkey int) "
        + "using text location '" + TpchTestBase.getInstance().getPath("lineitem") + "'");
    assertTrue(client.existTable("default.lineitem"));

    client.executeQueryAndGetResult("create external table default.customer (c_custkey int, c_name text) "
        + "using text location '" + TpchTestBase.getInstance().getPath("customer") + "'");
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

    LogicalPlanner planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    LogicalOptimizer optimizer = new LogicalOptimizer(conf, catalog, TablespaceManager.getInstance());
    Expr expr =  analyzer.parse(queryStr);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);

    optimizer.optimize(plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    QueryContext queryContext = new QueryContext(conf);
    MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
    GlobalPlanner globalPlanner = new GlobalPlanner(conf, catalog);
    globalPlanner.build(queryContext, masterPlan);

    CountDownLatch barrier  = new CountDownLatch(1);
    MockAsyncDispatch dispatch = new MockAsyncDispatch(barrier, StageEventType.SQ_INIT);

    QueryMaster qm = cluster.getTajoWorkers().get(0).getWorkerContext().getQueryMaster();
    QueryMasterTask queryMasterTask = new QueryMasterTask(qm.getContext(),
        queryId, session, defaultContext, expr.toJson(), NodeResources.createResource(512), dispatch);

    queryMasterTask.init(conf);
    queryMasterTask.getQueryTaskContext().getDispatcher().start();
    queryMasterTask.startQuery();

    try{
      barrier.await(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      fail("Query state : " + queryMasterTask.getQuery().getSynchronizedState());
    }

    Stage stage = queryMasterTask.getQuery().getStages().iterator().next();
    assertNotNull(stage);

    // fire kill event
    queryMasterTask.getEventHandler().handle(new QueryEvent(queryId, QueryEventType.KILL));

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

    SQLAnalyzer analyzer = new SQLAnalyzer();
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(conf);
    Session session = LocalTajoTestingUtility.createDummySession();
    CatalogService catalog = cluster.getMaster().getCatalog();

    LogicalPlanner planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    LogicalOptimizer optimizer = new LogicalOptimizer(conf, catalog, TablespaceManager.getInstance());
    Expr expr =  analyzer.parse(queryStr);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);

    optimizer.optimize(plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    QueryContext queryContext = new QueryContext(conf);
    MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
    GlobalPlanner globalPlanner = new GlobalPlanner(conf, catalog);
    globalPlanner.build(queryContext, masterPlan);

    CountDownLatch barrier  = new CountDownLatch(1);
    MockAsyncDispatch dispatch = new MockAsyncDispatch(barrier, TajoProtos.QueryState.QUERY_RUNNING);

    QueryMaster qm = cluster.getTajoWorkers().get(0).getWorkerContext().getQueryMaster();
    QueryMasterTask queryMasterTask = new QueryMasterTask(qm.getContext(),
        queryId, session, defaultContext, expr.toJson(), NodeResources.createResource(512), dispatch);

    queryMasterTask.init(conf);
    queryMasterTask.getQueryTaskContext().getDispatcher().start();
    queryMasterTask.startQuery();

    try{
      barrier.await(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      fail("Query state : " + queryMasterTask.getQuery().getSynchronizedState());
    }

    Stage stage = queryMasterTask.getQuery().getStages().iterator().next();
    assertNotNull(stage);

    // fire kill event
    queryMasterTask.getEventHandler().handle(new QueryEvent(queryId, QueryEventType.KILL));

    try {
      cluster.waitForQueryState(queryMasterTask.getQuery(), TajoProtos.QueryState.QUERY_KILLED, 50);
      assertEquals(TajoProtos.QueryState.QUERY_KILLED, queryMasterTask.getQuery().getSynchronizedState());
    }   finally {
      queryMasterTask.stop();
    }

    List<Stage> stages = Lists.newArrayList(queryMasterTask.getQuery().getStages());
    Stage lastStage = stages.get(stages.size() - 1);

    assertEquals(StageState.KILLED, lastStage.getSynchronizedState());

    lastStage.getStateMachine().doTransition(StageEventType.SQ_START,
        new StageEvent(lastStage.getId(), StageEventType.SQ_START));

    lastStage.getStateMachine().doTransition(StageEventType.SQ_KILL,
        new StageEvent(lastStage.getId(), StageEventType.SQ_KILL));

    lastStage.getStateMachine().doTransition(StageEventType.SQ_SHUFFLE_REPORT,
        new StageEvent(lastStage.getId(), StageEventType.SQ_SHUFFLE_REPORT));

    lastStage.getStateMachine().doTransition(StageEventType.SQ_STAGE_COMPLETED,
        new StageEvent(lastStage.getId(), StageEventType.SQ_STAGE_COMPLETED));

    lastStage.getStateMachine().doTransition(StageEventType.SQ_FAILED,
        new StageEvent(lastStage.getId(), StageEventType.SQ_FAILED));
  }

  @Test
  public void testKillTask() throws Throwable {
    QueryId qid = LocalTajoTestingUtility.newQueryId();
    ExecutionBlockId eid = QueryIdFactory.newExecutionBlockId(qid, 1);
    TaskId tid = QueryIdFactory.newTaskId(eid);
    final TajoConf conf = new TajoConf();
    TaskRequestImpl taskRequest = new TaskRequestImpl();
    WorkerConnectionInfo queryMaster = new WorkerConnectionInfo("host", 28091, 28092, 21000, 28093, 28080);

    TaskAttemptId attemptId = new TaskAttemptId(tid, 1);
    taskRequest.set(attemptId, new ArrayList<>(),
        null, false, PlanProto.LogicalNodeTree.newBuilder().build(), new QueryContext(conf),
        null, null, queryMaster.getHostAndQMPort());
    taskRequest.setInterQuery();


    ExecutionBlockContextResponse.Builder requestProtoBuilder =
        ExecutionBlockContextResponse.newBuilder();
    requestProtoBuilder.setExecutionBlockId(eid.getProto())
        .setPlanJson("test")
        .setQueryContext(new QueryContext(conf).getProto())
        .setQueryOutputPath("testpath")
        .setShuffleType(PlanProto.ShuffleType.HASH_SHUFFLE);

    TajoWorker.WorkerContext workerContext = new MockWorkerContext() {
      @Override
      public TajoConf getConf() {
        return conf;
      }

      @Override
      public TaskManager getTaskManager() {
        return null;
      }

      @Override
      public TaskExecutor getTaskExecuor() {
        return null;
      }

      @Override
      public NodeResourceManager getNodeResourceManager() {
        return null;
      }
    };

    ExecutionBlockContext context = new MockExecutionBlock(workerContext, requestProtoBuilder.build()) {
      @Override
      public Path createBaseDir() throws IOException {
        return new Path("test");
      }
    };

    org.apache.tajo.worker.Task task = new TaskImpl(taskRequest, context);
    task.kill();
    assertEquals(TajoProtos.TaskAttemptState.TA_KILLED, task.getTaskContext().getState());
    try {
      task.run();
      assertEquals(TajoProtos.TaskAttemptState.TA_KILLED, task.getTaskContext().getState());
    } catch (Exception e) {
      assertEquals(TajoProtos.TaskAttemptState.TA_KILLED, task.getTaskContext().getState());
    }
  }

  static class MockAsyncDispatch extends AsyncDispatcher {
    private CountDownLatch latch;
    private Enum eventType;

    MockAsyncDispatch(CountDownLatch latch, Enum eventType) {
      super();
      this.latch = latch;
      this.eventType = eventType;
    }

    @Override
    protected void dispatch(Event event) {
      if (event.getType() == eventType) {
        latch.countDown();
      }
      super.dispatch(event);
    }
  }
}
