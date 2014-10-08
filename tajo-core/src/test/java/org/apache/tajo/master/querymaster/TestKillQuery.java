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

import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalOptimizer;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.event.QueryEvent;
import org.apache.tajo.master.event.QueryEventType;
import org.apache.tajo.master.session.Session;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(IntegrationTest.class)
public class TestKillQuery {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    conf = cluster.getConfiguration();
  }

  @Test
  public final void testKillQueryFromInitState() throws Exception {
    SQLAnalyzer analyzer = new SQLAnalyzer();
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(conf);
    Session session = LocalTajoTestingUtility.createDummySession();
    CatalogService catalog = cluster.getMaster().getCatalog();
    String query = "select l_orderkey from lineitem group by l_orderkey";

    LogicalPlanner planner = new LogicalPlanner(catalog);
    LogicalOptimizer optimizer = new LogicalOptimizer(conf, catalog);
    Expr expr =  analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(defaultContext, expr);

    optimizer.optimize(plan);

    QueryId queryId = QueryIdFactory.newQueryId(System.currentTimeMillis(), 0);
    QueryContext queryContext = new QueryContext(conf);
    MasterPlan masterPlan = new MasterPlan(queryId, queryContext, plan);
    GlobalPlanner globalPlanner = new GlobalPlanner(conf, catalog);
    globalPlanner.build(masterPlan);

    QueryMaster qm = cluster.getTajoWorkers().get(0).getWorkerContext().getQueryMaster();
    QueryMasterTask queryMasterTask = new QueryMasterTask(qm.getContext(),
        queryId, session, defaultContext, expr.toJson(), plan.getRootBlock().getRoot().toJson());

    queryMasterTask.init(conf);
    queryMasterTask.getQueryTaskContext().getDispatcher().start();
    queryMasterTask.startQuery();

    try{
      cluster.waitForQueryState(queryMasterTask.getQuery(), TajoProtos.QueryState.QUERY_RUNNING, 2);
    } finally {
      assertEquals(TajoProtos.QueryState.QUERY_RUNNING, queryMasterTask.getQuery().getSynchronizedState());
    }

    SubQuery subQuery = queryMasterTask.getQuery().getSubQueries().iterator().next();
    assertNotNull(subQuery);

    try{
      cluster.waitForSubQueryState(subQuery, SubQueryState.INITED, 2);
    } finally {
      assertEquals(SubQueryState.INITED, subQuery.getSynchronizedState());
    }

    // fire kill event
    Query q = queryMasterTask.getQuery();
    q.handle(new QueryEvent(queryId, QueryEventType.KILL));

    try{
      cluster.waitForQueryState(queryMasterTask.getQuery(), TajoProtos.QueryState.QUERY_KILLED, 10);
    } finally {
      assertEquals(TajoProtos.QueryState.QUERY_KILLED, queryMasterTask.getQuery().getSynchronizedState());
    }
    queryMasterTask.stop();
  }
}
