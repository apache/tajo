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

package org.apache.tajo.engine.query;

import org.apache.tajo.*;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.exception.QueryNotFoundException;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.master.GlobalEngine;
import org.apache.tajo.master.QueryInfo;
import org.apache.tajo.master.QueryManager;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.session.Session;
import org.apache.tajo.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestSimpleQuery extends QueryTestCaseBase {
  private static String table;
  private static String partitionedTable;
  private NodeType nodeType;
  private String testTable;

  public TestSimpleQuery(NodeType nodeType) throws IOException {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
    this.nodeType = nodeType;
    if (nodeType == NodeType.SCAN) {
      testTable = table;
    } else if (nodeType == NodeType.PARTITIONS_SCAN) {
      testTable = partitionedTable;
    }
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        //type
        {NodeType.SCAN},
        {NodeType.PARTITIONS_SCAN},
    });
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    createTestTable();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    client.dropTable(table, true);
    client.dropTable(partitionedTable, true);
  }

  private static void createTestTable() throws Exception {
    partitionedTable = IdentifierUtil.normalizeIdentifier("TestSimpleQuery_Partitioned");
    client.executeQueryAndGetResult("create table " + partitionedTable +
        " (col4 text)  partition by column(col1 int4, col2 int4, col3 float8) "
        + "as select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");

    table = IdentifierUtil.normalizeIdentifier("TestSimpleQuery");
    client.executeQueryAndGetResult("create table " + table
        + " (col4 text, col1 int4, col2 int4, col3 float8) "
        + "as select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
  }

  @Test
  public final void testNoWhere() throws Exception {
    String query = "select * from " + testTable;

    isSimpleQuery(query, true);
    hasQueryMaster(query, false);
    ResultSet res = executeString(query);
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLimit() throws Exception {
    String query = "select * from " + testTable + " limit 1";

    isSimpleQuery(query, true);
    hasQueryMaster(query, false);
    ResultSet res = executeString(query);
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhere() throws Exception {
    String query = "select * from " + testTable + " where col4 = 'R' and col1 = 3";

    isSimpleQuery(query, false);
    hasQueryMaster(query, true);
    ResultSet res = executeString(query);
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testPartitionColumnWhere() throws Exception {
    String query = "select * from " + testTable + " where col1 = 1 and (col3 = 36.0 or col3 = 17.0) ";

    if (nodeType == NodeType.SCAN) {
      isSimpleQuery(query, false);
      hasQueryMaster(query, true);
    } else {
      isSimpleQuery(query, true);
      hasQueryMaster(query, false);
    }

    ResultSet res = executeString(query);
    assertResultSet(res);
    cleanupQuery(res);
  }

  private void isSimpleQuery(String queryStr, boolean expected) throws Exception {
    GlobalEngine globalEngine = testingCluster.getMaster().getContext().getGlobalEngine();

    QueryContext queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    Session session = testingCluster.getMaster().getContext().getSessionManager().getSession(client.getSessionId());
    LogicalPlan plan = globalEngine.getLogicalPlanner().
        createPlan(queryContext, globalEngine.buildExpressionFromSql(queryStr, session));

    globalEngine.getLogicalOptimizer().optimize(plan);
    assertEquals(expected, PlannerUtil.checkIfSimpleQuery(plan));
  }

  private void hasQueryMaster(String queryStr, boolean expected) throws QueryNotFoundException {
    ClientProtos.SubmitQueryResponse res = client.executeQuery(queryStr);
    QueryId queryId = new QueryId(res.getQueryId());

    QueryManager queryManager = testingCluster.getMaster().getContext().getQueryJobManager();
    if (expected) {
      assertEquals(ClientProtos.SubmitQueryResponse.ResultType.FETCH, res.getResultType());
      QueryStatus status = TajoClientUtil.waitCompletion(client, queryId);
      assertEquals(TajoProtos.QueryState.QUERY_SUCCEEDED, status.getState());
      client.closeQuery(queryId);
    } else {
      assertEquals(ClientProtos.SubmitQueryResponse.ResultType.ENCLOSED, res.getResultType());
      QueryInfo queryInfo = queryManager.getFinishedQuery(queryId);
      assertNotNull(queryInfo);
      assertTrue(StringUtils.isEmpty(queryInfo.getQueryMasterHost()));
      client.closeQuery(queryId);
    }
  }
}
