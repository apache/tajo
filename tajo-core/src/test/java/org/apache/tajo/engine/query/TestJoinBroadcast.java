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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.jdbc.TajoResultSet;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.worker.TajoWorker;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.ResultSet;

import static junit.framework.TestCase.*;
import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertNotNull;

@Category(IntegrationTest.class)
public class TestJoinBroadcast extends QueryTestCaseBase {
  private static final Log LOG = LogFactory.getLog(TestJoinBroadcast.class);
  public TestJoinBroadcast() throws Exception {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO.varname, "true");
    testingCluster.setAllTajoDaemonConfValue(
        TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname, "" + (5 * 1024));

    executeDDL("create_lineitem_large_ddl.sql", "lineitem_large");
    executeDDL("create_customer_large_ddl.sql", "customer_large");
  }

  @Test
  public final void testCrossJoin() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin6() throws Exception {
    ResultSet res = executeQuery();
    System.out.println(resultSetToString(res));
    cleanupQuery(res);
  }

  @Test
  public final void testTPCHQ2Join() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoin1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithConstantExpr1() throws Exception {
    // outer join with constant projections
    //
    // select c_custkey, orders.o_orderkey, 'val' as val from customer
    // left outer join orders on c_custkey = o_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithConstantExpr2() throws Exception {
    // outer join with constant projections
    //
    // select c_custkey, o.o_orderkey, 'val' as val from customer left outer join
    // (select * from orders) o on c_custkey = o.o_orderkey
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithConstantExpr3() throws Exception {
    // outer join with constant projections
    //
    // select a.c_custkey, 123::INT8 as const_val, b.min_name from customer a
    // left outer join ( select c_custkey, min(c_name) as min_name from customer group by c_custkey) b
    // on a.c_custkey = b.c_custkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testRightOuterJoin1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testFullOuterJoin1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testJoinCoReferredEvals1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testJoinCoReferredEvalsWithSameExprs1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testJoinCoReferredEvalsWithSameExprs2() throws Exception {
    // including grouping operator
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testCrossJoinAndCaseWhen() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testCrossJoinWithAsterisk1() throws Exception {
    // select region.*, customer.* from region, customer;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testCrossJoinWithAsterisk2() throws Exception {
    // select region.*, customer.* from customer, region;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testCrossJoinWithAsterisk3() throws Exception {
    // select * from customer, region
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testCrossJoinWithAsterisk4() throws Exception {
    // select length(r_regionkey), *, c_custkey*10 from customer, region
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInnerJoinWithEmptyTable() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithEmptyTable1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithEmptyTable2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithEmptyTable3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithEmptyTable4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testRightOuterJoinWithEmptyTable1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testFullOuterJoinWithEmptyTable1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testCrossJoinWithEmptyTable1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinOnMultipleDatabases() throws Exception {
    executeString("CREATE DATABASE JOINS");
    assertDatabaseExists("joins");
    executeString("CREATE TABLE JOINS.part_ as SELECT * FROM part");
    assertTableExists("joins.part_");
    executeString("CREATE TABLE JOINS.supplier_ as SELECT * FROM supplier");
    assertTableExists("joins.supplier_");
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
    executeString("DROP TABLE JOINS.part_ PURGE");
    executeString("DROP TABLE JOINS.supplier_ PURGE");
    executeString("DROP DATABASE JOINS");
  }

  private MasterPlan getQueryPlan(QueryId queryId) {
    for (TajoWorker eachWorker: testingCluster.getTajoWorkers()) {
      QueryMasterTask queryMasterTask = eachWorker.getWorkerContext().getQueryMaster().getQueryMasterTask(queryId, true);
      if (queryMasterTask != null) {
        return queryMasterTask.getQuery().getPlan();
      }
    }

    fail("Can't find query from workers" + queryId);
    return null;
  }

  @Test
  public final void testBroadcastBasicJoin() throws Exception {
    ResultSet res = executeQuery();
    TajoResultSet ts = (TajoResultSet)res;
    assertResultSet(res);
    cleanupQuery(res);

    MasterPlan plan = getQueryPlan(ts.getQueryId());
    ExecutionBlock rootEB = plan.getRoot();

    /*
    |-eb_1395998037360_0001_000006
       |-eb_1395998037360_0001_000005
     */
    assertEquals(1, plan.getChildCount(rootEB.getId()));

    ExecutionBlock firstEB = plan.getChild(rootEB.getId(), 0);

    assertNotNull(firstEB);
    assertEquals(2, firstEB.getBroadcastTables().size());
    assertTrue(firstEB.getBroadcastTables().contains("default.supplier"));
    assertTrue(firstEB.getBroadcastTables().contains("default.part"));
  }

  @Test
  public final void testBroadcastTwoPartJoin() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    MasterPlan plan = getQueryPlan(((TajoResultSet)res).getQueryId());
    ExecutionBlock rootEB = plan.getRoot();

    /*
    |-eb_1395996354406_0001_000010
       |-eb_1395996354406_0001_000009
          |-eb_1395996354406_0001_000008
          |-eb_1395996354406_0001_000005
     */
    assertEquals(1, plan.getChildCount(rootEB.getId()));

    ExecutionBlock firstJoinEB = plan.getChild(rootEB.getId(), 0);
    assertNotNull(firstJoinEB);
    assertEquals(NodeType.JOIN, firstJoinEB.getPlan().getType());
    assertEquals(0, firstJoinEB.getBroadcastTables().size());

    ExecutionBlock leafEB1 = plan.getChild(firstJoinEB.getId(), 0);
    assertTrue(leafEB1.getBroadcastTables().contains("default.orders"));
    assertTrue(leafEB1.getBroadcastTables().contains("default.part"));

    ExecutionBlock leafEB2 = plan.getChild(firstJoinEB.getId(), 1);
    assertTrue(leafEB2.getBroadcastTables().contains("default.nation"));
  }

  @Test
  public final void testBroadcastSubquery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testBroadcastSubquery2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testBroadcastPartitionTable() throws Exception {
    // If all tables participate in the BROADCAST JOIN, there is some missing data.
    executeDDL("customer_partition_ddl.sql", null);
    ResultSet res = executeFile("insert_into_customer_partition.sql");
    res.close();

    createMultiFile("nation", 2, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new TextDatum(columnDatas[1]),
            new Int4Datum(Integer.parseInt(columnDatas[2])),
            new TextDatum(columnDatas[3])
        });
      }
    });

    createMultiFile("orders", 1, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new Int4Datum(Integer.parseInt(columnDatas[1])),
            new TextDatum(columnDatas[2])
        });
      }
    });

    res = executeQuery();
    assertResultSet(res);
    res.close();

    executeString("DROP TABLE customer_broad_parts PURGE");
    executeString("DROP TABLE nation_multifile PURGE");
    executeString("DROP TABLE orders_multifile PURGE");
  }

  @Test
  public final void testBroadcastMultiColumnPartitionTable() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testBroadcastMultiColumnPartitionTable");
    ResultSet res = testBase.execute(
        "create table " + tableName + " (col1 int4, col2 float4) partition by column(col3 text, col4 text) ");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert overwrite into " + tableName
        + " select o_orderkey, o_totalprice, substr(o_orderdate, 6, 2), substr(o_orderdate, 1, 4) from orders");
    res.close();

    res = executeString(
        "select distinct a.col3 from " + tableName + " as a " +
            "left outer join lineitem_large b " +
            "on a.col1 = b.l_orderkey"
    );

    assertResultSet(res);
    cleanupQuery(res);
  }


  static interface TupleCreator {
    public Tuple createTuple(String[] columnDatas);
  }

  private void createMultiFile(String tableName, int numRowsEachFile, TupleCreator tupleCreator) throws Exception {
    // make multiple small file
    String multiTableName = tableName + "_multifile";
    executeDDL(multiTableName + "_ddl.sql", null);

    TableDesc table = client.getTableDesc(multiTableName);
    assertNotNull(table);

    TableMeta tableMeta = table.getMeta();
    Schema schema = table.getLogicalSchema();

    File file = new File("src/test/tpch/" + tableName + ".tbl");

    if (!file.exists()) {
      file = new File(System.getProperty("user.dir") + "/tajo-core/src/test/tpch/" + tableName + ".tbl");
    }
    String[] rows = FileUtil.readTextFile(file).split("\n");

    assertTrue(rows.length > 0);

    int fileIndex = 0;

    Appender appender = null;
    for (int i = 0; i < rows.length; i++) {
      if (i % numRowsEachFile == 0) {
        if (appender != null) {
          appender.flush();
          appender.close();
        }
        Path dataPath = new Path(table.getPath(), fileIndex + ".csv");
        fileIndex++;
        appender = StorageManagerFactory.getStorageManager(conf).getAppender(tableMeta, schema,
            dataPath);
        appender.init();
      }
      String[] columnDatas = rows[i].split("\\|");
      Tuple tuple = tupleCreator.createTuple(columnDatas);
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
  }
}
