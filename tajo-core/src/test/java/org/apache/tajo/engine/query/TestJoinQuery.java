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

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class TestJoinQuery extends QueryTestCaseBase {

  public TestJoinQuery(String joinOption) {
    super(TajoConstants.DEFAULT_DATABASE_NAME);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO.varname,
        ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname,
        ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(
        ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);

    if (joinOption.indexOf("NoBroadcast") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO.varname, "false");
      testingCluster.setAllTajoDaemonConfValue(ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname, "-1");
    }

    if (joinOption.indexOf("Hash") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD.varname, String.valueOf(256 * 1048576));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(256 * 1048576));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(256 * 1048576));
    }
    if (joinOption.indexOf("Sort") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD.varname, String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(1));
    }
  }

  @Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {"Hash_NoBroadcast"},
        {"Sort_NoBroadcast"},
        {"Hash"},
        {"Sort"},
    });
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
  public final void testJoinWithMultipleJoinQual1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithMultipleJoinQual2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithMultipleJoinQual3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithMultipleJoinQual4() throws Exception {
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
  public void testOuterJoinAndCaseWhen1() throws Exception {
    executeDDL("oj_table1_ddl.sql", "table1");
    executeDDL("oj_table2_ddl.sql", "table2");
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
    executeString("DROP TABLE table1").close();
    executeString("DROP TABLE table2").close();
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
    // select length(r_comment) as len, *, c_custkey*10 from customer, region order by len,r_regionkey,r_name
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
    /*
    select
      c_custkey,
      empty_orders.o_orderkey,
      empty_orders.o_orderstatus,
      empty_orders.o_orderdate
    from
      customer left outer join empty_orders on c_custkey = o_orderkey
    order by
      c_custkey, o_orderkey;
     */

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
  public final void testLeftOuterJoinWithEmptySubquery1() throws Exception {
    // Empty Null Supplying table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.put(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.put(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3", "4|table11-4", "5|table11-5" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 2);

    data = new String[]{ "1|table11-1", "2|table11-2" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data, 2);

    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.TESTCASE_MIN_TASK_NUM.varname, "2");

      ResultSet res = executeString("select a.id, b.id from table11 a " +
          "left outer join (" +
          "select table12.id from table12 inner join lineitem on table12.id = lineitem.l_orderkey and table12.id > 10) b " +
          "on a.id = b.id order by a.id");

      String expected = "id,id\n" +
          "-------------------------------\n" +
          "1,null\n" +
          "2,null\n" +
          "3,null\n" +
          "4,null\n" +
          "5,null\n";

      assertEquals(expected, resultSetToString(res));
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.TESTCASE_MIN_TASK_NUM.varname,
          ConfVars.TESTCASE_MIN_TASK_NUM.defaultVal);
      executeString("DROP TABLE table11 PURGE").close();
      executeString("DROP TABLE table12 PURGE").close();
    }
  }

  @Test
  public final void testLeftOuterJoinWithEmptySubquery2() throws Exception {
    //Empty Preserved Row table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.put(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.put(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3", "4|table11-4", "5|table11-5" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 2);

    data = new String[]{ "1|table11-1", "2|table11-2" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data, 2);

    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.TESTCASE_MIN_TASK_NUM.varname, "2");

      ResultSet res = executeString("select a.id, b.id from " +
          "(select table12.id, table12.name, lineitem.l_shipdate " +
          "from table12 inner join lineitem on table12.id = lineitem.l_orderkey and table12.id > 10) a " +
          "left outer join table11 b " +
          "on a.id = b.id");

      String expected = "id,id\n" +
          "-------------------------------\n";

      assertEquals(expected, resultSetToString(res));
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.TESTCASE_MIN_TASK_NUM.varname,
          ConfVars.TESTCASE_MIN_TASK_NUM.defaultVal);
      executeString("DROP TABLE table11 PURGE");
      executeString("DROP TABLE table12 PURGE");
    }
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

  @Test
  public final void testJoinWithJson() throws Exception {
    // select length(r_comment) as len, *, c_custkey*10 from customer, region order by len,r_regionkey,r_name
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithJson2() throws Exception {
    /*
    select t.n_nationkey, t.n_name, t.n_regionkey, t.n_comment, ps.ps_availqty, s.s_suppkey
    from (
      select n_nationkey, n_name, n_regionkey, n_comment
      from nation n
      join region r on (n.n_regionkey = r.r_regionkey)
    ) t
    join supplier s on (s.s_nationkey = t.n_nationkey)
    join partsupp ps on (s.s_suppkey = ps.ps_suppkey)
    where t.n_name in ('ARGENTINA','ETHIOPIA', 'MOROCCO');
     */
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinOnMultipleDatabasesWithJson() throws Exception {
    executeString("CREATE DATABASE JOINS");
    assertDatabaseExists("joins");
    executeString("CREATE TABLE JOINS.part_ as SELECT * FROM part");
    assertTableExists("joins.part_");
    executeString("CREATE TABLE JOINS.supplier_ as SELECT * FROM supplier");
    assertTableExists("joins.supplier_");
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE JOINS.part_ PURGE");
    executeString("DROP TABLE JOINS.supplier_ PURGE");
    executeString("DROP DATABASE JOINS");
  }

  @Test
  public final void testJoinAsterisk() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}
