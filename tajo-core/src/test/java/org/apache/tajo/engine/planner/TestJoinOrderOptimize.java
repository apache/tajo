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

package org.apache.tajo.engine.planner;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestJoinOrderOptimize extends QueryTestCaseBase {

  public TestJoinOrderOptimize() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @BeforeClass
  public static void classSetUp() throws Exception {
    testingCluster.getConfiguration().set(TajoConf.ConfVars.$TEST_PLAN_SHAPE_FIX_ENABLED.varname, "true");
  }

  @AfterClass
  public static void classTearDown() {
    testingCluster.getConfiguration().set(TajoConf.ConfVars.$TEST_PLAN_SHAPE_FIX_ENABLED.varname, "false");
  }

  private void createOuterJoinTestTable() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{ "1|table12-1" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{"2|table13-2", "3|table13-3" };
    TajoTestingCluster.createTable("table13", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{"1|table14-1", "2|table14-2", "3|table14-3", "4|table14-4" };
    TajoTestingCluster.createTable("table14", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    data = new String[]{};
    TajoTestingCluster.createTable("table15", schema, tableOptions, data);
  }

  private void dropOuterJoinTestTable() throws Exception {
    executeString("DROP TABLE table11 PURGE;");
    executeString("DROP TABLE table12 PURGE;");
    executeString("DROP TABLE table13 PURGE;");
    executeString("DROP TABLE table14 PURGE;");
    executeString("DROP TABLE table15 PURGE;");
  }

  @Test
  public final void testCrossJoin() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testCrossJoinWithThetaJoinConditionInWhere() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInnerJoinWithThetaJoinConditionInWhere() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithThetaJoinConditionInWhere() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testRightOuterJoinWithThetaJoinConditionInWhere() throws Exception {
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
    assertResultSet(res);
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
  public final void testJoinWithMultipleJoinTypes() throws Exception {
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
  public final void testLeftOuterJoinWithConstantExpr4() throws Exception {
    // outer join with constant projections
    //
    // select
    //   c_custkey,
    //   orders.o_orderkey,
    //   1 as key1
    // from customer left outer join orders on c_custkey = o_orderkey and key1 = 1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithConstantExpr5() throws Exception {
    // outer join with constant projections
    //
    // select
    //   c_custkey,
    //   orders.o_orderkey,
    //   1 as key1
    // from customer left outer join orders on c_custkey = o_orderkey and key1 = 1;
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
  public void testInnerJoinAndCaseWhen() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testComplexJoinsWithCaseWhen() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testComplexJoinsWithCaseWhen2() throws Exception {
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
  public final void testLeftOuterJoinWithEmptyTable5() throws Exception {
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

  @Test
  public final void testJoinAsterisk() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithNull1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithNull2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithNull3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id and t2.id = t3.id");

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase2() throws Exception {
    // outer -> outer -> inner
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
              "from table12 t2\n" +
              "left outer join table13 t3\n" +
              "on t2.id = t3.id\n" +
              "left outer join table11 t1\n" +
              "on t3.id = t1.id\n" +
              "inner join table14 t4\n" +
              "on t2.id = t4.id"
      );

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase2_1() throws Exception {
    // inner(on predication) -> outer(on predication) -> outer -> where
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "inner join table14 t4\n" +
              "on t1.id = t4.id and t4.id > 1\n" +
              "left outer join table13 t3\n" +
              "on t4.id = t3.id and t3.id = 2\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id \n" +
              "where t1.id > 1"
      );

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase3() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case J1: Join Predicate on Preserved Row Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2 \n" +
              "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id "
      );

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase4() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case J2: Join Predicate on Null Supplying Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id and t2.id > 1 \n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id"
      );

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase5() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W1: Where Predicate on Preserved Row Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "where t1.name > 'table11-1'"
      );

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase6() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W2: Where Predicate on Null Supplying Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "where t3.id > 2"
      );

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterWithEmptyTable() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W2: Where Predicate on Null Supplying Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id\n" +
              "from table11 t1\n" +
              "left outer join table15 t2\n" +
              "on t1.id = t2.id"
      );

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testRightOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "right outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "right outer join table13 t3\n" +
              "on t1.id = t3.id and t2.id = t3.id"
      );

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testRightOuterJoinPredicationCaseByCase2() throws Exception {
    // inner -> right
    // Notice: Join order should be preserved with origin order.
    // JoinEdge: t1 -> t4, t3 -> t1,t4
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "inner join table14 t4\n" +
              "on t1.id = t4.id and t4.id > 1\n" +
              "right outer join table13 t3\n" +
              "on t4.id = t3.id and t3.id = 2\n" +
              "where t3.id > 1"
      );
      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testRightOuterJoinPredicationCaseByCase3() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "right outer join table12 t2 \n" +
              "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
              "right outer join table13 t3\n" +
              "on t1.id = t3.id "
      );

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testFullOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();

    try {
      ResultSet res = executeString(
          "explain global select t1.id, t1.name, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "full outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "full outer join table14 t4\n" +
              "on t3.id = t4.id \n" +
              "order by t4.id"
      );

      assertResultSet(res);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testComplexJoinCondition1() throws Exception {
    // select n1.n_nationkey, n1.n_name, n2.n_name  from nation n1 join nation n2 on n1.n_name = upper(n2.n_name);
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testComplexJoinCondition2() throws Exception {
    // select n1.n_nationkey, n1.n_name, upper(n2.n_name) name from nation n1 join nation n2
    // on n1.n_name = upper(n2.n_name);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testComplexJoinCondition3() throws Exception {
    // select n1.n_nationkey, n1.n_name, n2.n_name from nation n1 join nation n2 on lower(n1.n_name) = lower(n2.n_name);
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testComplexJoinCondition4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testComplexJoinCondition5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testComplexJoinCondition6() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testComplexJoinCondition7() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testFullOuterJoinWithEmptyIntermediateData() throws Exception {
    ResultSet res = executeString(
        "explain global select a.l_orderkey \n" +
            "from (select * from lineitem where l_orderkey < 0) a\n" +
            "full outer join (select * from lineitem where l_orderkey < 0) b\n" +
            "on a.l_orderkey = b.l_orderkey"
    );

    try {
      assertResultSet(res);
    } finally {
      cleanupQuery(res);
    }
  }

  @Test
  public final void testJoinFilterOfRowPreservedTable1() throws Exception {
    // this test is for join filter of a row preserved table.
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithOrPredicates() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}
