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
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class TestOuterJoinQuery extends TestJoinQuery {

  public TestOuterJoinQuery(String joinOption) throws Exception {
    super(joinOption);
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithThetaJoinConditionInWhere() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testRightOuterJoinWithThetaJoinConditionInWhere() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoin1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithConstantExpr1() throws Exception {
    // outer join with constant projections
    //
    // select c_custkey, orders.o_orderkey, 'val' as val from customer
    // left outer join orders on c_custkey = o_orderkey;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithConstantExpr4() throws Exception {
    // outer join with constant projections
    //
    // select
    //   c_custkey,
    //   orders.o_orderkey,
    //   1 as key1
    // from customer left outer join orders on c_custkey = o_orderkey and key1 = 1;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithConstantExpr5() throws Exception {
    // outer join with constant projections
    //
    // select
    //   c_custkey,
    //   orders.o_orderkey,
    //   1 as key1
    // from customer left outer join orders on c_custkey = o_orderkey and key1 = 1;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testRightOuterJoin1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testFullOuterJoin1() throws Exception {
    runSimpleTests();
  }

  @Test
  public void testOuterJoinAndCaseWhen1() throws Exception {
    executeDDL("oj_table1_ddl.sql", "table1");
    executeDDL("oj_table2_ddl.sql", "table2");
    try {
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");
    }
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
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

    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithEmptyTable2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithEmptyTable3() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithEmptyTable4() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithEmptyTable5() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testRightOuterJoinWithEmptyTable1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testFullOuterJoinWithEmptyTable1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithNull1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithNull2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithNull3() throws Exception {
    runSimpleTests();
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id and t2.id = t3.id");

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,1,null\n" +
              "2,table11-2,null,null\n" +
              "3,table11-3,null,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
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
          "select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t2.id = t3.id\n" +
              "inner join table14 t4\n" +
              "on t2.id = t4.id"
      );

      String expected =
          "id,name,id,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,1,null,1\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
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
          "select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "inner join table14 t4\n" +
              "on t1.id = t4.id and t4.id > 1\n" +
              "left outer join table13 t3\n" +
              "on t4.id = t3.id and t3.id = 2\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id \n" +
              "where t1.id > 1"
      );

      String expected =
          "id,name,id,id,id\n" +
              "-------------------------------\n" +
              "2,table11-2,null,2,2\n" +
              "3,table11-3,null,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
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
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2 \n" +
              "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id "
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,1,null\n" +
              "2,table11-2,null,2\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
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
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id and t2.id > 1 \n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,null,null\n" +
              "2,table11-2,null,2\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
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
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "where t1.name > 'table11-1'"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "2,table11-2,null,2\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
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
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "where t3.id > 2"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
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
          "select t1.id, t1.name, t2.id\n" +
              "from table11 t1\n" +
              "left outer join table15 t2\n" +
              "on t1.id = t2.id"
      );

      String expected =
          "id,name,id\n" +
              "-------------------------------\n" +
              "1,table11-1,null\n" +
              "2,table11-2,null\n" +
              "3,table11-3,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testRightOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "right outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "right outer join table13 t3\n" +
              "on t1.id = t3.id and t2.id = t3.id"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "null,null,null,2\n" +
              "null,null,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
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
          "select t1.id, t1.name, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "inner join table14 t4\n" +
              "on t1.id = t4.id and t4.id > 1\n" +
              "right outer join table13 t3\n" +
              "on t4.id = t3.id and t3.id = 2\n" +
              "where t3.id > 1"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "2,table11-2,2,2\n" +
              "null,null,3,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testRightOuterJoinPredicationCaseByCase3() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "right outer join table12 t2 \n" +
              "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
              "right outer join table13 t3\n" +
              "on t1.id = t3.id "
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "null,null,null,2\n" +
              "null,null,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testFullOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();

    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "full outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "full outer join table14 t4\n" +
              "on t3.id = t4.id \n" +
              "order by t4.id"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "null,null,null,1\n" +
              "2,table11-2,2,2\n" +
              "3,table11-3,3,3\n" +
              "null,null,null,4\n" +
              "1,table11-1,null,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testJoinFilterOfRowPreservedTable1() throws Exception {
    // this test is for join filter of a row preserved table.
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoin2() throws Exception {
    // large, large, small, small
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoin3() throws Exception {
    // large, large, small, large, small, small
    runSimpleTests();
  }

  @Test
  public final void testLeftOuterJoinLeftSideSmallTable() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    String[] data = new String[]{ "1000000|a", "1000001|b", "2|c", "3|d", "4|e" };
    TajoTestingCluster.createTable("table1", schema, tableOptions, data, 1);

    data = new String[10000];
    for (int i = 0; i < data.length; i++) {
      data[i] = i + "|" + "this is testLeftOuterJoinLeftSideSmallTabletestLeftOuterJoinLeftSideSmallTable" + i;
    }
    TajoTestingCluster.createTable("table_large", schema, tableOptions, data, 2);

    try {
      ResultSet res = executeString(
          "select a.id, b.name from table1 a left outer join table_large b on a.id = b.id order by a.id"
      );

      String expected = "id,name\n" +
          "-------------------------------\n" +
          "2,this is testLeftOuterJoinLeftSideSmallTabletestLeftOuterJoinLeftSideSmallTable2\n" +
          "3,this is testLeftOuterJoinLeftSideSmallTabletestLeftOuterJoinLeftSideSmallTable3\n" +
          "4,this is testLeftOuterJoinLeftSideSmallTabletestLeftOuterJoinLeftSideSmallTable4\n" +
          "1000000,null\n" +
          "1000001,null\n";

      assertEquals(expected, resultSetToString(res));

      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE table1 PURGE").close();
      executeString("DROP TABLE table_large PURGE").close();
    }
  }

  @Test
  public void testMultipleBroadcastDataFileWithZeroLength() throws Exception {
    // According to node type(leaf or non-leaf) Broadcast join is determined differently by Repartitioner.
    // testMultipleBroadcastDataFileWithZeroLength testcase is for the leaf node
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
    addEmptyDataFile("nation_multifile", false);

    ResultSet res = executeQuery();

    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE nation_multifile PURGE");
  }

  @Test
  public void testMultipleBroadcastDataFileWithZeroLength2() throws Exception {
    // According to node type(leaf or non-leaf) Broadcast join is determined differently by Repartitioner.
    // testMultipleBroadcastDataFileWithZeroLength2 testcase is for the non-leaf node
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
    addEmptyDataFile("nation_multifile", false);

    ResultSet res = executeQuery();

    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE nation_multifile PURGE");
  }
}
