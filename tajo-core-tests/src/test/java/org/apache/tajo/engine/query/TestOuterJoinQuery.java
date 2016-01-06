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
import org.apache.tajo.NamedTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@NamedTest("TestJoinQuery")
public class TestOuterJoinQuery extends TestJoinQuery {

  public TestOuterJoinQuery(String joinOption) throws Exception {
    super(joinOption);
  }

  @BeforeClass
  public static void setup() throws Exception {
    TestJoinQuery.setup();
  }

  @AfterClass
  public static void classTearDown() throws SQLException {
    TestJoinQuery.classTearDown();
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
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
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
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
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
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest()
  public final void testFullOuterJoin1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testOuterJoinAndCaseWhen1() throws Exception {
    executeDDL("oj_table1_ddl.sql", "table1", "testOuterJoinAndCaseWhen1");
    executeDDL("oj_table2_ddl.sql", "table2", "testOuterJoinAndCaseWhen2");
    try {
      runSimpleTests();
    } finally {
      executeString("DROP TABLE testOuterJoinAndCaseWhen1");
      executeString("DROP TABLE testOuterJoinAndCaseWhen2");
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
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t2.id, t3.id\n" +
          "from jointable11 t1\n" +
          "left outer join jointable12 t2\n" +
          "on t1.id = t2.id\n" +
          "left outer join jointable13 t3\n" +
          "on t1.id = t3.id and t2.id = t3.id")
  })
  public final void testLeftOuterJoinPredicationCaseByCase1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
          "from jointable11 t1\n" +
          "left outer join jointable12 t2\n" +
          "on t1.id = t2.id\n" +
          "left outer join jointable13 t3\n" +
          "on t2.id = t3.id\n" +
          "inner join jointable14 t4\n" +
          "on t2.id = t4.id")
  })
  public final void testLeftOuterJoinPredicationCaseByCase2() throws Exception {
    // outer -> outer -> inner
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
          "from jointable11 t1\n" +
          "inner join jointable14 t4\n" +
          "on t1.id = t4.id and t4.id > 1\n" +
          "left outer join jointable13 t3\n" +
          "on t4.id = t3.id and t3.id = 2\n" +
          "left outer join jointable12 t2\n" +
          "on t1.id = t2.id \n" +
          "where t1.id > 1")
  })
  public final void testLeftOuterJoinPredicationCaseByCase2_1() throws Exception {
    // inner(on predication) -> outer(on predication) -> outer -> where
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t2.id, t3.id\n" +
          "from jointable11 t1\n" +
          "left outer join jointable12 t2 \n" +
          "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
          "left outer join jointable13 t3\n" +
          "on t1.id = t3.id ")
  })
  public final void testLeftOuterJoinPredicationCaseByCase3() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case J1: Join Predicate on Preserved Row Table
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t2.id, t3.id\n" +
          "from jointable11 t1\n" +
          "left outer join jointable12 t2\n" +
          "on t1.id = t2.id and t2.id > 1 \n" +
          "left outer join jointable13 t3\n" +
          "on t1.id = t3.id")
  })
  public final void testLeftOuterJoinPredicationCaseByCase4() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case J2: Join Predicate on Null Supplying Table
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t2.id, t3.id\n" +
          "from jointable11 t1\n" +
          "left outer join jointable12 t2\n" +
          "on t1.id = t2.id\n" +
          "left outer join jointable13 t3\n" +
          "on t1.id = t3.id\n" +
          "where t1.name > 'table11-1'")
  })
  public final void testLeftOuterJoinPredicationCaseByCase5() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W1: Where Predicate on Preserved Row Table
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t2.id, t3.id\n" +
          "from jointable11 t1\n" +
          "left outer join jointable12 t2\n" +
          "on t1.id = t2.id\n" +
          "left outer join jointable13 t3\n" +
          "on t1.id = t3.id\n" +
          "where t3.id > 2")
  })
  public final void testLeftOuterJoinPredicationCaseByCase6() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W2: Where Predicate on Null Supplying Table
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t2.id\n" +
          "from jointable11 t1\n" +
          "left outer join jointable15 t2\n" +
          "on t1.id = t2.id")
  })
  public final void testLeftOuterWithEmptyTable() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W2: Where Predicate on Null Supplying Table
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t2.id, t3.id\n" +
          "from jointable11 t1\n" +
          "right outer join jointable12 t2\n" +
          "on t1.id = t2.id\n" +
          "right outer join jointable13 t3\n" +
          "on t1.id = t3.id and t2.id = t3.id")
  })
  public final void testRightOuterJoinPredicationCaseByCase1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t3.id, t4.id\n" +
          "from jointable11 t1\n" +
          "inner join jointable14 t4\n" +
          "on t1.id = t4.id and t4.id > 1\n" +
          "right outer join jointable13 t3\n" +
          "on t4.id = t3.id and t3.id = 2\n" +
          "where t3.id > 1")
  })
  public final void testRightOuterJoinPredicationCaseByCase2() throws Exception {
    // inner -> right
    // Notice: Join order should be preserved with origin order.
    // JoinEdge: t1 -> t4, t3 -> t1,t4
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t2.id, t3.id\n" +
          "from jointable11 t1\n" +
          "right outer join jointable12 t2 \n" +
          "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
          "right outer join jointable13 t3\n" +
          "on t1.id = t3.id ")
  })
  public final void testRightOuterJoinPredicationCaseByCase3() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest(queries = {
      @QuerySpec("select t1.id, t1.name, t3.id, t4.id\n" +
          "from jointable11 t1\n" +
          "full outer join jointable13 t3\n" +
          "on t1.id = t3.id\n" +
          "full outer join jointable14 t4\n" +
          "on t3.id = t4.id \n" +
          "order by t4.id")
  })
  public final void testFullOuterJoinPredicationCaseByCase1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testJoinFilterOfRowPreservedTable1() throws Exception {
    // this test is for join filter of a row preserved table.
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest()
  public final void testLeftOuterJoin2() throws Exception {
    // large, large, small, small
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest()
  public final void testLeftOuterJoin3() throws Exception {
    // large, large, small, large, small, small
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
      @QuerySpec("select a.id, b.name from jointable1 a left outer join jointable_large b on a.id = b.id order by a.id")
  })
  public final void testLeftOuterJoinLeftSideSmallTable() throws Exception {
   runSimpleTests();
  }

  // FIXME: should be replaced by join queries with hints (See TAJO-2026)
  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest
  public void testMultipleBroadcastDataFileWithZeroLength() throws Exception {
    runSimpleTests();
  }

  // FIXME: should be replaced by join queries with hints (See TAJO-2026)
  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest
  public void testMultipleBroadcastDataFileWithZeroLength2() throws Exception {
    runSimpleTests();
  }
}
