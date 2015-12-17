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

import java.sql.ResultSet;
import java.sql.SQLException;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@NamedTest("TestJoinQuery")
public class TestInnerJoinQuery extends TestJoinQuery {

  public TestInnerJoinQuery(String joinOption) throws Exception {
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
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest()
  public final void testInnerJoinWithThetaJoinConditionInWhere() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testWhereClauseJoin1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testWhereClauseJoin2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testWhereClauseJoin3() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testWhereClauseJoin4() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testWhereClauseJoin5() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testWhereClauseJoin6() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testTPCHQ2Join() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testJoinWithMultipleJoinQual1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinCoReferredEvals1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinCoReferredEvalsWithSameExprs1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinCoReferredEvalsWithSameExprs2() throws Exception {
    // including grouping operator
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testInnerJoinAndCaseWhen() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testInnerJoinWithEmptyTable() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(prepare = {
      "CREATE DATABASE JOINS",
      "CREATE TABLE JOINS.part_ as SELECT * FROM part",
      "CREATE TABLE JOINS.supplier_ as SELECT * FROM supplier"
  }, cleanup = {
      "DROP TABLE JOINS.part_ PURGE",
      "DROP TABLE JOINS.supplier_ PURGE",
      "DROP DATABASE JOINS"
  })
  public final void testJoinOnMultipleDatabases() throws Exception {
    runSimpleTests();
  }

  @Test
  public final void testJoinWithJson() throws Exception {
    // select length(r_comment) as len, *, c_custkey*10 from customer, region order by len,r_regionkey,r_name
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

    try {
      ResultSet res = executeJsonQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE JOINS.part_ PURGE");
      executeString("DROP TABLE JOINS.supplier_ PURGE");
      executeString("DROP DATABASE JOINS");
    }
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testJoinAsterisk() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testDifferentTypesJoinCondition() throws Exception {
    // select * from table20 t3 join table21 t4 on t3.id = t4.id;
    executeDDL("table1_int8_ddl.sql", "table1", "table20");
    executeDDL("table1_int4_ddl.sql", "table2", "table21");
    try {
      runSimpleTests();
    } finally {
      executeString("DROP TABLE table20");
      executeString("DROP TABLE table21");
    }
  }

  @Test
  @SimpleTest
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  public void testComplexJoinCondition1() throws Exception {
    // select n1.n_nationkey, n1.n_name, n2.n_name  from nation n1 join nation n2 on n1.n_name = upper(n2.n_name);
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition2() throws Exception {
    // select n1.n_nationkey, n1.n_name, upper(n2.n_name) name from nation n1 join nation n2
    // on n1.n_name = upper(n2.n_name);
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition3() throws Exception {
    // select n1.n_nationkey, n1.n_name, n2.n_name from nation n1 join nation n2 on lower(n1.n_name) = lower(n2.n_name);
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition4() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testJoinWithOrPredicates() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testNaturalJoin() throws Exception {
    runSimpleTests();
  }

  // FIXME: should be replaced by join queries with hints (See TAJO-2026)
  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testBroadcastTwoPartJoin() throws Exception {
    runSimpleTests();
  }
}
