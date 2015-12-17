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
import org.apache.tajo.QueryTestCaseBase;
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
public class TestMultipleJoinTypes extends TestJoinQuery {

  public TestMultipleJoinTypes(String joinOption) throws Exception {
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
  @QueryTestCaseBase.Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @QueryTestCaseBase.SimpleTest()
  public final void testJoinWithMultipleJoinTypes() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinsWithCaseWhen() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinsWithCaseWhen2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest(prepare = {
      "CREATE TABLE customer_broad_parts (" +
          "  c_nationkey INT4," +
          "  c_name    TEXT," +
          "  c_address    TEXT," +
          "  c_phone    TEXT," +
          "  c_acctbal    FLOAT8," +
          "  c_mktsegment    TEXT," +
          "  c_comment    TEXT" +
          ") PARTITION BY COLUMN (c_custkey INT4)",
      "INSERT OVERWRITE INTO customer_broad_parts" +
          "  SELECT" +
          "    c_nationkey," +
          "    c_name," +
          "    c_address," +
          "    c_phone," +
          "    c_acctbal," +
          "    c_mktsegment," +
          "    c_comment," +
          "    c_custkey" +
          "  FROM customer"
  }, cleanup = {
      "DROP TABLE customer_broad_parts PURGE"
  }, queries = {
      @QuerySpec("select a.l_orderkey, b.o_orderkey, c.c_custkey from lineitem a " +
          "inner join orders b on a.l_orderkey = b.o_orderkey " +
          "left outer join customer_broad_parts c on a.l_orderkey = c.c_custkey and c.c_custkey < 0")
  })
  public final void testInnerAndOuterWithEmpty() throws Exception {
    runSimpleTests();
  }
}
