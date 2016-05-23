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
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoException;
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
public class TestInSubquery extends TestJoinQuery {

  public TestInSubquery(String joinOption) throws Exception {
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
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testInSubQuery() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testInSubQuery2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testNestedInSubQuery() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testInSubQueryWithOtherConditions() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testMultipleInSubQuery() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testInSubQueryWithJoin() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testInSubQueryWithTableSubQuery() throws Exception {
    runSimpleTests();
  }

  // TODO: enable after TAJO-2141
//  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testNotInSubQuery() throws Exception {
    runSimpleTests();
  }

  // TODO: enable after TAJO-2141
//  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testMultipleNotInSubQuery() throws Exception {
    runSimpleTests();
  }

  // TODO: enable after TAJO-2141
//  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testNestedNotInSubQuery() throws Exception {
    runSimpleTests();
  }

  // TODO: enable after TAJO-2141
//  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testInAndNotInSubQuery() throws Exception {
    runSimpleTests();
  }

  // TODO: enable after TAJO-2141
//  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testNestedInAndNotInSubQuery() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testNestedInSubQuery2() throws Exception {
    // select c_name from customer
    // where c_nationkey in (
    //    select n_nationkey from nation where n_name like 'C%' and n_regionkey in (
    //    select count(*)-1 from region where r_regionkey > 0 and r_regionkey < 3))
    runSimpleTests();
  }

  @Test(expected = NotImplementedException.class)
  public final void testCorrelatedSubQuery() throws Exception {
    // Use try-catch clause to verify the exact error message
    executeString("select * from nation where n_regionkey in (select r_regionkey from region where n_name > r_name)");
  }

  @Test
  @Option(withExplain = false, withExplainGlobal = false, parameterized = true, sort = true)
  @SimpleTest()
  public final void testSameKeyNameOfOuterAndInnerQueries() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(parameterized = true, sort = true)
  @SimpleTest()
  public final void testWithAsteriskAndJoin() throws Exception {
    // select * from lineitem, orders where l_orderkey = o_orderkey and l_partkey in
    // (select l_partkey from lineitem where l_linenumber in (1, 3, 5, 7, 9))
    runSimpleTests();
  }

  @Test(expected = NotImplementedException.class)
  public final void testNotInSubquery() throws TajoException {
    executeString("select n_name from nation where n_nationkey not in (select r_regionkey from region)");
  }
}
