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
import org.apache.tajo.exception.InvalidInputsForCrossJoin;
import org.apache.tajo.exception.TooLargeInputForCrossJoinException;
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
public class TestCrossJoin extends TestJoinQuery {

  public TestCrossJoin(String joinOption) throws Exception {
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
  @SimpleTest(queries = {
      @QuerySpec("select n_name, r_name, n_regionkey, r_regionkey from nation, region order by n_name, r_name"),
      // testCrossJoinWithAsterisk
      @QuerySpec("select region.*, customer.* from region, customer"),
      @QuerySpec("select region.*, customer.* from customer, region"),
      @QuerySpec("select * from customer, region"),
      @QuerySpec("select length(r_comment) as len, *, c_custkey*10 from customer, region order by len,r_regionkey,r_name")
  })
  public final void testCrossJoin() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testCrossJoinWithEmptyTable1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinAndCaseWhen() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinWithAsterisk1() throws Exception {
    // select region.*, customer.* from region, customer;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinWithAsterisk2() throws Exception {
    // select region.*, customer.* from customer, region;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinWithAsterisk3() throws Exception {
    // select * from customer, region
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinWithAsterisk4() throws Exception {
    // select length(r_regionkey), *, c_custkey*10 from customer, region
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true, sort = true)
  @SimpleTest()
  public final void testCrossJoinWithThetaJoinConditionInWhere() throws Exception {
    runSimpleTests();
  }

  @Test (expected = TooLargeInputForCrossJoinException.class)
  public final void testCrossJoinOfOneLargeTableAndJoin() throws Exception {
    try {
      executeString("SET SESSION BROADCAST_CROSS_JOIN_THRESHOLD to 2").close();
      executeString("select * from nation cross join region left outer join lineitem on r_regionkey = l_orderkey " +
          "inner join supplier on l_suppkey = s_suppkey");
    } finally {
      executeString("SET SESSION BROADCAST_CROSS_JOIN_THRESHOLD to "
          + ORIGINAL_BROADCAST_CROSS_JOIN_THRESHOLD).close();
    }
  }

  @Test (expected = TooLargeInputForCrossJoinException.class)
  public final void testCrossJoinOfTwoLargeTables() throws Exception {
    try {
      executeString("SET SESSION BROADCAST_CROSS_JOIN_THRESHOLD 2").close();
      executeString("select * from nation n1 cross join nation n2");
    } finally {
      executeString("SET SESSION BROADCAST_CROSS_JOIN_THRESHOLD "
          + ORIGINAL_BROADCAST_CROSS_JOIN_THRESHOLD).close();
    }
  }

  // FIXME: should be replaced by join queries with hints (See TAJO-2026)
  @Test (expected = InvalidInputsForCrossJoin.class)
  public final void testCrossJoinOfSubqueries() throws Exception {
    executeString("select * from (select * from nation, region where n_regionkey = r_regionkey) t1 " +
        "cross join (select * from orders, lineitem where l_orderkey = o_orderkey) t2");
  }

  // FIXME: should be replaced by join queries with hints (See TAJO-2026)
  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest (queries = {
      @QuerySpec("select * from nation cross join region")
  })
  public final void testCrossJoinOfOneSmallTable() throws Exception {
    runSimpleTests();
  }

  // FIXME: should be replaced by join queries with hints (See TAJO-2026)
  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest (queries = {
      @QuerySpec("select * from orders cross join region left outer join lineitem on r_regionkey = l_orderkey " +
          "inner join supplier on l_suppkey = s_suppkey")
  })
  public final void testCrossJoinOfOneSmallTableAndJoin() throws Exception {
    runSimpleTests();
  }

  // FIXME: should be replaced by join queries with hints (See TAJO-2026)
  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest (queries = {
      @QuerySpec("select * from lineitem cross join region")
  })
  public final void testCrossJoinOftwoSmallTables() throws Exception {
    runSimpleTests();
  }
}
