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

import com.google.protobuf.ServiceException;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.NamedTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@NamedTest("TestJoinQuery")
public class TestInnerJoinWithSubQuery extends TestJoinQuery {

  public TestInnerJoinWithSubQuery(String joinOption) throws Exception {
    super(joinOption);
  }

  @BeforeClass
  public static void setup() throws Exception {
    TestJoinQuery.setup();
  }

  @AfterClass
  public static void classTearDown() throws ServiceException {
    TestJoinQuery.classTearDown();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testJoinWithMultipleJoinQual2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testJoinWithMultipleJoinQual3() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testJoinWithMultipleJoinQual4() throws Exception {
    runSimpleTests();
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
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition5() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition6() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition7() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testBroadcastSubquery() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testBroadcastSubquery2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testThetaJoinKeyPairs() throws Exception {
    runSimpleTests();
  }
}
