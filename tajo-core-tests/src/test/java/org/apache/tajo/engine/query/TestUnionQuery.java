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
import org.junit.Test;
import org.junit.experimental.categories.Category;

/*
 * Notations
 * - S - select
 * - SA - select *
 * - U - union
 * - G - group by
 * - O - order by
 */
@Category(IntegrationTest.class)
public class TestUnionQuery extends QueryTestCaseBase {

  public TestUnionQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  /**
   * S (SA U SA) O
   */
  @Test
  @Option(stats = true, numRows = 8, numBytes = 96)
  @SimpleTest
  public final void testUnionAll1() throws Exception {
    runSimpleTests();
  }

  /**
   * S (S U S) O
   */
  @Test
  @Option(stats = true, numRows = 10, numBytes = 120)
  @SimpleTest
  public final void testUnionAll2() throws Exception {
    runSimpleTests();
  }

  /**
   * S O ((S G) U (S G))
   */
  @Test
  @Option(stats = true, numRows = 2, numBytes = 32)
  @SimpleTest
  public final void testUnionAll3() throws Exception {
    runSimpleTests();
  }

  /**
   * S G (S G)
   */
  @Test
  @Option(stats = true, numRows = 1, numBytes = 16)
  @SimpleTest
  public final void testUnionAll4() throws Exception {
    runSimpleTests();
  }

  /**
   * S G (S F G)
   */
  @Test
  @Option(stats = true, numRows = 1, numBytes = 16)
  @SimpleTest
  public final void testUnionAll5() throws Exception {
    runSimpleTests();
  }

  /**
   * S G (SA)
   */
  @Test
  @Option(stats = true, numRows = 1, numBytes = 16)
  @SimpleTest
  public final void testUnionAll6() throws Exception {
    runSimpleTests();
  }

  /**
   * S (SA)
   */
  @Test
  @Option(stats = true, numRows = 10, numBytes = 120)
  @SimpleTest
  public final void testUnionAll7() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 1, numBytes = 22)
  @SimpleTest
  public final void testUnionAll8() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 5, numBytes = 137)
  @SimpleTest
  public final void testUnionAll9() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 20, numBytes = 548)
  @SimpleTest
  public final void testUnionAll10() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 1, numBytes = 44)
  @SimpleTest
  public final void testUnionAll11() throws Exception {
    // test filter pushdown
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 5, numBytes = 414)
  @SimpleTest
  public final void testUnionAll12() throws Exception {
    // test filter pushdown
    // with stage in union query
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 5, numBytes = 414)
  @SimpleTest
  public final void testUnionAll13() throws Exception {
    // test filter pushdown
    // with stage in union query
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 7, numBytes = 175)
  @SimpleTest
  public final void testUnionAll14() throws Exception {
    // test filter pushdown
    // with group by stage in union query
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 3, numBytes = 75)
  @SimpleTest
  public final void testUnionAll15() throws Exception {
    // test filter pushdown
    // with group by out of union query and join in union query
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 3, numBytes = 75)
  @SimpleTest
  public final void testUnionAll16() throws Exception {
    // test filter pushdown
    // with count distinct out of union query and join in union query
    runSimpleTests();
  }

  /**
   * S (SA U SA) O
   */
  @Test
  @Option(stats = true, numRows = 5, numBytes = 60)
  @SimpleTest
  public final void testUnion1() throws Exception {
    runSimpleTests();
  }

  /**
   * S (S U S) O
   */
  @Test
  @Option(stats = true, numRows = 3, numBytes = 36)
  @SimpleTest
  public final void testUnion2() throws Exception {
    runSimpleTests();
  }

  /**
   * S O ((S G) U (S G))
   */
  @Test
  @Option(stats = true, numRows = 2, numBytes = 32)
  @SimpleTest
  public final void testUnion3() throws Exception {
    runSimpleTests();
  }

  /**
   * S G (S G)
   */
  @Test
  @Option(stats = true, numRows = 1, numBytes = 16)
  @SimpleTest
  public final void testUnion4() throws Exception {
    runSimpleTests();
  }

  /**
   * S G (S F G)
   */
  @Test
  @Option(stats = true, numRows = 1, numBytes = 16)
  @SimpleTest
  public final void testUnion5() throws Exception {
    runSimpleTests();
  }

  /**
   * S G (SA)
   */
  @Test
  @Option(stats = true, numRows = 1, numBytes = 16)
  @SimpleTest
  public final void testUnion6() throws Exception {
    runSimpleTests();
  }

  /**
   * S (SA)
   */
  @Test
  @Option(stats = true, numRows = 3, numBytes = 36)
  @SimpleTest
  public final void testUnion7() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 1, numBytes = 22)
  @SimpleTest
  public final void testUnion8() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 5, numBytes = 137)
  @SimpleTest
  public final void testUnion9() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 5, numBytes = 137)
  @SimpleTest
  public final void testUnion10() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 1, numBytes = 44)
  @SimpleTest
  public final void testUnion11() throws Exception {
    // test filter pushdown
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 5, numBytes = 414)
  @SimpleTest
  public final void testUnion12() throws Exception {
    // test filter pushdown
    // with stage in union query
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 5, numBytes = 414)
  @SimpleTest
  public final void testUnion13() throws Exception {
    // test filter pushdown
    // with stage in union query
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 7, numBytes = 175)
  @SimpleTest
  public final void testUnion14() throws Exception {
    // test filter pushdown
    // with group by stage in union query
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 3, numBytes = 75)
  @SimpleTest
  public final void testUnion15() throws Exception {
    // test filter pushdown
    // with group by out of union query and join in union query
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 3, numBytes = 75)
  @SimpleTest
  public final void testUnion16() throws Exception {
    // test filter pushdown
    // with count distinct out of union query and join in union query
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 10, numBytes = 120)
  @SimpleTest
  public final void testUnionAllWithSameAliasNames() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 2, numBytes = 44)
  @SimpleTest
  public final void testUnionAllWithDifferentAlias() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 5, numBytes = 160)
  @SimpleTest
  public final void testUnionAllWithDifferentAliasAndFunction() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 3, numBytes = 36)
  @SimpleTest
  public final void testUnionWithSameAliasNames() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 2, numBytes = 44)
  @SimpleTest
  public final void testUnionWithDifferentAlias() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 5, numBytes = 160)
  @SimpleTest
  public final void testUnionWithDifferentAliasAndFunction() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 8, numBytes = 423)
  @SimpleTest
  public final void testLeftUnionWithJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 8, numBytes = 423)
  @SimpleTest
  public final void testRightUnionWithJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 16, numBytes = 1215)
  @SimpleTest
  public final void testAllUnionWithJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 40, numBytes = 2115)
  @SimpleTest
  public final void testUnionWithCrossJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 30, numBytes = 360)
  @SimpleTest
  public final void testThreeJoinInUnion() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 5, numBytes = 100)
  @SimpleTest
  public void testUnionCaseOfFirstEmptyAndJoin() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 10, numBytes = 200)
  @SimpleTest
  public void testTajo1368Case1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(stats = true, numRows = 10, numBytes = 200)
  @SimpleTest
  public void testTajo1368Case2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, stats = true, numRows = 4, numBytes = 124)
  @SimpleTest
  public void testComplexUnion1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, sort = true, stats = true, numRows = 5, numBytes = 140)
  @SimpleTest
  public void testComplexUnion2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain =  true, sort = true, stats = true, numRows = 5, numBytes = 120)
  @SimpleTest
  public void testUnionAndFilter() throws Exception {
    runSimpleTests();
  }
  
}
