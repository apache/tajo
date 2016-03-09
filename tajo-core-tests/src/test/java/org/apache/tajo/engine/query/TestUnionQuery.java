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

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.jdbc.TajoResultSetBase;
import org.apache.tajo.master.QueryInfo;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    conf.setBoolVar(TajoConf.ConfVars.$DEBUG_ENABLED, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    conf.setBoolVar(TajoConf.ConfVars.$DEBUG_ENABLED, false);
  }

  /**
   * S (SA U SA) O
   */
  @Test
  @SimpleTest
  public final void testUnionAll1() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 8L);
  }

  /**
   * S (S U S) O
   */
  @Test
  @SimpleTest
  public final void testUnionAll2() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 10L);
  }

  /**
   * S O ((S G) U (S G))
   */
  @Test
  @SimpleTest
  public final void testUnionAll3() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 2L);
  }

  /**
   * S G (S G)
   */
  @Test
  @SimpleTest
  public final void testUnionAll4() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 1L);
  }

  /**
   * S G (S F G)
   */
  @Test
  @SimpleTest
  public final void testUnionAll5() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 1L);
  }

  /**
   * S G (SA)
   */
  @Test
  @SimpleTest
  public final void testUnionAll6() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 1L);
  }

  /**
   * S (SA)
   */
  @Test
  @SimpleTest
  public final void testUnionAll7() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 10L);
  }

  @Test
  @SimpleTest
  public final void testUnionAll8() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 1L);
  }

  @Test
  @SimpleTest
  public final void testUnionAll9() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @SimpleTest
  public final void testUnionAll10() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 20L);
  }

  @Test
  @SimpleTest
  public final void testUnionAll11() throws Exception {
    // test filter pushdown
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 1L);
  }

  @Test
  @SimpleTest
  public final void testUnionAll12() throws Exception {
    // test filter pushdown
    // with stage in union query
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @SimpleTest
  public final void testUnionAll13() throws Exception {
    // test filter pushdown
    // with stage in union query
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @SimpleTest
  public final void testUnionAll14() throws Exception {
    // test filter pushdown
    // with group by stage in union query
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 7L);
  }

  @Test
  @SimpleTest
  public final void testUnionAll15() throws Exception {
    // test filter pushdown
    // with group by out of union query and join in union query
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 3L);
  }

  @Test
  @SimpleTest
  public final void testUnionAll16() throws Exception {
    // test filter pushdown
    // with count distinct out of union query and join in union query
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 3L);
  }

  /**
   * S (SA U SA) O
   */
  @Test
  @SimpleTest
  public final void testUnion1() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  /**
   * S (S U S) O
   */
  @Test
  @SimpleTest
  public final void testUnion2() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 3L);
  }

  /**
   * S O ((S G) U (S G))
   */
  @Test
  @SimpleTest
  public final void testUnion3() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 2L);
  }

  /**
   * S G (S G)
   */
  @Test
  @SimpleTest
  public final void testUnion4() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 1L);
  }

  /**
   * S G (S F G)
   */
  @Test
  @SimpleTest
  public final void testUnion5() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 1L);
  }

  /**
   * S G (SA)
   */
  @Test
  @SimpleTest
  public final void testUnion6() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 1L);
  }

  /**
   * S (SA)
   */
  @Test
  @SimpleTest
  public final void testUnion7() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 3L);
  }

  @Test
  @SimpleTest
  public final void testUnion8() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 1L);
  }

  @Test
  @SimpleTest
  public final void testUnion9() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @SimpleTest
  public final void testUnion10() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @SimpleTest
  public final void testUnion11() throws Exception {
    // test filter pushdown
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 1L);
  }

  @Test
  @SimpleTest
  public final void testUnion12() throws Exception {
    // test filter pushdown
    // with stage in union query
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @SimpleTest
  public final void testUnion13() throws Exception {
    // test filter pushdown
    // with stage in union query
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @SimpleTest
  public final void testUnion14() throws Exception {
    // test filter pushdown
    // with group by stage in union query
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 7L);
  }

  @Test
  @SimpleTest
  public final void testUnion15() throws Exception {
    // test filter pushdown
    // with group by out of union query and join in union query
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 3L);
  }

  @Test
  @SimpleTest
  public final void testUnion16() throws Exception {
    // test filter pushdown
    // with count distinct out of union query and join in union query
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 3L);
  }

  @Test
  @SimpleTest
  public final void testUnionAllWithSameAliasNames() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 10L);
  }

  @Test
  @SimpleTest
  public final void testUnionAllWithDifferentAlias() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 2L);
  }

  @Test
  @SimpleTest
  public final void testUnionAllWithDifferentAliasAndFunction() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @SimpleTest
  public final void testUnionWithSameAliasNames() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 3L);
  }

  @Test
  @SimpleTest
  public final void testUnionWithDifferentAlias() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 2L);
  }

  @Test
  @SimpleTest
  public final void testUnionWithDifferentAliasAndFunction() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @SimpleTest
  public final void testLeftUnionWithJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 8L);
  }

  @Test
  @SimpleTest
  public final void testRightUnionWithJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 8L);
  }

  @Test
  @SimpleTest
  public final void testAllUnionWithJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 16L);
  }

  @Test
  @SimpleTest
  public final void testUnionWithCrossJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 40L);
  }

  @Test
  @SimpleTest
  public final void testThreeJoinInUnion() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 30L);
  }

  @Test
  @SimpleTest
  public void testUnionCaseOfFirstEmptyAndJoin() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @SimpleTest
  public void testTajo1368Case1() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 10L);
  }

  @Test
  @SimpleTest
  public void testTajo1368Case2() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 10L);
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true)
  @SimpleTest
  public void testComplexUnion1() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 4L);
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, sort = true)
  @SimpleTest
  public void testComplexUnion2() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  @Test
  @Option(withExplain =  true, sort = true)
  @SimpleTest
  public void testUnionAndFilter() throws Exception {
    Optional<TajoResultSetBase[]> existing = runSimpleTests();
    verifyResultStats(existing, 5L);
  }

  private void verifyResultStats(Optional<TajoResultSetBase[]> existing, long numRows) throws Exception {
    assertTrue(existing.isPresent());

    // Get TableStats using TajoResultSetBase.
    TajoResultSetBase[] resultSet = existing.get();
    QueryId qid = resultSet[0].getQueryId();
    QueryInfo queryInfo = testingCluster.getMaster().getContext().getQueryJobManager().getFinishedQuery(qid);
    TableDesc desc = queryInfo.getResultDesc();
    TableStats stats = desc.getStats();

    // Compare specified number of rows to the number of rows in TableStats.
    assertEquals(numRows, stats.getNumRows().longValue());

    // Compare the volume number of directRaw to the number of rows in TableStats.
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(desc.getUri());
    assertTrue(fs.exists(path));
    ContentSummary summary = fs.getContentSummary(path);
    assertEquals(summary.getLength(), stats.getNumBytes().longValue());

    closeResultSets(resultSet);
  }

}
