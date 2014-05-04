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
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class TestSelectQuery extends QueryTestCaseBase {

  public TestSelectQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testNonFromSelect1() throws Exception {
    // select upper('abc');
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSimpleQuery() throws Exception {
    // select * from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSimpleQueryWithLimit() throws Exception {
    // select * from lineitem limit 3;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testExplainSelect() throws Exception {
    // explain select l_orderkey, l_partkey from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelect() throws Exception {
    // select l_orderkey, l_partkey from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelect2() throws Exception {
    // select l_orderkey, l_partkey, l_orderkey + l_partkey as plus from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelect3() throws Exception {
    // select l_orderkey + l_partkey as plus from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectColumnAlias1() throws Exception {
    // select l_orderkey as col1, l_orderkey + 1 as col2 from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectSameConstantsWithDifferentAliases() throws Exception {
    // select l_orderkey, '20130819' as date1, '20130819' as date2 from lineitem where l_orderkey > -1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectSameConstantsWithDifferentAliases2() throws Exception {
    // select l_orderkey, '20130819' as date1, '20130819' as date2, '20130819' as date3, '20130819' as date4
    // from lineitem where l_orderkey > -1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectSameExprsWithDifferentAliases() throws Exception {
    // select l_orderkey, l_partkey + 1 as plus1, l_partkey + 1 as plus2 from lineitem where l_orderkey > -1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereCond1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereCond2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereCondWithAlias1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereCondWithAlias2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectAsterisk1() throws Exception {
    // select * from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectAsterisk2() throws Exception {
    // select * from lineitem where l_orderkey = 2;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectAsterisk3() throws Exception {
    // select * from lineitem where l_orderkey % 2 = 0;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectAsterisk4() throws Exception {
    // select length(l_comment), l_extendedprice * l_discount, *, l_tax * 10 from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectAsterisk5() throws Exception {
    // select * from (select l_orderkey, 1 from lineitem where l_orderkey % 2 = 0) t1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelectDistinct() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLikeClause() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testStringCompare() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testRealValueCompare() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testCaseWhen() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testCaseWhenWithoutElse() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNotEqual() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInClause() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInStrClause() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNotInStrClause() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNotInClause() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testCreateAfterSelect() throws Exception {
    ResultSet res = testBase.execute(
        "create table orderkeys as select l_orderkey from lineitem");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, "orderkeys"));
    TableDesc orderKeys = catalog.getTableDesc(DEFAULT_DATABASE_NAME, "orderkeys");
    if (!cluster.isHCatalogStoreRunning()) {
      assertEquals(5, orderKeys.getStats().getNumRows().intValue());
    }
  }

  @Test
  public final void testLimit() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testDatabaseRef() throws Exception {
    if (!testingCluster.isHCatalogStoreRunning()) {
      executeString("CREATE DATABASE \"TestSelectQuery\"").close();
      executeString("CREATE TABLE \"TestSelectQuery\".\"LineItem\" AS SELECT * FROM default.lineitem" ).close();

      ResultSet res = executeFile("testDatabaseRef1.sql");
      assertResultSet(res, "testDatabaseRef.result");
      cleanupQuery(res);

      res = executeFile("testDatabaseRef2.sql");
      assertResultSet(res, "testDatabaseRef.result");
      cleanupQuery(res);

      res = executeFile("testDatabaseRef3.sql");
      assertResultSet(res, "testDatabaseRef.result");
      cleanupQuery(res);

      executeString("DROP DATABASE \"TestSelectQuery\"").close();
    }
  }
}