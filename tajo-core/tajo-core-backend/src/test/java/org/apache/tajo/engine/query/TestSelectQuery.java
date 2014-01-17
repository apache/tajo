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
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class TestSelectQuery extends QueryTestCaseBase {

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
  public final void testWhereCond1() throws Exception {
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
  public final void testSelectAsterik() throws Exception {
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
    ResultSet res = testingCluster.execute(
        "create table orderkeys as select l_orderkey from lineitem");
    res.close();
    TajoTestingCluster cluster = testingCluster.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable("orderkeys"));
    TableDesc orderKeys = catalog.getTableDesc("orderkeys");
    assertEquals(5, orderKeys.getStats().getNumRows().intValue());
  }

  @Test
  public final void testLimit() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}