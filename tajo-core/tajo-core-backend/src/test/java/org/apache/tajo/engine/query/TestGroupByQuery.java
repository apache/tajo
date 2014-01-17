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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

@Category(IntegrationTest.class)
public class TestGroupByQuery extends QueryTestCaseBase {
  @Test
  public final void testGroupBy() throws Exception {
    // select count(1) as unique_key from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupBy2() throws Exception {
    // select count(1) as unique_key from lineitem group by l_linenumber;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupBy3() throws Exception {
    // select l_orderkey as gkey from lineitem group by gkey order by gkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupBy4() throws Exception {
    // select l_orderkey as gkey, count(1) as unique_key from lineitem group by lineitem.l_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByNested1() throws Exception {
    // select l_orderkey + l_partkey as unique_key from lineitem group by l_orderkey + l_partkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupByNested2() throws Exception {
    // select sum(l_orderkey) + sum(l_partkey) as total from lineitem group by l_orderkey + l_partkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testCountDistinct() throws Exception {
    // select l_orderkey, max(l_orderkey) as maximum, count(distinct l_linenumber) as unique_key from lineitem
    // group by l_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * This is an unit test for a combination of aggregation and distinct aggregation functions.
   */
  public final void testCountDistinct2() throws Exception {
    // select l_orderkey, count(*) as cnt, count(distinct l_linenumber) as unique_key from lineitem group by l_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testComplexParameter() throws Exception {
    // select sum(l_extendedprice*l_discount) as revenue from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testComplexParameterWithSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testComplexParameter2() throws Exception {
    // select count(*) + max(l_orderkey) as merged from lineitem;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testHavingWithNamedTarget() throws Exception {
    // select l_orderkey, avg(l_partkey) total, sum(l_linenumber) as num from lineitem group by l_orderkey
    // having total >= 2 or num = 3;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }


  @Test
  public final void testHavingWithAggFunction() throws Exception {
    // select l_orderkey, avg(l_partkey) total, sum(l_linenumber) as num from lineitem group by l_orderkey
    // having avg(l_partkey) = 2.5 or num = 1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}
