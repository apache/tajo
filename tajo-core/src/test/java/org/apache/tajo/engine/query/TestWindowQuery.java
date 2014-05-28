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

import java.sql.ResultSet;

@Category(IntegrationTest.class)
public class TestWindowQuery extends QueryTestCaseBase {

  public TestWindowQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testWindow1() throws Exception {
    ResultSet res = executeString("SELECT sum(l_quantity) OVER () FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow2() throws Exception {
    ResultSet res = executeString("SELECT l_orderkey, l_quantity, sum(l_quantity) OVER () FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow3() throws Exception {
    ResultSet res = executeString(
        "SELECT l_orderkey, l_quantity, sum(l_quantity) OVER (PARTITION BY l_orderkey) FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow4() throws Exception {
    ResultSet res = executeString(
        "SELECT l_orderkey, l_discount, sum(l_discount) OVER (PARTITION BY l_orderkey), sum(l_quantity) " +
            "OVER (PARTITION BY l_orderkey) FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow5() throws Exception {
    ResultSet res = executeString(
        "SELECT l_orderkey, sum(l_discount) OVER (PARTITION BY l_orderkey), l_discount, sum(l_quantity) " +
            "OVER (PARTITION BY l_orderkey) FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindow6() throws Exception {
    ResultSet res = executeString(
        "SELECT l_orderkey, l_discount, row_number() OVER (PARTITION BY l_orderkey) r1 , sum(l_discount) " +
            "OVER (PARTITION BY l_orderkey) r2 FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithOrderBy1() throws Exception {
    ResultSet res = executeString(
        "SELECT l_orderkey, l_discount, rank() OVER (ORDER BY l_discount) r1 FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithOrderBy2() throws Exception {
    ResultSet res = executeString(
        "SELECT l_orderkey, l_partkey, rank() OVER (PARTITION BY L_ORDERKEY ORDER BY l_partkey) r1 FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithOrderBy3() throws Exception {
    ResultSet res = executeString(
        "SELECT l_orderkey, l_partkey, rank() OVER (PARTITION BY L_ORDERKEY ORDER BY l_partkey desc) r1 FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithOrderBy4() throws Exception {
    ResultSet res = executeString(
        "SELECT l_orderkey, l_partkey, rank() OVER (ORDER BY l_orderkey) r1, rank() OVER(ORDER BY l_partkey desc) r2 " +
            "FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowBeforeLimit() throws Exception {
    ResultSet res = executeString(
        "select r_name, rank() over (order by r_regionkey) as ran from region limit 3;"
    );
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWindowWithSubQuery() throws Exception {
    ResultSet res = executeString(
        "select r_name, c, rank() over (order by r_regionkey) as ran from " +
            "(select r_name, r_regionkey, count(*) as c from region group by r_name, r_regionkey) a;"
    );
    assertResultSet(res);
    cleanupQuery(res);
  }

//  @Test
//  public final void testWindowWithSubQuery2() throws Exception {
//    ResultSet res = executeString(
//        "select r_name, c, rank() over (partition by r_regionkey order by r_regionkey) as ran from " +
//            "(select r_name, r_regionkey, count(*) as c from region group by r_name, r_regionkey) a;"
//    );
//    assertResultSet(res);
//    cleanupQuery(res);
//  }

  @Test
  public final void rowNumber1() throws Exception {
    ResultSet res = executeString(
        "SELECT l_orderkey, row_number() OVER () as row_num FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void rowNumber2() throws Exception {
    ResultSet res = executeString(
        "SELECT l_orderkey, row_number() OVER (PARTITION BY L_ORDERKEY) as row_num FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void rowNumber3() throws Exception {
    ResultSet res = executeString(
    "SELECT l_orderkey, row_number() OVER (PARTITION BY L_ORDERKEY) as row_num, l_discount, avg(l_discount) " +
        "OVER (PARTITION BY L_ORDERKEY) as average FROM LINEITEM");
    assertResultSet(res);
    cleanupQuery(res);
  }
}