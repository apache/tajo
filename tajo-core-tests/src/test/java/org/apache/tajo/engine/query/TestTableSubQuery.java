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

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.junit.Test;

import java.sql.ResultSet;

public class TestTableSubQuery extends QueryTestCaseBase {

  public TestTableSubQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testTableSubquery1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupBySubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinSubQuery2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testGroupbySubqueryWithJson() throws Exception {
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinSubqueryWithJson() throws Exception {
    /*
    SELECT
      A.n_regionkey, B.r_regionkey, A.n_name, B.r_name
    FROM
      (SELECT * FROM nation WHERE n_name LIKE 'A%') A
    JOIN region B ON A.n_regionkey=B.r_regionkey;
    */
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  @Option(sort = true)
  @SimpleTest(
      queries = @QuerySpec("" +
          "select \n" +
          "  o_custkey, cnt \n" +
          "from \n" +
          "  ( \n" +
          "    select \n" +
          "      o_custkey, cnt, row_number() over (partition by o_custkey order by cnt desc) ranking \n" +
          "    from \n" +
          "      (\n" +
          "        select \n" +
          "          o_custkey, l_suppkey, count(*) cnt\n" +
          "        from \n" +
          "          orders, lineitem\n" +
          "        where \n" +
          "          l_orderkey = o_orderkey\n" +
          "        group by \n" +
          "          o_custkey, l_suppkey\n" +
          "        having cnt > 0\n" +
          "      ) t\n" +
          "  ) t2 \n" +
          "where \n" +
          "  ranking = 1;"
      )
  )
  public void testMultipleSubqueriesWithAggregation() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(sort = true)
  @SimpleTest(
      queries = @QuerySpec("" +
          "select sum(t.cnt) as cnt, l_orderkey, l_partkey, 'my view' from (" +
          "select l_orderkey, l_partkey, CAST(COUNT(1) AS INT4) as cnt from lineitem group by l_orderkey, l_partkey " +
          "union all " +
          "select l_orderkey, l_partkey, l_linenumber as cnt from lineitem) as t " +
          "group by l_orderkey, l_partkey")
  )
  public void testGroupbyOnUnion() throws Exception {
    runSimpleTests();
  }
}
