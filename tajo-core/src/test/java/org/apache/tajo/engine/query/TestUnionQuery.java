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

import static org.junit.Assert.assertEquals;

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

  @Test
  /**
   * S (SA U SA) O
   */
  public final void testUnion1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S (S U S) O
   */
  public final void testUnion2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S O ((S G) U (S G))
   */
  public final void testUnion3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S G (S G)
   */
  public final void testUnion4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S G (S F G)
   */
  public final void testUnion5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S G (SA)
   */
  public final void testUnion6() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  /**
   * S (SA)
   */
  public final void testUnion7() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnion8() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnion9() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnion10() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnion11() throws Exception {
    // test filter pushdown
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnion12() throws Exception {
    // test filter pushdown
    // with subquery in union query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnion13() throws Exception {
    // test filter pushdown
    // with subquery in union query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnion14() throws Exception {
    // test filter pushdown
    // with group by subquery in union query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnion15() throws Exception {
    // test filter pushdown
    // with group by out of union query and join in union query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnion16() throws Exception {
    // test filter pushdown
    // with count distinct out of union query and join in union query
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnionWithSameAliasNames() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnionWithDifferentAlias() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testUnionWithDifferentAliasAndFunction() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftUnionWithJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    ResultSet res = executeString(
        "select * from ( " +
        "  select a.id, b.c_name, a.code from ( " +
        "    select l_orderkey as id, 'lineitem' as code from lineitem " +
        "    union all " +
        "    select o_orderkey as id, 'order' as code from orders " +
         "  ) a " +
         "  join customer b on a.id = b.c_custkey" +
        ") c order by id, code"
    );

    String expected =
        "id,c_name,code\n" +
            "-------------------------------\n" +
            "1,Customer#000000001,lineitem\n" +
            "1,Customer#000000001,lineitem\n" +
            "1,Customer#000000001,order\n" +
            "2,Customer#000000002,lineitem\n" +
            "2,Customer#000000002,order\n" +
            "3,Customer#000000003,lineitem\n" +
            "3,Customer#000000003,lineitem\n" +
            "3,Customer#000000003,order\n";

    assertEquals(expected, resultSetToString(res));

    cleanupQuery(res);
  }

  @Test
  public final void testRightUnionWithJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    ResultSet res = executeString(
            "select * from ( " +
            "  select a.id, b.c_name, a.code from customer b " +
            "  join ( " +
            "    select l_orderkey as id, 'lineitem' as code from lineitem " +
            "    union all " +
            "    select o_orderkey as id, 'order' as code from orders " +
            "  ) a on a.id = b.c_custkey" +
            ") c order by id, code"
    );

    String expected =
        "id,c_name,code\n" +
            "-------------------------------\n" +
            "1,Customer#000000001,lineitem\n" +
            "1,Customer#000000001,lineitem\n" +
            "1,Customer#000000001,order\n" +
            "2,Customer#000000002,lineitem\n" +
            "2,Customer#000000002,order\n" +
            "3,Customer#000000003,lineitem\n" +
            "3,Customer#000000003,lineitem\n" +
            "3,Customer#000000003,order\n";

    assertEquals(expected, resultSetToString(res));

    cleanupQuery(res);
  }

  @Test
  public final void testAllUnionWithJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    ResultSet res = executeString(
        "select * from ( " +
        "  select a.id, a.code as code, b.name, b.code as code2 from ( " +
        "    select l_orderkey as id, 'lineitem' as code from lineitem " +
        "    union all " +
        "    select o_orderkey as id, 'order' as code from orders " +
        "  ) a " +
        "  join ( " +
        "    select c_custkey as id, c_name as name, 'customer' as code from customer " +
        "    union all " +
        "    select p_partkey as id, p_name as name, 'part' as code from part " +
        "  ) b on a.id = b.id" +
        ") c order by id, code, code2"
    );

    String expected =
        "id,code,name,code2\n" +
            "-------------------------------\n" +
            "1,lineitem,Customer#000000001,customer\n" +
            "1,lineitem,Customer#000000001,customer\n" +
            "1,lineitem,goldenrod lavender spring chocolate lace,part\n" +
            "1,lineitem,goldenrod lavender spring chocolate lace,part\n" +
            "1,order,Customer#000000001,customer\n" +
            "1,order,goldenrod lavender spring chocolate lace,part\n" +
            "2,lineitem,Customer#000000002,customer\n" +
            "2,lineitem,blush thistle blue yellow saddle,part\n" +
            "2,order,Customer#000000002,customer\n" +
            "2,order,blush thistle blue yellow saddle,part\n" +
            "3,lineitem,Customer#000000003,customer\n" +
            "3,lineitem,Customer#000000003,customer\n" +
            "3,lineitem,spring green yellow purple cornsilk,part\n" +
            "3,lineitem,spring green yellow purple cornsilk,part\n" +
            "3,order,Customer#000000003,customer\n" +
            "3,order,spring green yellow purple cornsilk,part\n";

    assertEquals(expected, resultSetToString(res));

    cleanupQuery(res);
  }

  @Test
  public final void testUnionWithCrossJoin() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    ResultSet res = executeString(
        "select * from ( " +
        "  select a.id, b.c_name, a.code from ( " +
            "    select l_orderkey as id, 'lineitem' as code from lineitem " +
            "    union all " +
            "    select o_orderkey as id, 'order' as code from orders " +
            "  ) a, " +
            "  customer b " +
        ") c order by id, code, c_name"
    );

    String expected =
        "id,c_name,code\n" +
            "-------------------------------\n" +
            "1,Customer#000000001,lineitem\n" +
            "1,Customer#000000001,lineitem\n" +
            "1,Customer#000000002,lineitem\n" +
            "1,Customer#000000002,lineitem\n" +
            "1,Customer#000000003,lineitem\n" +
            "1,Customer#000000003,lineitem\n" +
            "1,Customer#000000004,lineitem\n" +
            "1,Customer#000000004,lineitem\n" +
            "1,Customer#000000005,lineitem\n" +
            "1,Customer#000000005,lineitem\n" +
            "1,Customer#000000001,order\n" +
            "1,Customer#000000002,order\n" +
            "1,Customer#000000003,order\n" +
            "1,Customer#000000004,order\n" +
            "1,Customer#000000005,order\n" +
            "2,Customer#000000001,lineitem\n" +
            "2,Customer#000000002,lineitem\n" +
            "2,Customer#000000003,lineitem\n" +
            "2,Customer#000000004,lineitem\n" +
            "2,Customer#000000005,lineitem\n" +
            "2,Customer#000000001,order\n" +
            "2,Customer#000000002,order\n" +
            "2,Customer#000000003,order\n" +
            "2,Customer#000000004,order\n" +
            "2,Customer#000000005,order\n" +
            "3,Customer#000000001,lineitem\n" +
            "3,Customer#000000001,lineitem\n" +
            "3,Customer#000000002,lineitem\n" +
            "3,Customer#000000002,lineitem\n" +
            "3,Customer#000000003,lineitem\n" +
            "3,Customer#000000003,lineitem\n" +
            "3,Customer#000000004,lineitem\n" +
            "3,Customer#000000004,lineitem\n" +
            "3,Customer#000000005,lineitem\n" +
            "3,Customer#000000005,lineitem\n" +
            "3,Customer#000000001,order\n" +
            "3,Customer#000000002,order\n" +
            "3,Customer#000000003,order\n" +
            "3,Customer#000000004,order\n" +
            "3,Customer#000000005,order\n";

    assertEquals(expected, resultSetToString(res));

    cleanupQuery(res);
  }

  @Test
  public final void testThreeJoinInUnion() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-881
    ResultSet res = executeString(
      "select orders.o_orderkey \n" +
          "from orders\n" +
          "join lineitem on orders.o_orderkey = lineitem.l_orderkey\n" +
          "join customer on orders.o_custkey =  customer.c_custkey\n" +
          "union all \n" +
          "select nation.n_nationkey from nation"
    );
    String expected =
        "o_orderkey\n" +
            "-------------------------------\n" +
            "1\n" +
            "1\n" +
            "2\n" +
            "3\n" +
            "3\n" +
            "0\n" +
            "1\n" +
            "2\n" +
            "3\n" +
            "4\n" +
            "5\n" +
            "6\n" +
            "7\n" +
            "8\n" +
            "9\n" +
            "10\n" +
            "11\n" +
            "12\n" +
            "13\n" +
            "14\n" +
            "15\n" +
            "16\n" +
            "17\n" +
            "18\n" +
            "19\n" +
            "20\n" +
            "21\n" +
            "22\n" +
            "23\n" +
            "24\n";

    assertEquals(expected, resultSetToString(res));

    cleanupQuery(res);
  }
}