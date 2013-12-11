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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.util.TUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestGroupByQuery {
  private static TpchTestBase tpch;
  public TestGroupByQuery() throws IOException {
    super();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = TpchTestBase.getInstance();
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  @Test
  public final void testGroupBy() throws Exception {
    ResultSet res = tpch.execute(
        "select count(1) as unique_key from lineitem;");
    assertTrue(res.next());
    assertEquals(5, res.getLong(1));
    assertFalse(res.next());
    res.close();
  }

  @Test
  public final void testGroupBy2() throws Exception {
    ResultSet res = tpch.execute(
        "select count(1) as unique_key from lineitem group by l_linenumber");
    Set<Long> expected = Sets.newHashSet(2l,3l);
    for (int i = 0; i < 2; i++) {
      assertTrue(res.next());
      assertTrue(expected.contains(res.getLong(1)));
    }
    assertFalse(res.next());
    res.close();
  }

  @Test
  public final void testGroupBy3() throws Exception {
    ResultSet res = tpch.execute(
        "select l_orderkey as gkey from lineitem group by gkey order by gkey");
    assertTrue(res.next());
    assertEquals(1, res.getLong(1));
    assertTrue(res.next());
    assertEquals(2, res.getLong(1));
    assertTrue(res.next());
    assertEquals(3, res.getLong(1));
    assertFalse(res.next());
    res.close();
  }

  @Test
  public final void testGroupBy4() throws Exception {
    ResultSet res = tpch.execute(
        "select l_orderkey as gkey, count(1) as unique_key from lineitem group by lineitem.l_orderkey");

    long [][] expectedRows = new long[3][];
    expectedRows[0] = new long [] {1,2};
    expectedRows[1] = new long [] {2,1};
    expectedRows[2] = new long [] {3,2};
    Map<Long, long []> expected = Maps.newHashMap();
    for (long [] expectedRow : expectedRows) {
      expected.put(expectedRow[0], expectedRow);
    }

    for (int i = 0; i < expectedRows.length; i++) {
      assertTrue(res.next());
      long [] expectedRow = expected.get(res.getLong(1));
      assertEquals(expectedRow[0], res.getLong(1));
      assertEquals(expectedRow[1], res.getLong(2));
    }
    assertFalse(res.next());
    res.close();
  }

  @Test
  public final void testCountDistinct() throws Exception {
    ResultSet res = tpch.execute(
        "select l_orderkey, max(l_orderkey) as maximum, count(distinct l_linenumber) as unique_key from lineitem " +
            "group by l_orderkey");

    long [][] expectedRows = new long[3][];
    expectedRows[0] = new long [] {1,1,2};
    expectedRows[1] = new long [] {2,2,1};
    expectedRows[2] = new long [] {3,3,2};

    Map<Long, long []> expected = Maps.newHashMap();
    for (long [] expectedRow : expectedRows) {
      expected.put(expectedRow[0], expectedRow);
    }
    for (int i = 0; i < expectedRows.length; i++) {
      assertTrue(res.next());
      long [] expectedRow = expected.get(res.getLong(1));
      assertEquals(expectedRow[1], res.getLong(2));
      assertEquals(expectedRow[2], res.getLong(3));
    }
    assertFalse(res.next());
    res.close();
  }

  @Test
  /**
   * This is an unit test for a combination of aggregation and distinct aggregation functions.
   */
  public final void testCountDistinct2() throws Exception {
    ResultSet res = tpch.execute(
        "select l_orderkey, count(*) as cnt, count(distinct l_linenumber) as unique_key from lineitem " +
            "group by l_orderkey");

    long [][] expectedRows = new long[3][];
    expectedRows[0] = new long [] {1,2,2};
    expectedRows[1] = new long [] {2,1,1};
    expectedRows[2] = new long [] {3,2,2};

    Map<Long, long []> expected = Maps.newHashMap();
    for (long [] expectedRow : expectedRows) {
      expected.put(expectedRow[0], expectedRow);
    }
    for (int i = 0; i < expectedRows.length; i++) {
      assertTrue(res.next());
      long [] expectedRow = expected.get(res.getLong(1));
      assertEquals(expectedRow[1], res.getLong(2));
      assertEquals(expectedRow[2], res.getLong(3));
    }
    assertFalse(res.next());
    res.close();
  }

  @Test
  public final void testComplexParameter() throws Exception {
    ResultSet res = tpch.execute(
        "select sum(l_extendedprice*l_discount) as revenue from lineitem");
    try {
      assertNotNull(res);
      assertTrue(res.next());
      assertTrue(12908 == (int) res.getDouble("revenue"));
      assertFalse(res.next());
    } finally {
      res.close();
    }
  }

  @Test
  public final void testComplexParameterWithSubQuery() throws Exception {


    ResultSet res = tpch.execute(
        "select count(*) as total from ("+
            "        select * from lineitem " +
            "        union all"+
            "        select * from lineitem ) l");
    try {
      assertNotNull(res);
      assertTrue(res.next());
      assertTrue(10 == (int) res.getDouble("total"));
      assertFalse(res.next());
    } finally {
      res.close();
    }
  }

  @Test
  public final void testComplexParameter2() throws Exception {
    ResultSet res = tpch.execute("select count(*) + max(l_orderkey) as merged from lineitem");
    try {
      assertTrue(res.next());
      assertEquals(8, res.getLong("merged"));
    } finally {
      res.close();
    }
  }

  @Test
  public final void testHavingWithNamedTarget() throws Exception {
    ResultSet res = tpch.execute(
        "select l_orderkey, avg(l_partkey) total, sum(l_linenumber) as num from lineitem " +
            "group by l_orderkey having total >= 2 or num = 3");
    Map<Integer, Double> result = TUtil.newHashMap();
    result.put(3, 2.5d);
    result.put(2, 2.0d);
    result.put(1, 1.0d);

    for (int i = 0; i < 3; i++) {
      assertTrue(res.next());
      assertTrue(result.containsKey(res.getInt("l_orderkey")));
      assertTrue(result.get(res.getInt("l_orderkey")) == res.getDouble("total"));
    }
    assertFalse(res.next());
    res.close();
  }


  @Test
  public final void testHavingWithAggFunction() throws Exception {
    ResultSet res = tpch.execute(
        "select l_orderkey, avg(l_partkey) total, sum(l_linenumber) as num from lineitem " +
            "group by l_orderkey having avg(l_partkey) = 2.5 or num = 1");
    Map<Integer, Double> result = TUtil.newHashMap();
    result.put(3, 2.5d);
    result.put(2, 2.0d);

    for (int i = 0; i < 2; i++) {
      assertTrue(res.next());
      assertTrue(result.containsKey(res.getInt("l_orderkey")));
      assertTrue(result.get(res.getInt("l_orderkey")) == res.getDouble("total"));
    }
    assertFalse(res.next());
    res.close();
  }



  //@Test
  public final void testCube() throws Exception {
    ResultSet res = tpch.execute(
        "cube_test := select l_orderkey, l_partkey, sum(l_quantity) from lineitem " +
            "group by cube(l_orderkey, l_partkey)");
    try {
      int count = 0;
      for (;res.next();) {
        count++;
      }
      assertEquals(11, count);
    } finally {
      res.close();
    }
  }
}
