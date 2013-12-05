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
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.util.FileUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestJoinQuery {
  static TpchTestBase tpch;

  public TestJoinQuery() throws IOException {
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
  public final void testCrossJoin() throws Exception {
    ResultSet res = tpch.execute("select n_name, r_name, n_regionkey, r_regionkey from nation, region");

    try {
      int cnt = 0;
      while(res.next()) {
        cnt++;
      }
      // TODO - to check their joined contents
      assertEquals(25 * 5, cnt);
    } finally {
      res.close();
    }
  }

  @Test
  public final void testCrossJoinWithExplicitJoinQual() throws Exception {
    ResultSet res = tpch.execute(
        "select n_name, r_name, n_regionkey, r_regionkey from nation, region where n_regionkey = r_regionkey");
    try {
      int cnt = 0;
      while(res.next()) {
        cnt++;
      }
      // TODO - to check their joined contents
      assertEquals(25, cnt);
    } finally {
      res.close();
    }
  }

  @Test
  public final void testTPCHQ2Join() throws Exception {
    ResultSet res = tpch.execute(FileUtil
        .readTextFile(new File("src/test/queries/tpch_q2_simplified.sql")));

    try {
      Object [][] result = new Object[3][3];

      int tupleId = 0;
      int colId = 0;
      result[tupleId][colId++] = 4032.68f;
      result[tupleId][colId++] = "Supplier#000000002";
      result[tupleId++][colId] = "ETHIOPIA";

      colId = 0;
      result[tupleId][colId++] = 4641.08f;
      result[tupleId][colId++] = "Supplier#000000004";
      result[tupleId++][colId] = "MOROCCO";

      colId = 0;
      result[tupleId][colId++] = 4192.4f;
      result[tupleId][colId++] = "Supplier#000000003";
      result[tupleId][colId] = "ARGENTINA";

      Map<Float, Object[]> resultSet =
          Maps.newHashMap();
      for (Object [] t : result) {
        resultSet.put((Float) t[0], t);
      }

      for (int i = 0; i < 3; i++) {
        res.next();
        Object [] resultTuple = resultSet.get(res.getFloat("s_acctbal"));
        assertEquals(resultTuple[0], res.getFloat("s_acctbal"));
        assertEquals(resultTuple[1], res.getString("s_name"));
        assertEquals(resultTuple[2], res.getString("n_name"));
      }

      assertFalse(res.next());
    } finally {
      res.close();
    }
  }

  @Test
  public final void testLeftOuterJoin1() throws Exception {
    ResultSet res = tpch.execute(
        "select c_custkey, orders.o_orderkey from customer left outer join orders on c_custkey = o_orderkey;");
    try {
      Map<Integer, Integer> result = Maps.newHashMap();
      result.put(1, 1);
      result.put(2, 2);
      result.put(3, 3);
      result.put(4, 0);
      result.put(5, 0);
      while(res.next()) {
        assertTrue(result.get(res.getInt(1)) == res.getInt(2));
      }
    } finally {
      res.close();
    }
  }

  @Test
  public final void testRightOuterJoin1() throws Exception {
    ResultSet res = tpch.execute(
        "select c_custkey, orders.o_orderkey from orders right outer join customer on c_custkey = o_orderkey;");
    try {
      Map<Integer, Integer> result = Maps.newHashMap();
      result.put(1, 1);
      result.put(2, 2);
      result.put(3, 3);
      result.put(4, 0);
      result.put(5, 0);
      while(res.next()) {
        assertTrue(result.get(res.getInt(1)) == res.getInt(2));
      }
    } finally {
      res.close();
    }
  }

  @Test
  public final void testFullOuterJoin1() throws Exception {
    ResultSet res = tpch.execute(
        "select c_custkey, orders.o_orderkey, from orders full outer join customer on c_custkey = o_orderkey;");
    try {
      Map<Integer, Integer> result = Maps.newHashMap();
      result.put(1, 1);
      result.put(2, 2);
      result.put(3, 3);
      result.put(4, 0);
      result.put(5, 0);
      while(res.next()) {
        assertTrue(result.get(res.getInt(1)) == res.getInt(2));
      }
    } finally {
      res.close();
    }
  }

  @Test
  public void testJoinRefEval() throws Exception {
    ResultSet res = tpch.execute("select r_regionkey, n_regionkey, (r_regionkey + n_regionkey) as plus from region, nation where r_regionkey = n_regionkey");
    try {
      int r, n;
      while(res.next()) {
        r = res.getInt(1);
        n = res.getInt(2);
        assertEquals(r + n, res.getInt(3));
      }
    } finally {
      res.close();
    }
  }

  @Test
  public void testJoinAndCaseWhen() throws Exception {
    ResultSet res = tpch.execute("select r_regionkey, n_regionkey, " +
        "case when r_regionkey = 1 then 'one' " +
        "when r_regionkey = 2 then 'two' " +
        "when r_regionkey = 3 then 'three' " +
        "when r_regionkey = 4 then 'four' " +
        "else 'zero' " +
        "end as cond from region, nation where r_regionkey = n_regionkey");

    try {
      Map<Integer, String> result = Maps.newHashMap();
      result.put(0, "zero");
      result.put(1, "one");
      result.put(2, "two");
      result.put(3, "three");
      result.put(4, "four");
      while(res.next()) {
        assertEquals(result.get(res.getInt(1)), res.getString(3));
      }
    } finally {
      res.close();
    }
  }
}
