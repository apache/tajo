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

package org.apache.tajo.engine.function;

import com.google.common.collect.Maps;
import org.apache.tajo.client.ResultSetUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TpchTestBase;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class TestBuiltinFunctions {
  static TpchTestBase tpch;

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = TpchTestBase.getInstance();
  }

  @AfterClass
  public static void tearDown() throws IOException {
  }

  @Test
  public void testMaxLong() throws Exception {
    ResultSet res = tpch.execute("select max(l_orderkey) as total_max from lineitem");
    try {
      res.next();
      assertEquals(3, res.getInt(1));
    } finally {
      res.close();
    }
  }

  @Test
  public void testMinLong() throws Exception {
    ResultSet res = tpch.execute("select min(l_orderkey) as total_min from lineitem");
    try {
      res.next();
      assertEquals(1, res.getInt(1));
    } finally {
      res.close();
    }
  }

  @Test
  public void testCount() throws Exception {
    ResultSet res = tpch.execute("select count(*) as rownum from lineitem");
    try {
      res.next();
      assertEquals(5, res.getInt(1));
    } finally {
      res.close();
    }
  }

  @Test
  public void testAvgDouble() throws Exception {
    Map<Long, Float> result = Maps.newHashMap();
    result.put(1l, 0.065f);
    result.put(2l, 0.0f);
    result.put(3l, 0.08f);

    ResultSet res = tpch.execute("select l_orderkey, avg(l_discount) as revenue from lineitem group by l_orderkey");

    try {
      while(res.next()) {
        assertTrue(result.get(res.getLong(1)) == res.getFloat(2));
      }
    } finally {
      res.close();
    }
  }

  @Test
  public void testAvgLong() throws Exception {
    ResultSet res = tpch.execute("select avg(l_orderkey) as total_avg from lineitem");
    try {
      res.next();
      assertEquals(2, res.getLong(1));
    } finally {
      res.close();
    }
  }

  @Test
  public void testAvgInt() throws Exception {
    ResultSet res = tpch.execute("select avg(l_partkey) as total_avg from lineitem");
    try {
      res.next();
      System.out.println(res.getFloat(1));
      assertTrue(1.8f == res.getFloat(1));
    } finally {
      res.close();
    }
  }

  @Test
  public void testRandom() throws Exception {
    ResultSet res = tpch.execute("select l_orderkey, random(3) as rndnum from lineitem group by l_orderkey, rndnum");

    try {
      while(res.next()) {
        assertTrue(res.getInt(2) >= 0 && res.getInt(2) < 3);
      }
    } finally {
      res.close();
    }
  }

  @Test
  public void testSplitPart() throws Exception {
    ResultSet res = tpch.execute("select split_part(l_shipinstruct, ' ', 0) from lineitem");

    String [] result ={
      "DELIVER",
      "TAKE",
      "TAKE",
      "NONE",
      "TAKE"
    };

    for (int i = 0; i < result.length; i++) {
      assertTrue(res.next());
      assertEquals(result[i], res.getString(1));
    }
    assertFalse(res.next());

    res.close();
  }

  @Test
  public void testSplitPartNested() throws Exception {
    ResultSet res = tpch.execute("select split_part(split_part(l_shipinstruct, ' ', 0), 'A', 1) from lineitem");

    String [] result ={
        "",
        "KE",
        "KE",
        "",
        "KE"
    };

    for (int i = 0; i < result.length; i++) {
      assertTrue(res.next());
      assertEquals(result[i], res.getString(1));
    }
    assertFalse(res.next());

    res.close();
  }
}
