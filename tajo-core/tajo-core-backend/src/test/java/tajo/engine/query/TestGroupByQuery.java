/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package tajo.engine.query;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import tajo.IntegrationTest;
import tajo.TpchTestBase;

import java.io.IOException;
import java.sql.ResultSet;

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
  public final void testComplexParameter() throws Exception {
    ResultSet res = tpch.execute(
        "select sum(l_extendedprice*l_discount) as revenue from lineitem");
    assertTrue(res.next());
    assertTrue(12908 == (int) res.getDouble("revenue"));
    assertFalse(res.next());
  }

  @Test
  public final void testComplexParameter2() throws Exception {
    ResultSet res = tpch.execute("select count(*) + max(l_orderkey) as merged from lineitem");
    res.next();
    assertEquals(8, res.getLong("merged"));
  }

  //@Test
  public final void testCube() throws Exception {
    ResultSet res = tpch.execute(
        "cube_test := select l_orderkey, l_partkey, sum(l_quantity) from lineitem group by cube(l_orderkey, l_partkey)");
    int count = 0;
    for (;res.next();) {
      count++;
    }
    assertEquals(11, count);
  }

  //@Test
  // TODO - to fix the limit processing and then enable it
  public final void testGroupByLimit() throws Exception {
    ResultSet res = tpch.execute("select l_orderkey from lineitem limit 2");
    int count = 0;
    for (;res.next();) {
      count++;
    }
    assertEquals(2, count);
  }
}
