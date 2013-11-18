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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TpchTestBase;

import java.io.IOException;
import java.sql.ResultSet;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestSortQuery {
  static TpchTestBase tpch;
  public TestSortQuery() throws IOException {
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
  public final void testSort() throws Exception {
    ResultSet res = tpch.execute(
        "select l_linenumber, l_orderkey from lineitem order by l_orderkey");
    try {
      int cnt = 0;
      Long prev = null;
      while(res.next()) {
        if (prev == null) {
          prev = res.getLong(2);
        } else {
          assertTrue(prev <= res.getLong(2));
          prev = res.getLong(2);
        }
        cnt++;
      }

      assertEquals(5, cnt);
    } finally {
      res.close();
    }
  }

  @Test
  public final void testSortWithAliasKey() throws Exception {
    ResultSet res = tpch.execute(
        "select l_linenumber, l_orderkey as sortkey from lineitem order by sortkey");
    try {
      int cnt = 0;
      Long prev = null;
      while(res.next()) {
        if (prev == null) {
          prev = res.getLong(2);
        } else {
          assertTrue(prev <= res.getLong(2));
          prev = res.getLong(2);
        }
        cnt++;
      }

      assertEquals(5, cnt);
    } finally {
      res.close();
    }
  }

  @Test
  public final void testSortWithAliasButOriginalName() throws Exception {
    ResultSet res = tpch.execute(
        "select l_linenumber, l_orderkey as sortkey from lineitem order by l_orderkey");
    try {
      int cnt = 0;
      Long prev = null;
      while(res.next()) {
        if (prev == null) {
          prev = res.getLong(2);
        } else {
          assertTrue(prev <= res.getLong(2));
          prev = res.getLong(2);
        }
        cnt++;
      }

      assertEquals(5, cnt);
    } finally {
      res.close();
    }
  }

  @Test
  public final void testSortDesc() throws Exception {
    ResultSet res = tpch.execute(
        "select l_linenumber, l_orderkey from lineitem order by l_orderkey desc");
    try {
      int cnt = 0;
      Long prev = null;
      while(res.next()) {
        if (prev == null) {
          prev = res.getLong(2);
        } else {
          assertTrue(prev >= res.getLong(2));
          prev = res.getLong(2);
        }
        cnt++;
      }

      assertEquals(5, cnt);
    } finally {
      res.close();
    }
  }

  @Test
  public final void testTopK() throws Exception {
    ResultSet res = tpch.execute(
        "select l_orderkey, l_linenumber from lineitem order by l_orderkey desc limit 3");
    try {
      assertTrue(res.next());
      assertEquals(3, res.getLong(1));
      assertTrue(res.next());
      assertEquals(3, res.getLong(1));
      assertTrue(res.next());
      assertEquals(2, res.getLong(1));
      assertFalse(res.next());
    } finally {
      res.close();
    }
  }

  @Test
  public final void testSortAfterGroupby() throws Exception {
    ResultSet res = tpch.execute("select max(l_quantity), l_orderkey "
        + "from lineitem group by l_orderkey order by l_orderkey");

    try {
      int cnt = 0;
      Long prev = null;
      while(res.next()) {
        if (prev == null) {
          prev = res.getLong(1);
        } else {
          assertTrue(prev <= res.getLong(1));
          prev = res.getLong(1);
        }
        cnt++;
      }

      assertEquals(3, cnt);
    } finally {
      res.close();
    }
  }

  @Test
  public final void testSortAfterGroupbyWithAlias() throws Exception {
    ResultSet res = tpch.execute("select max(l_quantity) as max_quantity, l_orderkey "
        + "from lineitem group by l_orderkey order by max_quantity");

    try {
      int cnt = 0;
      Long prev = null;
      while(res.next()) {
        if (prev == null) {
          prev = res.getLong(1);
        } else {
          assertTrue(prev <= res.getLong(1));
          prev = res.getLong(1);
        }
        cnt++;
      }

      assertEquals(3, cnt);
    } finally {
      res.close();
    }
  }
}
