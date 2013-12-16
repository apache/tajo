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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.client.ResultSetUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCaseByCases {
  private static TpchTestBase tpch;

  public TestCaseByCases() throws IOException {
    super();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = TpchTestBase.getInstance();
  }

  @Test
  public final void testTAJO415Case() throws Exception {
    ResultSet res = tpch.execute(FileUtil.readTextFile(new File("src/test/queries/tajo415_case.sql")));
    try {
      Map<Integer, List<Integer>> result = Maps.newHashMap();
      result.put(1, Lists.newArrayList(1, 1));
      result.put(2, Lists.newArrayList(2, 1));
      result.put(3, Lists.newArrayList(3, 1));
      result.put(4, Lists.newArrayList(0, 1));
      result.put(5, Lists.newArrayList(0, 1));
      while(res.next()) {
        assertTrue(result.get(res.getInt(1)).get(0) == res.getInt(2));
        assertTrue(result.get(res.getInt(1)).get(1) == res.getInt(3));
      }
    } finally {
      res.close();
    }
  }

  @Test
  public final void testTAJO418Case() throws Exception {
    ResultSet res = tpch.execute(FileUtil.readTextFile(new File("src/test/queries/tajo418_case.sql")));
    try {
      assertTrue(res.next());
      assertEquals("R", res.getString(1));
      assertEquals("F", res.getString(2));
      assertFalse(res.next());
    } finally {
      res.close();
    }
  }
}
