/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.benchmark;

import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import tajo.IntegrationTest;
import tajo.TpchTestBase;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestTPCH {
  private static TpchTestBase tpch;

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = TpchTestBase.getInstance();
  }

  @AfterClass
  public static void tearDown() throws IOException {
  }

  /**
   * it verifies NTA-788.
   */
  @Test
  public void testQ1OrderBy() throws Exception {
    ResultSet res = tpch.execute("select l_returnflag, l_linestatus, count(*) as count_order from lineitem " +
        "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus");

    Map<String,Integer> result = Maps.newHashMap();
    result.put("NO", 3);
    result.put("RF", 2);

    res.next();
    assertTrue(result.get(res.getString(1) + res.getString(2)) == res.getInt(3));
    res.next();
    assertTrue(result.get(res.getString(1) + res.getString(2)) == res.getInt(3));
    assertFalse(res.next());
  }

  @Test
  public void testQ2FiveWayJoin() throws Exception {
    ResultSet res = tpch.execute(
        "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, ps_supplycost, " +
            "r_name, p_type, p_size " +
            "from region join nation on n_regionkey = r_regionkey and r_name = 'AMERICA' " +
            "join supplier on s_nationkey = n_nationkey " +
            "join partsupp on s_suppkey = ps_suppkey " +
            "join part on p_partkey = ps_partkey and p_type like '%BRASS' and p_size = 15");

    assertTrue(res.next());
    assertEquals("AMERICA", res.getString(10));
    String [] pType = res.getString(11).split(" ");
    assertEquals("BRASS", pType[pType.length - 1]);
    assertEquals(15, res.getInt(12));
    assertFalse(res.next());
  }

  @Test
  public void testTPCH14Expr() throws Exception {
    ResultSet res = tpch.execute("select 100 * sum(" +
        "case when p_type like 'PROMO%' then l_extendedprice else 0 end) / sum(l_extendedprice * (1 - l_discount)) "
        + "as promo_revenue from lineitem, part where l_partkey = p_partkey");

    res.next();
    assertEquals(33, res.getInt(1));
  }
}