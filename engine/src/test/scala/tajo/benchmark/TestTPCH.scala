package tajo.benchmark

import org.scalatest.junit.AssertionsForJUnit
import tajo.engine.TpchTestBase
import org.junit.Test
import org.junit.Assert._
import java.sql.{ResultSetMetaData, ResultSet}
import tajo.client.ResultSetUtil
import collection.immutable.HashMap

/**
  * @author Hyunsik Choi
  */

class TestTPCH extends AssertionsForJUnit {
  val tpch = new TpchTestBase

  /**
   * it verifies NTA-788.
   */
  @Test def testQ1OrderBy() {
    val res = tpch.execute("select l_returnflag, l_linestatus, count(*) as count_order from lineitem " +
      "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus")
    var result = new HashMap[String, Int]
    result += ("NO" -> 3)
    result += ("RF" -> 2)

    res.next()
    assertEquals(result(res.getString(1) + res.getString(2)), res.getInt(3))
    res.next()
    assertEquals(result(res.getString(1) + res.getString(2)), res.getInt(3))
    assertFalse(res.next())
  }

  @Test def testQ2FiveWayJoin() {
    val res = tpch.execute(
      "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, ps_supplycost, " +
        "r_name, p_type, p_size " +
        "from region join nation on n_regionkey = r_regionkey and r_name = 'AMERICA' " +
        "join supplier on s_nationkey = n_nationkey " +
        "join partsupp on s_suppkey = ps_suppkey " +
        "join part on p_partkey = ps_partkey and p_type like '%BRASS' and p_size = 15")
    assertTrue(res.next)
    assertEquals("AMERICA", res.getString(10))
    var pType = res.getString(11).split(" ")
    assertEquals("BRASS", pType(pType.length - 1))
    assertEquals(15, res.getInt(12))
    assertFalse(res.next)
  }

  @Test final def testTPCH14Expr {
    val query: String = "select 100 * sum(" + "case when p_type like 'PROMO%' then l_extendedprice else 0 end) / sum(l_extendedprice * (1 - l_discount)) " + "as promo_revenue from lineitem, part where l_partkey = p_partkey"
    val res: ResultSet = tpch.execute(query)
    res.next
    assertEquals(33, res.getInt(1))
  }
}
