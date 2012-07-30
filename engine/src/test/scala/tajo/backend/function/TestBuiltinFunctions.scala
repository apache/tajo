package tajo.backend.function

import org.scalatest.junit.{AssertionsForJUnit}
import tajo.engine.TpchTestBase
import org.junit.Test
import org.junit.Assert._
import java.sql.ResultSet
import tajo.client.ResultSetUtil

/**
 * @author Hyunsik Choi
 */

class TestBuiltinFunctions extends AssertionsForJUnit {
  val tpch = new TpchTestBase

  @Test def testMaxLong() = {
    val res = tpch.execute("select max(l_orderkey) as max from lineitem")
    res.next()
    assertEquals(3, res.getInt(1))
  }

  @Test def testMinLong() = {
    val res = tpch.execute("select min(l_orderkey) as max from lineitem")
    res.next()
    assertEquals(1, res.getInt(1))
  }

  @Test def testCount() = {
    val res = tpch.execute("select count(*) as rownum from lineitem")
    res.next()
    assertEquals(5, res.getInt(1))
  }

  @Test def testAvgDouble = {
    val res: ResultSet = tpch.execute("select l_orderkey, avg(l_discount) as revenue from lineitem group by l_orderkey")
    res.next
    assertTrue(0.065f == res.getFloat(2))
    res.next
    assertTrue(0.0f == res.getFloat(2))
    res.next
    assertTrue(0.08f == res.getFloat(2))
    assertFalse(res.next)
  }

  @Test def testRandom() = {
    val res = tpch.execute("select l_orderkey, random(3) as rndnum from lineitem group by l_orderkey, rndnum")
  }
}
