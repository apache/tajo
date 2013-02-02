package tajo.engine.function;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Hyunsik Choi
 */
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
    ResultSet res = tpch.execute("select max(l_orderkey) as max from lineitem");
    res.next();
    assertEquals(3, res.getInt(1));
  }

  @Test
  public void testMinLong() throws Exception {
    ResultSet res = tpch.execute("select min(l_orderkey) as max from lineitem");
    res.next();
    assertEquals(1, res.getInt(1));
  }

  @Test
  public void testCount() throws Exception {
    ResultSet res = tpch.execute("select count(*) as rownum from lineitem");
    res.next();
    assertEquals(5, res.getInt(1));
  }

  @Test
  public void testAvgDouble() throws Exception {
    Map<Long, Float> result = Maps.newHashMap();
    result.put(1l, 0.065f);
    result.put(2l, 0.0f);
    result.put(3l, 0.08f);

    ResultSet res = tpch.execute("select l_orderkey, avg(l_discount) as revenue from lineitem group by l_orderkey");

    while(res.next()) {
      assertTrue(result.get(res.getLong(1)) == res.getFloat(2));
    }
  }

  @Test
  public void testAvgLong() throws Exception {
    ResultSet res = tpch.execute("select avg(l_orderkey) as avg from lineitem");
    res.next();
    assertEquals(2, res.getLong(1));
  }

  @Test
  public void testAvgInt() throws Exception {
    ResultSet res = tpch.execute("select avg(l_partkey) as avg from lineitem");
    res.next();
    System.out.println(res.getFloat(1));
    assertTrue(1.8f == res.getFloat(1));
  }

  @Test
  public void testRandom() throws Exception {
    ResultSet res = tpch.execute("select l_orderkey, random(3) as rndnum from lineitem group by l_orderkey, rndnum");

    while(res.next()) {
      assertTrue(res.getInt(2) >= 0 && res.getInt(2) < 3);
    }
  }
}
