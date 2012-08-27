package tajo.engine;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;

import static org.junit.Assert.*;

/**
 * @author Hyunsik Choi
 */
public class TestGroupByQuery {
  private static TpchTestBase tpch;
  public TestGroupByQuery() throws IOException {
    super();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = new TpchTestBase();
    tpch.setUp();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tpch.tearDown();
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
}
