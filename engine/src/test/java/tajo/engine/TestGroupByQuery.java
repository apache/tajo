package tajo.engine;

import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * @author Hyunsik Choi
 */
public class TestGroupByQuery extends TpchTestBase {
  public TestGroupByQuery() throws IOException {
    super();
  }

  @Test
  public final void testComplexParameter() throws Exception {
    ResultSet res = execute("select sum(l_extendedprice*l_discount) as revenue from lineitem");
    assertTrue(res.next());
    assertTrue(12908 == (int) res.getDouble("revenue"));
    assertFalse(res.next());
  }

  @Test
  public final void testComplexParameter2() throws Exception {
    ResultSet res = execute("select count(*) + max(l_orderkey) as merged from lineitem");
    res.next();
    assertEquals(8, res.getLong("merged"));
  }
}
