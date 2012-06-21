package tajo.engine;

import org.junit.Test;
import tajo.client.ResultSetUtil;

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

  @Test
  public final void testTPCH14Expr() throws Exception {
    // The query result is computed as:
    // 100 * (21168.23 + 45983) / ((21168.23 * (1-0.04)) + (45983.16 * (1-0.09)) + (44694.46 * 1)
    // + (54058.05 * (1-0.06)) + (46796.47 * (1-0.10)))

    String query = "select 100 * sum("+
        "case when p_type like 'PROMO%' then l_extendedprice else 0 end) / sum(l_extendedprice * (1 - l_discount)) " +
        "as promo_revenue from lineitem, part where l_partkey = p_partkey";
    ResultSet res = execute(query);
    res.next();
    assertEquals(33, res.getInt(1));
  }

  //@Test
  public final void testAvgDouble() throws Exception {
    ResultSet res = execute("select avg(l_discount) as revenue from lineitem");
    System.out.println(ResultSetUtil.prettyFormat(res));
  }
}
