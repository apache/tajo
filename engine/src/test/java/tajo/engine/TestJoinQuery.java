package tajo.engine;

import nta.engine.query.ResultSetImpl;
import org.junit.Test;
import tajo.client.ResultSetUtil;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Hyunsik Choi
 */
public class TestJoinQuery extends TpchTestBase {

  public TestJoinQuery() throws IOException {
    super();
  }

  @Test
  public final void testCrossJoin() throws Exception {
    ResultSet res = execute("select n_name, r_name, n_regionkey, r_regionkey from nation, region");
    int cnt = 0;
    while(res.next()) {
      cnt++;
    }
    // TODO - to check their joined contents
    assertEquals(25 * 5, cnt);
  }

  @Test
  public final void testCrossJoinWithExplicitJoinQual() throws Exception {
    ResultSet res = execute("select n_name, r_name, n_regionkey, r_regionkey from nation, region where n_regionkey = r_regionkey");
    int cnt = 0;
    while(res.next()) {
      cnt++;
    }
    // TODO - to check their joined contents
    assertEquals(25, cnt);
  }

  @Test
  public final void testTPCHQ2Join() throws Exception {
    ResultSet res = execute(
        "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment " +
        "from part, supplier, partsupp, nation, region " +
        "where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey " +
        "and n_regionkey = r_regionkey");

    res.next();
    assertTrue(4032.68f == res.getFloat("s_acctbal"));
    assertEquals("Supplier#000000002", res.getString("s_name"));
    assertEquals("ETHIOPIA", res.getString("n_name"));

    res.next();
    assertTrue(4641.08f == res.getFloat("s_acctbal"));
    assertEquals("Supplier#000000004", res.getString("s_name"));
    assertEquals("MOROCCO", res.getString("n_name"));

    res.next();
    assertTrue(4192.4f == res.getFloat("s_acctbal"));
    assertEquals("Supplier#000000003", res.getString("s_name"));
    assertEquals("ARGENTINA", res.getString("n_name"));

    assertFalse(res.next());
  }

  //@Test
  public final void testCount() throws Exception {
    ResultSet res = execute("select count(l_orderkey) as total from lineitem");
  }
}
