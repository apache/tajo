package tajo.engine;

import nta.util.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;

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
    ResultSet res = execute(FileUtil.readTextFile(new File("src/test/queries/tpch_q2_simplified.tql")));

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
