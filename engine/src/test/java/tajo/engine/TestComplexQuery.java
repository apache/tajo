package tajo.engine;

import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author jihoon
 */
public class TestComplexQuery extends TpchTestBase {

  public TestComplexQuery() throws IOException {
    super();
  }

  @Test
  public final void testSortAfterGroupby() throws Exception {
    ResultSet res = execute("select max(l_quantity), l_orderkey "
        + "from lineitem group by l_orderkey order by l_orderkey");

    int cnt = 0;
    Long prev = null;
    while(res.next()) {
      if (prev == null) {
        prev = res.getLong(1);
      } else {
        assertTrue(prev <= res.getLong(1));
        prev = res.getLong(1);
      }
      cnt++;
    }

    assertEquals(3, cnt);
  }
}
