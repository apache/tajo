package tajo.engine;

import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Hyunsik Choi
 */
public class TestSortQuery extends TpchTestBase {
  public TestSortQuery() throws IOException {
    super();
  }

  @Test
  public final void testSort() throws Exception {
    ResultSet res = execute("select l_orderkey from lineitem order by l_orderkey");
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

    assertEquals(5, cnt);
  }
}
