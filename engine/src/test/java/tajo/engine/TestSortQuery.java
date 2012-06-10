package tajo.engine;

import org.junit.Test;
import tajo.client.ResultSetUtil;

import java.io.IOException;
import java.sql.ResultSet;

/**
 * @author Hyunsik Choi
 */
public class TestSortQuery extends TpchTestBase {
  public TestSortQuery() throws IOException {
    super();
  }

  @Test
  public final void testSort() throws Exception {
    ResultSet res = execute("select l_linenumber, l_orderkey from lineitem order by l_orderkey");
    System.out.println(ResultSetUtil.prettyFormat(res));
    /*int cnt = 0;
    Long prev = null;
    while(res.next()) {
      if (prev == null) {
        prev = res.getLong(2);
      } else {
        assertTrue(prev <= res.getLong(2));
        prev = res.getLong(2);
      }
      cnt++;
    }

    assertEquals(5, cnt);*/
  }
}
