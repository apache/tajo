package tajo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Hyunsik Choi
 */
public class TestSortQuery {
  static TpchTestBase tpch;
  public TestSortQuery() throws IOException {
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
  public final void testSort() throws Exception {
    ResultSet res = tpch.execute(
        "select l_linenumber, l_orderkey from lineitem order by l_orderkey");
    int cnt = 0;
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

    assertEquals(5, cnt);
  }
}
