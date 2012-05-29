package tajo.engine;

import org.apache.hadoop.thirdparty.guava.common.collect.Sets;
import org.junit.Test;
import tajo.client.ResultSetUtil;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Set;

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
    assertTrue(12908 == (int)res.getDouble("revenue"));
    assertFalse(res.next());
  }
}
