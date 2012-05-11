package tajo.engine;

import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Hyunsik Choi
 */
public class TestSelectQuery extends TpchTestBase {
  public TestSelectQuery() throws IOException {
    super();
  }

  @Test
  public final void testSelect() throws Exception {
    ResultSet res = execute("select l_orderkey,l_partkey from lineitem");
    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals(155190, res.getInt(2));

    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals(67310, res.getInt(2));

    res.next();
    assertEquals(2, res.getInt(1));
    assertEquals(106170, res.getInt(2));
  }

  @Test
  public final void testSelectAsterik() throws Exception {
    ResultSet res = execute("select * from lineitem");
    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals(155190, res.getInt(2));
    assertEquals(7706, res.getInt(3));
    assertEquals(1, res.getInt(4));
    assertTrue(17 == res.getFloat(5));
    assertTrue(21168.23f == res.getFloat(6));
    assertTrue(0.04f == res.getFloat(7));
    assertTrue(0.02f == res.getFloat(8));
    assertEquals("N",res.getString(9));
    assertEquals("O",res.getString(10));
    assertEquals("1996-03-13",res.getString(11));
    assertEquals("1996-02-12",res.getString(12));
    assertEquals("1996-03-22",res.getString(13));
    assertEquals("DELIVER IN PERSON",res.getString(14));
    assertEquals("TRUCK",res.getString(15));
    assertEquals("egular courts above the",res.getString(16));

    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals(67310, res.getInt(2));
    assertEquals(7311, res.getInt(3));
    assertEquals(2, res.getInt(4));
    assertTrue(36 == res.getFloat(5));
    assertTrue(45983.16f == res.getFloat(6));
    assertTrue(0.09f == res.getFloat(7));
    assertTrue(0.06f == res.getFloat(8));
    assertEquals("N",res.getString(9));
    assertEquals("O",res.getString(10));
    assertEquals("1996-04-12",res.getString(11));
    assertEquals("1996-02-28",res.getString(12));
    assertEquals("1996-04-20",res.getString(13));
    assertEquals("TAKE BACK RETURN",res.getString(14));
    assertEquals("MAIL",res.getString(15));
    assertEquals("ly final dependencies: slyly bold",res.getString(16));
  }

  @Test
  public final void testSelectWithFilter() throws Exception {
    ResultSet res = execute("select l_orderkey, l_linenumber from lineitem where l_shipdate = '1997-01-28'");
    res.next();
    assertEquals(2, res.getInt(1));
    assertEquals(1, res.getInt(2));
  }
}
