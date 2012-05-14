package tajo.engine;

import org.apache.hadoop.thirdparty.guava.common.collect.Sets;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Set;

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
  public final void testSelectDistinct() throws Exception {
    Set<String> result1 = Sets.newHashSet();
    result1.add("1,1");
    result1.add("1,2");
    result1.add("2,1");
    result1.add("3,1");
    result1.add("3,2");

    ResultSet res = execute("select distinct l_orderkey, l_linenumber from lineitem");
    int cnt = 0;
    while(res.next()) {
      assertTrue(result1.contains(res.getInt(1) + "," + res.getInt(2)));
      cnt++;
    }
    assertEquals(5, cnt);

    res = execute("select distinct l_orderkey from lineitem");
    Set<Integer> result2 = Sets.newHashSet(new Integer[]{1,2,3});
    cnt = 0;
    while (res.next()) {
      assertTrue(result2.contains(res.getInt(1)));
      cnt++;
    }
    assertEquals(3,cnt);
  }

  @Test
  public final void testLikeClause() throws Exception {
    Set<String> result = Sets.newHashSet(new String[]
        {"ALGERIA", "ETHIOPIA", "INDIA", "INDONESIA", "ROMANIA", "SAUDI ARABIA", "RUSSIA"});

    ResultSet res = execute("select n_name from nation where n_name like '%IA'");
    int cnt = 0;
    while(res.next()) {
      assertTrue(result.contains(res.getString(1)));
      cnt++;
    }
    assertEquals(result.size(), cnt);
  }
}
