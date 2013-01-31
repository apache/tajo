package tajo.engine.query;

import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import tajo.IntegrationTest;
import tajo.TpchTestBase;
import tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Hyunsik Choi
 */
@Category(IntegrationTest.class)
public class TestJoinQuery {
  static TpchTestBase tpch;

  public TestJoinQuery() throws IOException {
    super();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = TpchTestBase.getInstance();
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  @Test
  public final void testCrossJoin() throws Exception {
    ResultSet res = tpch.execute("select n_name, r_name, n_regionkey, r_regionkey from nation, region");

    int cnt = 0;
    while(res.next()) {
      cnt++;
    }
    // TODO - to check their joined contents
    assertEquals(25 * 5, cnt);
  }

  @Test
  public final void testCrossJoinWithExplicitJoinQual() throws Exception {
    ResultSet res = tpch.execute(
        "select n_name, r_name, n_regionkey, r_regionkey from nation, region where n_regionkey = r_regionkey");
    int cnt = 0;
    while(res.next()) {
      cnt++;
    }
    // TODO - to check their joined contents
    assertEquals(25, cnt);
  }

  @Test
  public final void testTPCHQ2Join() throws Exception {
    ResultSet res = tpch.execute(FileUtil
        .readTextFile(new File("src/test/queries/tpch_q2_simplified.tql")));

    Object [][] result = new Object[3][3];

    int tupleId = 0;
    int colId = 0;
    result[tupleId][colId++] = 4032.68f;
    result[tupleId][colId++] = "Supplier#000000002";
    result[tupleId++][colId] = "ETHIOPIA";

    colId = 0;
    result[tupleId][colId++] = 4641.08f;
    result[tupleId][colId++] = "Supplier#000000004";
    result[tupleId++][colId] = "MOROCCO";

    colId = 0;
    result[tupleId][colId++] = 4192.4f;
    result[tupleId][colId++] = "Supplier#000000003";
    result[tupleId][colId] = "ARGENTINA";

    Map<Float, Object[]> resultSet =
        Maps.newHashMap();
    for (Object [] t : result) {
      resultSet.put((Float) t[0], t);
    }

    for (int i = 0; i < 3; i++) {
      res.next();
      Object [] resultTuple = resultSet.get(res.getFloat("s_acctbal"));
      assertEquals(resultTuple[0], res.getFloat("s_acctbal"));
      assertEquals(resultTuple[1], res.getString("s_name"));
      assertEquals(resultTuple[2], res.getString("n_name"));
    }

    assertFalse(res.next());
  }

  @Test
  public void testJoinRefEval() throws Exception {
    ResultSet res = tpch.execute("select r_regionkey, n_regionkey, (r_regionkey + n_regionkey) as plus from region, nation where r_regionkey = n_regionkey");
    int r, n;
    while(res.next()) {
      r = res.getInt(1);
      n = res.getInt(2);
      assertEquals(r + n, res.getInt(3));
    }
  }

  @Test
  public void testJoinAndCaseWhen() throws Exception {
    ResultSet res = tpch.execute("select r_regionkey, n_regionkey, " +
        "case when (((r_regionkey + n_regionkey) % 2) = 0 and r_regionkey = 1) then 'one' " +
        "when (((r_regionkey + n_regionkey) % 2) = 0 and r_regionkey = 2) then 'two' " +
        "when (((r_regionkey + n_regionkey) % 2) = 0 and r_regionkey = 3) then 'three' " +
        "when (((r_regionkey + n_regionkey) % 2) = 0 and r_regionkey = 4) then 'four' " +
        "else 'zero' " +
        "end as cond from region, nation where r_regionkey = n_regionkey");



    Map<Integer, String> result = Maps.newHashMap();
    result.put(0, "zero");
    result.put(1, "one");
    result.put(2, "two");
    result.put(3, "three");
    result.put(4, "four");
    while(res.next()) {
      assertEquals(result.get(res.getInt(1)), res.getString(3));
    }
  }
}
