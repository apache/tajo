package org.apache.tajo.engine.query;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

@Category(IntegrationTest.class)
public class TestCommonConditionReduce extends QueryTestCaseBase {

  public TestCommonConditionReduce() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  @Option(withExplain = true)
  @SimpleTest (
      queries = @QuerySpec("select * from nation where (n_regionkey = 1 or n_name is not null) and (n_regionkey = 1 or n_comment is not null)")
  )
  public void test1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true)
  @SimpleTest(
      queries = @QuerySpec("select * from nation where (n_regionkey = 1 or n_name is not null) and (n_regionkey = 1 or n_name is not null)")
  )
  public void test2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true)
  @SimpleTest(
      queries = @QuerySpec("select * from nation where (n_regionkey = 1 and n_name is not null) or (n_regionkey = 1 and n_comment is not null)")
  )
  public void test3() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true)
  @SimpleTest(
      queries = @QuerySpec("select * from lineitem where (l_orderkey = 1 and l_suppkey = 7706 and l_comment is not null) or (l_orderkey = 1 and l_suppkey = 7706 and l_linenumber = 17) or (l_orderkey = 1 and l_suppkey = 7706 and l_commitdate is not null)")
  )
  public void test4() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true)
  @SimpleTest(
      queries = @QuerySpec("select * from lineitem where (l_orderkey = 1 and l_suppkey = 7706 and l_comment is not null) and (l_orderkey = 1 and l_suppkey = 7706 and l_linenumber = 1) and (l_orderkey = 1 and l_suppkey = 7706 and l_commitdate is not null)")
  )
  public void test5() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true)
  @SimpleTest(
      queries = @QuerySpec("select * from lineitem \n" +
          "where (l_orderkey = 1 and (l_suppkey = 7706 or l_comment is not null)) or (l_orderkey = 1 and (l_suppkey = 7706 or l_linenumber = 1)) or (l_orderkey = 1 and (l_suppkey = 7706 or l_commitdate is not null))")
  )
  public void test6() throws Exception {
    runSimpleTests();
  }
}
