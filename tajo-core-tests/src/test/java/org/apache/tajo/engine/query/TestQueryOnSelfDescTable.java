package org.apache.tajo.engine.query;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.exception.TajoException;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;

public class TestQueryOnSelfDescTable extends QueryTestCaseBase {

  public TestQueryOnSelfDescTable() throws IOException, TajoException {
    super();

    executeString(String.format("create external table self_desc_table1 using json location '%s'",
        getDataSetFile("sample1")));

    executeString(String.format("create external table self_desc_table2 using json location '%s'",
        getDataSetFile("sample2")));

    executeString(String.format("create external table self_desc_table3 using json location '%s'",
        getDataSetFile("tweets")));
  }

  @After
  public void teardown() throws TajoException {
    executeString("drop table self_desc_table1");
    executeString("drop table self_desc_table2");
    executeString("drop table self_desc_table3");
  }

  @Test
  public final void testSelect() throws Exception {
    ResultSet res = executeString("select glossary.title, glossary.\"GlossDiv\".title, glossary.\"GlossDiv\".null_expected, glossary.\"GlossDiv\".\"GlossList\".\"GlossEntry\".\"SortAs\", glossary.\"GlossDiv\".\"GlossList\".\"GlossEntry\".\"Abbrev\" from self_desc_table2");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testSelect2() throws Exception {
    ResultSet res = executeString("select glossary.title, glossary.\"GlossDiv\".title, glossary.\"GlossDiv\".null_expected, glossary.\"GlossDiv\".\"GlossList\".\"GlossEntry\".\"SortAs\" from self_desc_table2 where glossary.\"GlossDiv\".\"GlossList\".\"GlossEntry\".\"Abbrev\" = 'ISO 8879:1986'");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testGroupby() throws Exception {
    ResultSet res = executeString("select name.first_name, count(*) from self_desc_table1 group by name.first_name");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testGroupby2() throws Exception {
    ResultSet res = executeString("select coordinates, avg(retweet_count::int4) from self_desc_table3 group by coordinates");
    System.out.println(resultSetToString(res));
  }
}
