/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.query;

import org.apache.tajo.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class TestSortQuery extends QueryTestCaseBase {

  public TestSortQuery(String sortAlgorithm) {
    super(TajoConstants.DEFAULT_DATABASE_NAME);

    Map<String, String> variables = new HashMap<>();
    variables.put(SessionVars.SORT_LIST_SIZE.keyname(), "100");
    variables.put(SessionVars.SORT_ALGORITHM.keyname(), sortAlgorithm);
    client.updateSessionVariables(variables);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.unsetSessionVariables(Arrays.asList(SessionVars.SORT_ALGORITHM.keyname()));
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {"TIM"},
        {"MSD_RADIX"},
    });
  }

  @Test
  public final void testSort() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithAlias1() throws Exception {
    // select l_linenumber, l_orderkey as sortkey from lineitem order by sortkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithAlias2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithAlias3() throws Exception {
    ResultSet res = executeQuery();
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithExpr1() throws Exception {
    // select l_linenumber, l_orderkey as sortkey from lineitem order by l_orderkey + 1;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithExpr2() throws Exception {
    // select l_linenumber, l_orderkey as sortkey from lineitem order by l_linenumber, l_orderkey, (l_orderkey is null);
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithAliasButOriginalName() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortDesc() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }


  @Test
  public final void testSortFirstDesc() throws Exception {
    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "2");

      Schema schema = SchemaBuilder.builder()
          .add("col1", Type.INT4)
          .add("col2", Type.TEXT)
          .build();
      String[] data = new String[]{
          "1|abc",
          "3|dfa",
          "3|das",
          "1|abb",
          "1|abc",
          "3|dfb",
          "3|dat",
          "1|abe"
      };
      TajoTestingCluster.createTable(conf, "sortfirstdesc", schema, data, 2);

      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "0");
      executeString("DROP TABLE sortfirstdesc PURGE;").close();
    }
  }


  @Test
  public final void testTopK() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortAfterGroupby() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortAfterGroupbyWithAlias() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithDate() throws Exception {
    // skip this test if catalog uses HiveCatalogStore.
    // It is because HiveCatalogStore does not support Time data type.

    if (!testingCluster.isHiveCatalogStoreRunning()) {
      // create external table table1 (col1 timestamp, col2 date, col3 time) ...
      executeDDL("create_table_with_date_ddl.sql", "table1");

      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);

      executeString("drop table testSortWithDate");
    }
  }

  @Test
  public final void testAsterisk() throws Exception {
    //select *, length(l_comment) as len_comment from lineitem order by len_comment;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortWithAscDescKeys() throws Exception {
    executeDDL("create_table_with_asc_desc_keys.sql", "table2");

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("drop table table2");
  }

  @Test
  public final void testSortWithJson() throws Exception {
    // select max(l_quantity) as max_quantity, l_orderkey from lineitem group by l_orderkey order by max_quantity;
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTopkWithJson() throws Exception {
    // select l_orderkey, l_linenumber from lineitem order by l_orderkey desc limit 5;
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortOnNullColumn() throws Exception {
    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "2");

      Schema schema = SchemaBuilder.builder()
          .add("id", Type.INT4)
          .add("name", Type.TEXT)
          .build();
      String[] data = new String[]{
          "1|BRAZIL",
          "2|ALGERIA",
          "3|ARGENTINA",
          "4|CANADA"
      };
      TajoTestingCluster.createTable(conf, "nullsort", schema, data, 2);

      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "0");
      executeString("DROP TABLE nullsort PURGE;").close();
    }
  }

  @Test
  public final void testSortOnNullColumn2() throws Exception {
    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("name", Type.TEXT)
        .build();
    String[] data = new String[]{ "1|111", "2|\\N", "3|333" };
    TajoTestingCluster.createTable(conf, "testSortOnNullColumn2".toLowerCase(), schema, data, 1);

    try {
      ResultSet res = executeString("select * from testSortOnNullColumn2 order by name asc");
      String ascExpected = "id,name\n" +
          "-------------------------------\n" +
          "1,111\n" +
          "3,333\n" +
          "2,null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();

      res = executeString("select * from testSortOnNullColumn2 order by name desc");
      String descExpected = "id,name\n" +
          "-------------------------------\n" +
          "2,null\n" +
          "3,333\n" +
          "1,111\n";

      assertEquals(descExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE testSortOnNullColumn2 PURGE");
    }
  }

  @Test
  public final void testSortOnNullColumn3() throws Exception {
    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("name", Type.TEXT)
        .build();
    String[] data = new String[]{ "1|111", "2|\\N", "3|333" };
    TajoTestingCluster.createTable(conf, "testSortOnNullColumn3".toLowerCase(), schema, data, 1);

    try {
      ResultSet res = executeString("select * from testSortOnNullColumn3 order by name nulls first");
      String ascExpected = "id,name\n" +
          "-------------------------------\n" +
          "2,null\n" +
          "1,111\n" +
          "3,333\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();

    } finally {
      executeString("DROP TABLE testSortOnNullColumn3 PURGE");
    }
  }

  @Test
  public final void testSortOnNullColumn4() throws Exception {
    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("name", Type.TEXT)
        .build();
    String[] data = new String[]{ "1|111", "2|\\N", "3|333" };
    TajoTestingCluster.createTable(conf, "testSortOnNullColumn4".toLowerCase(), schema, data, 1);

    try {
      ResultSet res = executeString("select * from testSortOnNullColumn4 order by name desc nulls last");
      String ascExpected = "id,name\n" +
          "-------------------------------\n" +
          "3,333\n" +
          "1,111\n" +
          "2,null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();

    } finally {
      executeString("DROP TABLE testSortOnNullColumn4 PURGE");
    }
  }

  @Test
  public final void testSortOnNullColumn5() throws Exception {
    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("name", Type.TEXT)
        .build();
    String[] data = new String[]{ "1|111", "2|\\N", "3|333" };
    TajoTestingCluster.createTable(conf, "testSortOnNullColumn5".toLowerCase(), schema, data, 1);

    try {
      ResultSet res = executeString("select * from testSortOnNullColumn5 order by name asc nulls first");
      String ascExpected = "id,name\n" +
          "-------------------------------\n" +
          "2,null\n" +
          "1,111\n" +
          "3,333\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();

    } finally {
      executeString("DROP TABLE testSortOnNullColumn5 PURGE");
    }
  }

  @Test
  public final void testSortOnUnicodeTextAsc() throws Exception {
    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "2");
      Schema schema = SchemaBuilder.builder()
          .add("col1", Type.INT4)
          .add("col2", Type.TEXT)
          .build();
      String[] data = new String[]{
          "1|하하하",
          "2|캬캬캬",
          "3|가가가",
          "4|냐하하"
      };
      TajoTestingCluster.createTable(conf, "unicode_sort1", schema, data, 2);

      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "0");
      executeString("DROP TABLE unicode_sort1 PURGE;").close();
    }
  }

  @Test
  public final void testSortOnUnicodeTextDesc() throws Exception {
    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "2");

      Schema schema = SchemaBuilder.builder()
          .add("col1", Type.INT4)
          .add("col2", Type.TEXT)
          .build();
      String[] data = new String[]{
          "1|하하하",
          "2|캬캬캬",
          "3|가가가",
          "4|냐하하"
      };
      TajoTestingCluster.createTable(conf, "unicode_sort2", schema, data, 2);

      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "0");
      executeString("DROP TABLE unicode_sort2 PURGE;").close();
    }
  }

  @Test
  public final void testSortWithConstKeys() throws Exception {
    // select
    //   l_orderkey,
    //   l_linenumber,
    //   1 as key1,
    //   2 as key2
    // from
    //   lineitem
    // order by
    //   key1,
    //  key2;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true)
  @SimpleTest()
  public final void testSubQuerySortAfterGroupMultiBlocks() throws Exception {
    runSimpleTests();
  }

  @Test
  public final void testOutOfScope() throws Exception {
    executeDDL("create_table_with_unique_small_dataset.sql", "table3");
    // table has 5 files
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "5");
    try {
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "0");
      executeString("drop table testOutOfScope");
    }
  }
}
