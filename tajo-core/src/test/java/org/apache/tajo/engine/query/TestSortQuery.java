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

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class TestSortQuery extends QueryTestCaseBase {

  public TestSortQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
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
    System.out.println(resultSetToString(res));
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
      KeyValueSet tableOptions = new KeyValueSet();
      tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
      tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

      Schema schema = new Schema();
      schema.addColumn("col1", Type.INT4);
      schema.addColumn("col2", Type.TEXT);
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
      TajoTestingCluster.createTable("sortfirstdesc", schema, tableOptions, data, 2);

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
    // skip this test if catalog uses HCatalogStore.
    // It is because HCatalogStore does not support Time data type.

    if (!testingCluster.isHCatalogStoreRunning()) {
      // create external table table1 (col1 timestamp, col2 date, col3 time) ...
      executeDDL("create_table_with_date_ddl.sql", "table1");

      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
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
    // select l_orderkey, l_linenumber from lineitem order by l_orderkey desc limit 3;
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSortOnNullColumn() throws Exception {
    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "2");
      KeyValueSet tableOptions = new KeyValueSet();
      tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
      tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

      Schema schema = new Schema();
      schema.addColumn("id", Type.INT4);
      schema.addColumn("name", Type.TEXT);
      String[] data = new String[]{
          "1|BRAZIL",
          "2|ALGERIA",
          "3|ARGENTINA",
          "4|CANADA"
      };
      TajoTestingCluster.createTable("nullsort", schema, tableOptions, data, 2);

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
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1|111", "2|\\N", "3|333" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select * from table11 order by name asc");
      String ascExpected = "id,name\n" +
          "-------------------------------\n" +
          "1,111\n" +
          "3,333\n" +
          "2,null\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();

      res = executeString("select * from table11 order by name desc");
      String descExpected = "id,name\n" +
          "-------------------------------\n" +
          "2,null\n" +
          "3,333\n" +
          "1,111\n";

      assertEquals(descExpected, resultSetToString(res));
      res.close();
    } finally {
      executeString("DROP TABLE table11 PURGE");
    }
  }

  @Test
  public final void testSortOnNullColumn3() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1|111", "2|\\N", "3|333" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 1);

    try {
      ResultSet res = executeString("select * from table11 order by name null first");
      String ascExpected = "id,name\n" +
          "-------------------------------\n" +
          "2,null\n" +
          "1,111\n" +
          "3,333\n";

      assertEquals(ascExpected, resultSetToString(res));
      res.close();

    } finally {
      executeString("DROP TABLE table11 PURGE");
    }
  }

  @Test
  public final void testSortOnUnicodeTextAsc() throws Exception {
    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "2");
      KeyValueSet tableOptions = new KeyValueSet();
      tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
      tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

      Schema schema = new Schema();
      schema.addColumn("col1", Type.INT4);
      schema.addColumn("col2", Type.TEXT);
      String[] data = new String[]{
          "1|하하하",
          "2|캬캬캬",
          "3|가가가",
          "4|냐하하"
      };
      TajoTestingCluster.createTable("unicode_sort1", schema, tableOptions, data, 2);

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
      KeyValueSet tableOptions = new KeyValueSet();
      tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
      tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

      Schema schema = new Schema();
      schema.addColumn("col1", Type.INT4);
      schema.addColumn("col2", Type.TEXT);
      String[] data = new String[]{
          "1|하하하",
          "2|캬캬캬",
          "3|가가가",
          "4|냐하하"
      };
      TajoTestingCluster.createTable("unicode_sort2", schema, tableOptions, data, 2);

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
}
