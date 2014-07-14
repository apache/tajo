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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestCreateTable extends QueryTestCaseBase {

  @Test
  public final void testVariousTypes() throws Exception {
    List<String> createdNames;
    if (testingCluster.isHCatalogStoreRunning()) {
      createdNames = executeDDL("create_table_various_types_for_hcatalog.sql", null);
    } else {
      createdNames = executeDDL("create_table_various_types.sql", null);
    }
    assertTableExists(createdNames.get(0));
  }

  @Test
  public final void testCreateTable1() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1", "table1");
    assertTableExists(createdNames.get(0));
    executeString("DROP TABLE table1");
  }

  @Test
  public final void testCreateTable2() throws Exception {
    executeString("CREATE DATABASE D1;").close();
    executeString("CREATE DATABASE D2;").close();

    executeString("CREATE TABLE D1.table1 (age int);").close();
    executeString("CREATE TABLE D1.table2 (age int);").close();
    executeString("CREATE TABLE d2.table3 (age int);").close();
    executeString("CREATE TABLE d2.table4 (age int);").close();

    assertTableExists("d1.table1");
    assertTableExists("d1.table2");
    assertTableNotExists("d2.table1");
    assertTableNotExists("d2.table2");

    assertTableExists("d2.table3");
    assertTableExists("d2.table4");
    assertTableNotExists("d1.table3");
    assertTableNotExists("d1.table4");

    executeString("DROP TABLE D1.table1");
    executeString("DROP TABLE D1.table2");
    executeString("DROP TABLE D2.table3");
    executeString("DROP TABLE D2.table4");

    assertDatabaseExists("d1");
    assertDatabaseExists("d2");
    executeString("DROP DATABASE D1").close();
    executeString("DROP DATABASE D2").close();
    assertDatabaseNotExists("d1");
    assertDatabaseNotExists("d2");
  }

  private final void assertPathOfCreatedTable(final String databaseName,
                                              final String originalTableName,
                                              final String newTableName,
                                              String createTableStmt) throws Exception {
    // create one table
    executeString("CREATE DATABASE " + CatalogUtil.denormalizeIdentifier(databaseName)).close();
    getClient().existDatabase(CatalogUtil.denormalizeIdentifier(databaseName));
    final String oldFQTableName = CatalogUtil.buildFQName(databaseName, originalTableName);

    ResultSet res = executeString(createTableStmt);
    res.close();
    assertTableExists(oldFQTableName);
    TableDesc oldTableDesc = client.getTableDesc(oldFQTableName);


    // checking the existence of the table directory and validating the path
    FileSystem fs = testingCluster.getMaster().getStorageManager().getFileSystem();
    Path warehouseDir = TajoConf.getWarehouseDir(testingCluster.getConfiguration());
    assertTrue(fs.exists(oldTableDesc.getPath()));
    assertEquals(StorageUtil.concatPath(warehouseDir, databaseName, originalTableName), oldTableDesc.getPath());

    // Rename
    client.executeQuery("ALTER TABLE " + CatalogUtil.denormalizeIdentifier(oldFQTableName)
        + " RENAME to " + CatalogUtil.denormalizeIdentifier(newTableName));

    // checking the existence of the new table directory and validating the path
    final String newFQTableName = CatalogUtil.buildFQName(databaseName, newTableName);
    TableDesc newTableDesc = client.getTableDesc(newFQTableName);
    assertTrue(fs.exists(newTableDesc.getPath()));
    assertEquals(StorageUtil.concatPath(warehouseDir, databaseName, newTableName), newTableDesc.getPath());
  }

  @Test
  public final void testCreatedTableViaCTASAndVerifyPath() throws Exception {
    assertPathOfCreatedTable("d4", "old_table", "new_mgmt_table",
        "CREATE TABLE d4.old_table AS SELECT * FROM default.lineitem;");
  }

  @Test
  public final void testCreatedTableJustCreatedAndVerifyPath() throws Exception {
    assertPathOfCreatedTable("d5", "old_table", "new_mgmt_table", "CREATE TABLE d5.old_table (age integer);");
  }

  @Test
  public final void testCreatedTableWithQuotedIdentifierAndVerifyPath() throws Exception {
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertPathOfCreatedTable("D6", "OldTable", "NewMgmtTable", "CREATE TABLE \"D6\".\"OldTable\" (age integer);");
    }
  }

  @Test
  public final void testCreateTableIfNotExists() throws Exception {
    executeString("CREATE DATABASE D3;").close();

    assertTableNotExists("d3.table1");
    executeString("CREATE TABLE D3.table1 (age int);").close();
    assertTableExists("d3.table1");

    executeString("CREATE TABLE IF NOT EXISTS D3.table1 (age int);").close();
    assertTableExists("d3.table1");

    executeString("DROP TABLE D3.table1");
  }

  @Test
  public final void testDropTableIfExists() throws Exception {
    executeString("CREATE DATABASE D7;").close();

    assertTableNotExists("d7.table1");
    executeString("CREATE TABLE d7.table1 (age int);").close();
    assertTableExists("d7.table1");

    executeString("DROP TABLE d7.table1;").close();
    assertTableNotExists("d7.table1");

    executeString("DROP TABLE IF EXISTS d7.table1");
    assertTableNotExists("d7.table1");

    executeString("DROP DATABASE D7;").close();
  }

  @Test
  public final void testDelimitedIdentifierWithNonAsciiCharacters() throws Exception {

    if (!testingCluster.isHCatalogStoreRunning()) {
      ResultSet res = null;
      try {
        List<String> tableNames = executeDDL("quoted_identifier_non_ascii_ddl.sql", "table1", "\"테이블1\"");
        assertTableExists(tableNames.get(0));

        // SELECT "아이디", "텍스트", "숫자" FROM "테이블1";
        res = executeFile("quoted_identifier_non_ascii_1.sql");
        assertResultSet(res, "quoted_identifier_non_ascii_1.result");
      } finally {
        cleanupQuery(res);
      }

      // SELECT "아이디" as "진짜아이디", "텍스트" as text, "숫자" FROM "테이블1" as "테이블 별명"
      try {
        res = executeFile("quoted_identifier_non_ascii_2.sql");
        assertResultSet(res, "quoted_identifier_non_ascii_2.result");
      } finally {
        cleanupQuery(res);
      }

      // SELECT "아이디" "진짜아이디", char_length("텍스트") as "길이", "숫자" * 2 FROM "테이블1" "테이블 별명"
      try {
        res = executeFile("quoted_identifier_non_ascii_3.sql");
        assertResultSet(res, "quoted_identifier_non_ascii_3.result");
      } finally {
        cleanupQuery(res);
      }
    }
  }

  @Test
  public final void testDelimitedIdentifierWithMixedCharacters() throws Exception {
    if (!testingCluster.isHCatalogStoreRunning()) {
      ResultSet res = null;

      try {
        List<String> tableNames = executeDDL("quoted_identifier_mixed_chars_ddl_1.sql", "table1", "\"TABLE1\"");
        assertTableExists(tableNames.get(0));

        tableNames = executeDDL("quoted_identifier_mixed_chars_ddl_1.sql", "table2", "\"tablE1\"");
        assertTableExists(tableNames.get(0));

        // SELECT "aGe", "tExt", "Number" FROM "TABLE1";
        res = executeFile("quoted_identifier_mixed_chars_1.sql");
        assertResultSet(res, "quoted_identifier_mixed_chars_1.result");
      } finally {
        cleanupQuery(res);
      }

      try {
        res = executeFile("quoted_identifier_mixed_chars_2.sql");
        assertResultSet(res, "quoted_identifier_mixed_chars_2.result");
      } finally {
        cleanupQuery(res);
      }

      try {
        res = executeFile("quoted_identifier_mixed_chars_3.sql");
        assertResultSet(res, "quoted_identifier_mixed_chars_3.result");
      } finally {
        cleanupQuery(res);
      }
    }
  }

  @Test
  public final void testNonreservedKeywordTableNames() throws Exception {
    List<String> createdNames = null;
    createdNames = executeDDL("table1_ddl.sql", "table1", "filter");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "first");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "format");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "grouping");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "hash");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "index");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "insert");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "last");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "location");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "max");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "min");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "national");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "nullif");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "overwrite");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "precision");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "range");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "regexp");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "rlike");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "set");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "unknown");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "var_pop");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "var_samp");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "varying");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "zone");
    assertTableExists(createdNames.get(0));

    createdNames = executeDDL("table1_ddl.sql", "table1", "bigint");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "bit");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "blob");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "bool");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "boolean");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "bytea");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "char");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "date");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "decimal");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "double");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "float");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "float4");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "float8");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "inet4");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "int");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "int1");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "int2");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "int4");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "int8");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "integer");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "nchar");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "numeric");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "nvarchar");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "real");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "smallint");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "text");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "time");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "timestamp");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "timestamptz");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "timetz");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "tinyint");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "varbinary");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "varbit");
    assertTableExists(createdNames.get(0));
    createdNames = executeDDL("table1_ddl.sql", "table1", "varchar");
    assertTableExists(createdNames.get(0));
  }

  private boolean isClonedSchema(Schema origSchema, Schema newSchema)  {
    // Check schema of tables
    boolean schemaEqual =
      (origSchema.size() == newSchema.size());
    if(schemaEqual == false)  {
      fail("Number of columns in schema not equal");
      return false;
    }

    for(int col = 0; col < origSchema.size(); col++)  {
      Column colA = origSchema.getColumn(col);
      Column colB = newSchema.getColumn(col);
      if(colA.getSimpleName().equals(colB.getSimpleName()) == false)  {
        fail("Column names at index " + col + " do not match");
        return false;
      }
      if(colA.getDataType().equals(colB.getDataType()) == false) {
        fail("Column datatypes at index " + col + " do not match");
        return false;
      }
    }
    return true;
  }

  private boolean isClonedTable(String orignalTable, String newTable) throws Exception  {
    assertTableExists(newTable);
    TableDesc origTableDesc = client.getTableDesc(orignalTable);
    TableDesc newTableDesc = client.getTableDesc(newTable);

    if(isClonedSchema(origTableDesc.getSchema(), newTableDesc.getSchema()) == false) {
      fail("Schema of input tables do not match");
      return false;
    }

    // Check partition information
    PartitionMethodDesc origPartMethod = origTableDesc.getPartitionMethod();
    PartitionMethodDesc newPartMethod = newTableDesc.getPartitionMethod();
    if(origPartMethod != null) {
      if(newPartMethod == null)  {
        fail("New table does not have partition info");
        return false;
      }
      if(isClonedSchema(origPartMethod.getExpressionSchema(),
                        newPartMethod.getExpressionSchema()) == false) {
	fail("Partition columns of input tables do not match");
        return false;
      }

      if(origPartMethod.getPartitionType().equals(newPartMethod.getPartitionType()) == false)  {
        fail("Partition type of input tables do not match");
        return false;
      }
    }

    // Check external flag
    if(origTableDesc.isExternal() != newTableDesc.isExternal()) {
      fail("External table flag on input tables not equal");
      return false;
    }

    if(origTableDesc.getMeta() != null) {
      TableMeta origMeta = origTableDesc.getMeta();
      TableMeta newMeta = newTableDesc.getMeta();
      if(origMeta.getStoreType().equals(newMeta.getStoreType()) == false) {
        fail("Store type of input tables not equal");
        return false;
      }

      KeyValueSet origOptions = origMeta.getOptions();
      KeyValueSet newOptions = newMeta.getOptions();
      if(origOptions.equals(newOptions) == false)  {
        fail("Meta options of input tables not equal");
        return false;
      }
    }
    return true;
  }

  @Test
  public final void testCreateTableLike1() throws Exception {
    // Hcatalog does not support varchar type in hive-0.12.0
    if (testingCluster.isHCatalogStoreRunning()) {
      // Basic create table with default database
      executeString("CREATE TABLE table1 (c1 int, c2 text);").close();
      executeString("CREATE TABLE table2 LIKE table1");
      String testMsg = "testCreateTableLike1: Basic create table with default db";
      assertTrue(testMsg,isClonedTable("table1","table2"));
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");

      // Basic create table with database
      executeString("CREATE DATABASE d1").close();
      executeString("CREATE TABLE d1.table1 (c1 int, c2 text);").close();
      executeString("CREATE TABLE d1.table2 LIKE d1.table1");
      testMsg = "testCreateTableLike1: Basic create table with db test failed";
      assertTrue(testMsg, isClonedTable("d1.table1","d1.table2"));
      executeString("DROP TABLE d1.table1");
      executeString("DROP TABLE d1.table2");

      // Table with non-default store type
      executeString("CREATE TABLE table1 (c1 int, c2 text) USING rcfile;").close();
      executeString("CREATE TABLE table2 LIKE table1");
      testMsg = "testCreateTableLike1: Table with non-default store type test failed";
      assertTrue(testMsg, isClonedTable("table1","table2"));
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");

      // Table with non-default meta options
      executeString("CREATE TABLE table1 (c1 int, c2 text) USING csv WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec');").close();
      executeString("CREATE TABLE table2 LIKE table1");
      testMsg = "testCreateTableLike1: Table with non-default meta options test failed";
      assertTrue(testMsg, isClonedTable("table1","table2"));
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");


      // Table with partitions (default partition type)
      executeString("CREATE TABLE table1 (c1 int, c2 text) PARTITION BY COLUMN (c3 int, c4 float, c5 text);").close();
      executeString("CREATE TABLE table2 LIKE table1");
      testMsg = "testCreateTableLike1: Table with partitions test failed";
      assertTrue(testMsg, isClonedTable("table1","table2"));
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");


      // Table with external flag
      // Use existing file as input for creating external table
      String className = getClass().getSimpleName();
      Path currentDatasetPath = new Path(datasetBasePath, className);
      Path filePath = StorageUtil.concatPath(currentDatasetPath, "table1");
      executeString("CREATE EXTERNAL TABLE table3 (c1 int, c2 text) USING rcfile LOCATION '" + filePath.toUri() + "'").close();
      executeString("CREATE TABLE table2 LIKE table3");
      testMsg = "testCreateTableLike1: Table with external table flag test failed";
      assertTrue(testMsg, isClonedTable("table3","table2"));
      executeString("DROP TABLE table3");
      executeString("DROP TABLE table2");


      // Table created using CTAS
      executeString("CREATE TABLE table3 (c1 int, c2 text) PARTITION BY COLUMN (c3 int);").close();
      executeString("CREATE TABLE table4 AS SELECT c1 * c1 as m_c1, c2, c2 as c2_a,c3 from table3;").close();
      executeString("CREATE TABLE table2 LIKE table4");
      testMsg = "testCreateTableLike1: Table using CTAS test failed";
      assertTrue(testMsg, isClonedTable("table4","table2"));
      executeString("DROP TABLE table3");
      executeString("DROP TABLE table4");
      executeString("DROP TABLE table2");
    } else {
      // Basic create table with default database
      executeString("CREATE TABLE table1 (c1 int, c2 varchar);").close();
      executeString("CREATE TABLE table2 LIKE table1");
      String testMsg = "testCreateTableLike1: Basic create table with default db";
      assertTrue(testMsg,isClonedTable("table1","table2"));
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");

      // Basic create table with database
      executeString("CREATE DATABASE d1").close();
      executeString("CREATE TABLE d1.table1 (c1 int, c2 varchar);").close();
      executeString("CREATE TABLE d1.table2 LIKE d1.table1");
      testMsg = "testCreateTableLike1: Basic create table with db test failed";
      assertTrue(testMsg, isClonedTable("d1.table1","d1.table2"));
      executeString("DROP TABLE d1.table1");
      executeString("DROP TABLE d1.table2");

      // Table with non-default store type
      executeString("CREATE TABLE table1 (c1 int, c2 varchar) USING rcfile;").close();
      executeString("CREATE TABLE table2 LIKE table1");
      testMsg = "testCreateTableLike1: Table with non-default store type test failed";
      assertTrue(testMsg, isClonedTable("table1","table2"));
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");

      // Table with non-default meta options
      executeString("CREATE TABLE table1 (c1 int, c2 varchar) USING csv WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec');").close();
      executeString("CREATE TABLE table2 LIKE table1");
      testMsg = "testCreateTableLike1: Table with non-default meta options test failed";
      assertTrue(testMsg, isClonedTable("table1","table2"));
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");


      // Table with partitions (default partition type)
      executeString("CREATE TABLE table1 (c1 int, c2 varchar) PARTITION BY COLUMN (c3 int, c4 float, c5 text);").close();
      executeString("CREATE TABLE table2 LIKE table1");
      testMsg = "testCreateTableLike1: Table with partitions test failed";
      assertTrue(testMsg, isClonedTable("table1","table2"));
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");


      // Table with external flag
      // Use existing file as input for creating external table
      String className = getClass().getSimpleName();
      Path currentDatasetPath = new Path(datasetBasePath, className);
      Path filePath = StorageUtil.concatPath(currentDatasetPath, "table1");
      executeString("CREATE EXTERNAL TABLE table3 (c1 int, c2 varchar) USING rcfile LOCATION '" + filePath.toUri() + "'").close();
      executeString("CREATE TABLE table2 LIKE table3");
      testMsg = "testCreateTableLike1: Table with external table flag test failed";
      assertTrue(testMsg, isClonedTable("table3","table2"));
      executeString("DROP TABLE table3");
      executeString("DROP TABLE table2");


      // Table created using CTAS
      executeString("CREATE TABLE table3 (c1 int, c2 varchar) PARTITION BY COLUMN (c3 int);").close();
      executeString("CREATE TABLE table4 AS SELECT c1*c1, c2, c2 as c2_a,c3 from table3;").close();
      executeString("CREATE TABLE table2 LIKE table4");
      testMsg = "testCreateTableLike1: Table using CTAS test failed";
      assertTrue(testMsg, isClonedTable("table4","table2"));
      executeString("DROP TABLE table3");
      executeString("DROP TABLE table4");
      executeString("DROP TABLE table2");


    /* Enable when view is supported
    // View
    executeString("CREATE TABLE table3 (c1 int, c2 varchar) PARTITION BY COLUMN (c3 int);").close();
    executeString("CREATE VIEW table4(c1,c2,c3) AS SELECT c1*c1, c2, c2,c3 from table3;").close();
    executeString("CREATE TABLE table2 LIKE table4");
    testMsg = "testCreateTableLike1: Table using VIEW test failed";
    assertTrue(testMsg, isClonedTable("table4","table2"));
    executeString("DROP TABLE table3");
    executeString("DROP TABLE table4");
    executeString("DROP TABLE table2");
    */

    /*  Enable when partition type other than column is supported
    // Table with partitions (range partition)
    executeString("CREATE TABLE table1 (c1 int, c2 varchar) PARTITION BY RANGE (c1) (  PARTITION c1 VALUES LESS THAN (2),  PARTITION c1 VALUES LESS THAN (5),  PARTITION c1 VALUES LESS THAN (MAXVALUE) );").close();
    executeString("CREATE TABLE table2 LIKE table1");
    testMsg = "testCreateTableLike1: Table using non-default partition type failed";
    assertTrue(testMsg, isClonedTable("table1","table2"));
    executeString("DROP TABLE table1");
    executeString("DROP TABLE table2");
    */
    }
  }
}
