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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.ResultSet;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestInsertQuery extends QueryTestCaseBase {

  @Test
  public final void testInsertOverwrite() throws Exception {
    ResultSet res = executeFile("table1_ddl.sql");
    res.close();

    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), "table1"));

    res = executeFile("testInsertOverwrite.sql");
    res.close();

    TableDesc desc = catalog.getTableDesc(getCurrentDatabase(), "table1");
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    executeString("DROP TABLE table1 PURGE");
  }

  @Test
  public final void testInsertOverwriteSmallerColumns() throws Exception {
    ResultSet res = executeFile("table1_ddl.sql");
    res.close();

    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), "table1"));
    TableDesc originalDesc = catalog.getTableDesc(getCurrentDatabase(), "table1");

    res = executeFile("testInsertOverwriteSmallerColumns.sql");
    res.close();
    TableDesc desc = catalog.getTableDesc(getCurrentDatabase(), "table1");
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }
    assertEquals(originalDesc.getSchema(), desc.getSchema());

    executeString("DROP TABLE table1 PURGE");
  }

  @Test
  public final void testInsertOverwriteWithTargetColumns() throws Exception {
    ResultSet res = executeFile("table1_ddl.sql");
    res.close();

    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), "table1"));
    TableDesc originalDesc = catalog.getTableDesc(getCurrentDatabase(), "table1");

    res = executeFile("testInsertOverwriteWithTargetColumns.sql");
    res.close();
    TableDesc desc = catalog.getTableDesc(getCurrentDatabase(), "table1");
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    res = executeString("select * from " + CatalogUtil.denormalizeIdentifier(getCurrentDatabase()) + ".table1");

    assertTrue(res.next());
    assertEquals(1, res.getLong(1));
    assertTrue(0f == res.getFloat(2));
    assertTrue(res.wasNull());
    assertTrue(17.0 == res.getFloat(3));

    assertTrue(res.next());
    assertEquals(1, res.getLong(1));
    assertTrue(0f == res.getFloat(2));
    assertTrue(res.wasNull());
    assertTrue(36.0 == res.getFloat(3));

    assertTrue(res.next());
    assertEquals(2, res.getLong(1));
    assertTrue(0f == res.getFloat(2));
    assertTrue(res.wasNull());
    assertTrue(38.0 == res.getFloat(3));

    assertTrue(res.next());
    assertTrue(0f == res.getFloat(2));
    assertTrue(res.wasNull());
    assertTrue(45.0 == res.getFloat(3));

    assertTrue(res.next());
    assertEquals(3, res.getLong(1));
    assertTrue(0f == res.getFloat(2));
    assertTrue(res.wasNull());
    assertTrue(49.0 == res.getFloat(3));

    assertFalse(res.next());
    res.close();

    assertEquals(originalDesc.getSchema(), desc.getSchema());

    executeString("DROP TABLE table1 PURGE");
  }

  @Test
  public final void testInsertOverwriteWithAsterisk() throws Exception {
    ResultSet res = executeFile("full_table_csv_ddl.sql");
    res.close();

    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), "full_table_csv"));

    res = executeString("insert overwrite into full_table_csv select * from default.lineitem where l_orderkey = 3");
    res.close();
    TableDesc desc = catalog.getTableDesc(getCurrentDatabase(), "full_table_csv");
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(2, desc.getStats().getNumRows().intValue());
    }
    executeString("DROP TABLE full_table_csv PURGE");
  }

  @Test
  public final void testInsertOverwriteIntoSelect() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("insertoverwriteintoselect");
    ResultSet res = executeString("create table " + tableName + " as select l_orderkey from default.lineitem");
    assertFalse(res.next());
    res.close();


    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), tableName));
    TableDesc orderKeys = catalog.getTableDesc(getCurrentDatabase(), tableName);
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(5, orderKeys.getStats().getNumRows().intValue());
    }

    // this query will result in the two rows.
    res = executeString("insert overwrite into " + tableName + " select l_orderkey from default.lineitem where l_orderkey = 3");
    assertFalse(res.next());
    res.close();

    assertTrue(catalog.existsTable(getCurrentDatabase(), tableName));
    orderKeys = catalog.getTableDesc(getCurrentDatabase(), tableName);
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(2, orderKeys.getStats().getNumRows().intValue());
    }
    executeString("DROP TABLE " + tableName + " PURGE");
  }

  @Test
  public final void testInsertOverwriteCapitalTableName() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testInsertOverwriteCapitalTableName");
    ResultSet res = executeString("create table " + tableName + " as select * from default.lineitem");
    res.close();

    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), tableName));

    res = executeString("insert overwrite into " + tableName + " select * from default.lineitem where l_orderkey = 3");
    res.close();
    TableDesc desc = catalog.getTableDesc(getCurrentDatabase(), tableName);
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(2, desc.getStats().getNumRows().intValue());
    }
    executeString("DROP TABLE " + tableName + " PURGE");
  }

  @Test
  public final void testInsertOverwriteLocation() throws Exception {
    ResultSet res = executeQuery();
    res.close();
    FileSystem fs = FileSystem.get(testingCluster.getConfiguration());
    assertTrue(fs.exists(new Path("/tajo-data/testInsertOverwriteCapitalTableName")));
    assertEquals(1, fs.listStatus(new Path("/tajo-data/testInsertOverwriteCapitalTableName")).length);
  }

  @Test
  public final void testInsertOverwriteWithCompression() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testInsertOverwriteWithCompression");
    ResultSet res = executeFile("testInsertOverwriteWithCompression_ddl.sql");
    res.close();

    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), tableName));

    res = executeQuery();
    res.close();
    TableDesc desc = catalog.getTableDesc(getCurrentDatabase(), tableName);
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(2, desc.getStats().getNumRows().intValue());
    }

    FileSystem fs = FileSystem.get(testingCluster.getConfiguration());
    assertTrue(fs.exists(desc.getPath()));
    CompressionCodecFactory factory = new CompressionCodecFactory(testingCluster.getConfiguration());

    for (FileStatus file : fs.listStatus(desc.getPath())) {
      CompressionCodec codec = factory.getCodec(file.getPath());
      assertTrue(codec instanceof DeflateCodec);
    }
    executeString("DROP TABLE " + tableName + " PURGE");
  }

  @Test
  public final void testInsertOverwriteLocationWithCompression() throws Exception {
    if (!testingCluster.isHCatalogStoreRunning()) {
      ResultSet res = executeQuery();
      res.close();
      FileSystem fs = FileSystem.get(testingCluster.getConfiguration());
      Path path = new Path("/tajo-data/testInsertOverwriteLocationWithCompression");
      assertTrue(fs.exists(path));
      assertEquals(1, fs.listStatus(path).length);

      CompressionCodecFactory factory = new CompressionCodecFactory(testingCluster.getConfiguration());
      for (FileStatus file : fs.listStatus(path)){
        CompressionCodec codec = factory.getCodec(file.getPath());
        assertTrue(codec instanceof DeflateCodec);
      }
    }
  }

  @Test
  public final void testInsertOverwriteWithAsteriskUsingParquet() throws Exception {
    if (!testingCluster.isHCatalogStoreRunning()) {
      ResultSet res = executeFile("full_table_parquet_ddl.sql");
      res.close();

      CatalogService catalog = testingCluster.getMaster().getCatalog();
      assertTrue(catalog.existsTable(getCurrentDatabase(), "full_table_parquet"));

      res = executeString(
          "insert overwrite into full_table_parquet select * from default.lineitem where l_orderkey = 3");
      res.close();
      TableDesc desc = catalog.getTableDesc(getCurrentDatabase(), "full_table_parquet");
      if (!testingCluster.isHCatalogStoreRunning()) {
        assertEquals(2, desc.getStats().getNumRows().intValue());
      }

      res = executeString("select * from full_table_parquet;");
      assertResultSet(res);

      res = executeString("select l_orderkey, l_partkey from full_table_parquet;");
      assertResultSet(res, "testInsertOverwriteWithAsteriskUsingParquet2.result");

      executeString("DROP TABLE full_table_parquet PURGE");
    }
  }

  @Test
  public final void testInsertOverwriteWithDatabase() throws Exception {
    ResultSet res = executeFile("table1_ddl.sql");
    res.close();

    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), "table1"));

    res = executeQuery();
    res.close();

    TableDesc desc = catalog.getTableDesc(getCurrentDatabase(), "table1");
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }
    executeString("DROP TABLE table1 PURGE");
  }

  @Test
  public final void testInsertOverwriteTableWithNonFromQuery() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("InsertOverwriteWithEvalQuery");
    ResultSet res = executeString("create table " + tableName +" (col1 int4, col2 float4, col3 text)");
    res.close();
    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), tableName));

    res = executeString("insert overwrite into " + tableName
        + " select 1::INT4, 2.1::FLOAT4, 'test'; ");

    res.close();

    TableDesc desc = catalog.getTableDesc(getCurrentDatabase(), tableName);
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(1, desc.getStats().getNumRows().intValue());
    }

    res = executeString("select * from " + tableName + ";");
    assertTrue(res.next());

    assertEquals(3, res.getMetaData().getColumnCount());
    assertEquals(1, res.getInt(1));
    assertEquals(2.1f, res.getFloat(2), 10);
    assertEquals("test", res.getString(3));

    res.close();
    executeString("DROP TABLE " + tableName + " PURGE");
  }

  @Test
  public final void testInsertOverwriteTableWithNonFromQuery2() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("InsertOverwriteWithEvalQuery");
    ResultSet res = executeString("create table " + tableName +" (col1 int4, col2 float4, col3 text)");
    res.close();
    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), tableName));

    res = executeString("insert overwrite into " + tableName + " (col1, col3) select 1::INT4, 'test';");
    res.close();

    TableDesc desc = catalog.getTableDesc(getCurrentDatabase(), tableName);
    if (!testingCluster.isHCatalogStoreRunning()) {
      assertEquals(1, desc.getStats().getNumRows().intValue());
    }

    res = executeString("select * from " + tableName + ";");
    assertTrue(res.next());

    assertEquals(3, res.getMetaData().getColumnCount());
    assertEquals(1, res.getInt(1));
    assertEquals("", res.getString(2));
    assertEquals("test", res.getString(3));

    res.close();
    executeString("DROP TABLE " + tableName + " PURGE");
  }

  @Test
  public final void testInsertOverwritePathWithNonFromQuery() throws Exception {
    ResultSet res = executeString("insert overwrite into location " +
        "'/tajo-data/testInsertOverwritePathWithNonFromQuery' " +
        "USING csv WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
        "select 1::INT4, 2.1::FLOAT4, 'test'");

    res.close();
    FileSystem fs = FileSystem.get(testingCluster.getConfiguration());
    Path path = new Path("/tajo-data/testInsertOverwritePathWithNonFromQuery");
    assertTrue(fs.exists(path));
    assertEquals(1, fs.listStatus(path).length);

    CompressionCodecFactory factory = new CompressionCodecFactory(testingCluster.getConfiguration());
    FileStatus file = fs.listStatus(path)[0];
    CompressionCodec codec = factory.getCodec(file.getPath());
    assertTrue(codec instanceof DeflateCodec);

    BufferedReader reader = new BufferedReader(
        new InputStreamReader(codec.createInputStream(fs.open(file.getPath()))));

    try {
      String line = reader.readLine();
      assertNotNull(line);

      String[] tokens = line.split("\\|");

      assertEquals(3, tokens.length);
      assertEquals("1", tokens[0]);
      assertEquals("2.1", tokens[1]);
      assertEquals("test", tokens[2]);
    } finally {
      reader.close();
    }
  }
}
