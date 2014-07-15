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
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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
  public final void testInsertInto() throws Exception {
    // create table and upload test data
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

    res = executeFile("testInsertInto.sql");
    res.close();

    List<Path> dataFiles = listTableFiles("table1");
    assertEquals(2, dataFiles.size());

    for (int i = 0; i < dataFiles.size(); i++) {
      String name = dataFiles.get(i).getName();
      assertTrue(name.matches("part-[0-9]*-[0-9]*-[0-9]*"));
      String[] tokens = name.split("-");
      assertEquals(4, tokens.length);
      assertEquals(i, Integer.parseInt(tokens[3]));
    }

    String tableDatas = getTableFileContents("table1");

    String expected = "1|1|17.0\n" +
        "1|1|36.0\n" +
        "2|2|38.0\n" +
        "3|2|45.0\n" +
        "3|3|49.0\n" +
        "1|1|17.0\n" +
        "1|1|36.0\n" +
        "2|2|38.0\n" +
        "3|2|45.0\n" +
        "3|3|49.0\n";

    assertNotNull(tableDatas);
    assertEquals(expected, tableDatas);

    executeString("DROP TABLE table1 PURGE");
  }

  @Test
  public final void testInsertIntoLocation() throws Exception {
    FileSystem fs = null;
    Path path = new Path("/tajo-data/testInsertIntoLocation");
    try {
      executeString("insert into location '" + path + "' select l_orderkey, l_partkey, l_linenumber from default.lineitem").close();

      String resultFileData = getTableFileContents(path);
      String expected = "1|1|1\n" +
          "1|1|2\n" +
          "2|2|1\n" +
          "3|2|1\n" +
          "3|3|2\n";

      assertEquals(expected, resultFileData);

      fs = path.getFileSystem(testingCluster.getConfiguration());

      FileStatus[] files = fs.listStatus(path);
      assertNotNull(files);
      assertEquals(1, files.length);

      for (FileStatus eachFileStatus : files) {
        String name = eachFileStatus.getPath().getName();
        assertTrue(name.matches("part-[0-9]*-[0-9]*-[0-9]*"));
      }

      executeString("insert into location '" + path + "' select l_orderkey, l_partkey, l_linenumber from default.lineitem").close();
      resultFileData = getTableFileContents(path);
      expected = "1|1|1\n" +
          "1|1|2\n" +
          "2|2|1\n" +
          "3|2|1\n" +
          "3|3|2\n";

      assertEquals(expected + expected, resultFileData);

      files = fs.listStatus(path);
      assertNotNull(files);
      assertEquals(2, files.length);

      for (FileStatus eachFileStatus : files) {
        String name = eachFileStatus.getPath().getName();
        assertTrue(name.matches("part-[0-9]*-[0-9]*-[0-9]*"));
      }
    } finally {
      if (fs != null) {
        fs.delete(path, true);
      }
    }
  }

  @Test
  public final void testInsertIntoPartitionedTable() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testInsertIntoPartitionedTable");
    executeString("create table " + tableName + " (n_name TEXT, n_regionkey INT4)" +
        "USING csv PARTITION by column(n_nationkey INT4)" ).close();

    try {
      executeString("insert into " + tableName + " select n_name, n_regionkey, n_nationkey from default.nation").close();

      ResultSet res = executeString("select * from " + tableName);

      String expected = "n_name,n_regionkey,n_nationkey\n" +
          "-------------------------------\n" +
          "ALGERIA,0,0\n" +
          "ARGENTINA,1,1\n" +
          "IRAN,4,10\n" +
          "IRAQ,4,11\n" +
          "JAPAN,2,12\n" +
          "JORDAN,4,13\n" +
          "KENYA,0,14\n" +
          "MOROCCO,0,15\n" +
          "MOZAMBIQUE,0,16\n" +
          "PERU,1,17\n" +
          "CHINA,2,18\n" +
          "ROMANIA,3,19\n" +
          "BRAZIL,1,2\n" +
          "SAUDI ARABIA,4,20\n" +
          "VIETNAM,2,21\n" +
          "RUSSIA,3,22\n" +
          "UNITED KINGDOM,3,23\n" +
          "UNITED STATES,1,24\n" +
          "CANADA,1,3\n" +
          "EGYPT,4,4\n" +
          "ETHIOPIA,0,5\n" +
          "FRANCE,3,6\n" +
          "GERMANY,3,7\n" +
          "INDIA,2,8\n" +
          "INDONESIA,2,9\n";

      assertEquals(expected, resultSetToString(res));
      res.close();

      executeString("insert into " + tableName + " select n_name, n_regionkey, n_nationkey from default.nation").close();
      res = executeString("select * from " + tableName);
      expected = "n_name,n_regionkey,n_nationkey\n" +
          "-------------------------------\n" +
          "ALGERIA,0,0\n" +
          "ALGERIA,0,0\n" +
          "ARGENTINA,1,1\n" +
          "ARGENTINA,1,1\n" +
          "IRAN,4,10\n" +
          "IRAN,4,10\n" +
          "IRAQ,4,11\n" +
          "IRAQ,4,11\n" +
          "JAPAN,2,12\n" +
          "JAPAN,2,12\n" +
          "JORDAN,4,13\n" +
          "JORDAN,4,13\n" +
          "KENYA,0,14\n" +
          "KENYA,0,14\n" +
          "MOROCCO,0,15\n" +
          "MOROCCO,0,15\n" +
          "MOZAMBIQUE,0,16\n" +
          "MOZAMBIQUE,0,16\n" +
          "PERU,1,17\n" +
          "PERU,1,17\n" +
          "CHINA,2,18\n" +
          "CHINA,2,18\n" +
          "ROMANIA,3,19\n" +
          "ROMANIA,3,19\n" +
          "BRAZIL,1,2\n" +
          "BRAZIL,1,2\n" +
          "SAUDI ARABIA,4,20\n" +
          "SAUDI ARABIA,4,20\n" +
          "VIETNAM,2,21\n" +
          "VIETNAM,2,21\n" +
          "RUSSIA,3,22\n" +
          "RUSSIA,3,22\n" +
          "UNITED KINGDOM,3,23\n" +
          "UNITED KINGDOM,3,23\n" +
          "UNITED STATES,1,24\n" +
          "UNITED STATES,1,24\n" +
          "CANADA,1,3\n" +
          "CANADA,1,3\n" +
          "EGYPT,4,4\n" +
          "EGYPT,4,4\n" +
          "ETHIOPIA,0,5\n" +
          "ETHIOPIA,0,5\n" +
          "FRANCE,3,6\n" +
          "FRANCE,3,6\n" +
          "GERMANY,3,7\n" +
          "GERMANY,3,7\n" +
          "INDIA,2,8\n" +
          "INDIA,2,8\n" +
          "INDONESIA,2,9\n" +
          "INDONESIA,2,9\n";

      assertEquals(expected, resultSetToString(res));

      TableDesc tableDesc = testingCluster.getMaster().getCatalog().getTableDesc(getCurrentDatabase(), tableName);
      assertNotNull(tableDesc);

      Path path = tableDesc.getPath();
      FileSystem fs = path.getFileSystem(testingCluster.getConfiguration());

      FileStatus[] files = fs.listStatus(path);
      assertNotNull(files);
      assertEquals(25, files.length);

      for (FileStatus eachFileStatus: files) {
        assertTrue(eachFileStatus.getPath().getName().indexOf("n_nationkey=") == 0);
        FileStatus[] dataFiles = fs.listStatus(eachFileStatus.getPath());
        assertEquals(2, dataFiles.length);
        for (FileStatus eachDataFileStatus: dataFiles) {
          String name = eachDataFileStatus.getPath().getName();
          assertTrue(name.matches("part-[0-9]*-[0-9]*-[0-9]*"));
        }
      }
    } finally {
      executeString("DROP TABLE " + tableName + " PURGE");
    }
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
    assertNull(res.getString(2));
    assertEquals(0.0, res.getDouble(2), 10);
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

  @Test
  public final void testInsertOverwriteWithUnion() throws Exception {
    ResultSet res = executeFile("table1_ddl.sql");
    res.close();

    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), "table1"));

    res = executeFile("testInsertOverwriteWithUnion.sql");
    res.close();

    String tableDatas = getTableFileContents("table1");

    String expected = "1|1|17.0\n" +
        "1|1|36.0\n" +
        "2|2|38.0\n" +
        "3|2|45.0\n" +
        "3|3|49.0\n" +
        "1|3|173665.47\n" +
        "2|4|46929.18\n" +
        "3|2|193846.25\n";

    assertNotNull(tableDatas);
    assertEquals(expected, tableDatas);

    executeString("DROP TABLE table1 PURGE");
  }

  @Test
  public final void testInsertOverwriteWithUnionDifferentAlias() throws Exception {
    ResultSet res = executeFile("table1_ddl.sql");
    res.close();

    CatalogService catalog = testingCluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(getCurrentDatabase(), "table1"));

    res = executeFile("testInsertOverwriteWithUnionDifferentAlias.sql");
    res.close();

    String tableDatas = getTableFileContents("table1");

    String expected = "1|1|17.0\n" +
        "1|1|36.0\n" +
        "2|2|38.0\n" +
        "3|2|45.0\n" +
        "3|3|49.0\n" +
        "1|3|173665.47\n" +
        "2|4|46929.18\n" +
        "3|2|193846.25\n";

    assertNotNull(tableDatas);
    assertEquals(expected, tableDatas);

    executeString("DROP TABLE table1 PURGE");
  }

  @Test
  public final void testInsertOverwriteLocationWithUnion() throws Exception {
    ResultSet res = executeFile("testInsertOverwriteLocationWithUnion.sql");
    res.close();

    String resultDatas= getTableFileContents(new Path("/tajo-data/testInsertOverwriteLocationWithUnion"));

    String expected = "1|1|17.0\n" +
        "1|1|36.0\n" +
        "2|2|38.0\n" +
        "3|2|45.0\n" +
        "3|3|49.0\n" +
        "1|3|173665.47\n" +
        "2|4|46929.18\n" +
        "3|2|193846.25\n";

    assertNotNull(resultDatas);
    assertEquals(expected, resultDatas);
  }

  @Test
  public final void testInsertOverwriteLocationWithUnionDifferenceAlias() throws Exception {
    ResultSet res = executeFile("testInsertOverwriteLocationWithUnionDifferenceAlias.sql");
    res.close();

    String resultDatas= getTableFileContents(new Path("/tajo-data/testInsertOverwriteLocationWithUnionDifferenceAlias"));

    String expected = "1|1|17.0\n" +
        "1|1|36.0\n" +
        "2|2|38.0\n" +
        "3|2|45.0\n" +
        "3|3|49.0\n" +
        "1|3|173665.47\n" +
        "2|4|46929.18\n" +
        "3|2|193846.25\n";

    assertNotNull(resultDatas);
    assertEquals(expected, resultDatas);
  }
}
