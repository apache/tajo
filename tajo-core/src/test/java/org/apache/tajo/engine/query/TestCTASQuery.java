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

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.util.Map;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.catalog.CatalogUtil.buildFQName;
import static org.junit.Assert.*;


/**
 * Test CREATE TABLE AS SELECT statements
 */
@Category(IntegrationTest.class)
public class TestCTASQuery extends QueryTestCaseBase {

  public TestCTASQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testCtasWithoutTableDefinition() throws Exception {
    ResultSet res = executeQuery();
    res.close();

    String tableName = CatalogUtil.normalizeIdentifier("testCtasWithoutTableDefinition");
    CatalogService catalog = testBase.getTestingCluster().getMaster().getCatalog();
    String qualifiedTableName = buildFQName(DEFAULT_DATABASE_NAME, tableName);
    TableDesc desc = catalog.getTableDesc(qualifiedTableName);
    assertTrue(catalog.existsTable(qualifiedTableName));

    assertTrue(desc.getSchema().contains("default.testctaswithouttabledefinition.col1"));
    PartitionMethodDesc partitionDesc = desc.getPartitionMethod();
    assertEquals(partitionDesc.getPartitionType(), CatalogProtos.PartitionType.COLUMN);
    assertEquals("key", partitionDesc.getExpressionSchema().getRootColumns().get(0).getSimpleName());

    FileSystem fs = FileSystem.get(testBase.getTestingCluster().getConfiguration());
    Path path = new Path(desc.getPath());
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=36.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=49.0")));
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    ResultSet res2 = executeFile("check1.sql");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});

    int i = 0;
    while(res2.next()) {
      assertEquals(resultRows1.get(res2.getDouble(3))[0], res2.getInt(1));
      assertEquals(resultRows1.get(res2.getDouble(3))[1], res2.getInt(2));
      i++;
    }
    res2.close();
    assertEquals(2, i);
  }

  @Test
  public final void testCtasWithColumnedPartition() throws Exception {
    ResultSet res = executeQuery();
    res.close();

    String tableName = CatalogUtil.normalizeIdentifier("testCtasWithColumnedPartition");

    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    TableDesc desc = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
    PartitionMethodDesc partitionDesc = desc.getPartitionMethod();
    assertEquals(partitionDesc.getPartitionType(), CatalogProtos.PartitionType.COLUMN);
    assertEquals("key", partitionDesc.getExpressionSchema().getRootColumns().get(0).getSimpleName());

    FileSystem fs = FileSystem.get(cluster.getConfiguration());
    Path path = new Path(desc.getPath());
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=36.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=49.0")));
    if (!cluster.isHiveCatalogStoreRunning()) {
      assertEquals(5, desc.getStats().getNumRows().intValue());
    }

    ResultSet res2 = executeFile("check2.sql");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});

    int i = 0;
    while(res2.next()) {
      assertEquals(resultRows1.get(res2.getDouble(3))[0], res2.getInt(1));
      assertEquals(resultRows1.get(res2.getDouble(3))[1], res2.getInt(2));
      i++;
    }
    res2.close();
    assertEquals(2, i);
  }

  @Test
  public final void testCtasWithGroupby() throws Exception {
    ResultSet res = executeFile("CtasWithGroupby.sql");
    res.close();

    ResultSet res2 = executeQuery();
    assertResultSet(res2);
    res2.close();
  }

  @Test
  public final void testCtasWithOrderby() throws Exception {
    ResultSet res = executeFile("CtasWithOrderby.sql");
    res.close();

    ResultSet res2 = executeQuery();
    assertResultSet(res2);
    res2.close();
  }

  @Test
  public final void testCtasWithLimit() throws Exception {
    ResultSet res = executeFile("CtasWithLimit.sql");
    res.close();

    ResultSet res2 = executeQuery();
    assertResultSet(res2);
    res2.close();
  }

  @Test
  public final void testCtasWithUnion() throws Exception {
    ResultSet res = executeFile("CtasWithUnion.sql");
    res.close();

    ResultSet res2 = executeQuery();
    resultSetToString(res2);
    res2.close();
  }

  @Test
  public final void testCtasWithMultipleUnions() throws Exception {
    ResultSet res = executeFile("CtasWithMultipleUnions.sql");
    res.close();

    ResultSet res2 = executeQuery();
    String actual = resultSetToString(res2);
    res2.close();

    String expected = "c_custkey,c_nationkey\n" +
        "-------------------------------\n" +
        "1,15\n" +
        "2,13\n" +
        "3,1\n" +
        "4,4\n" +
        "5,3\n" +
        "1,15\n" +
        "2,13\n" +
        "3,1\n" +
        "4,4\n" +
        "5,3\n";

    assertEquals(expected, actual);

    TableDesc desc = client.getTableDesc(CatalogUtil.normalizeIdentifier(res2.getMetaData().getTableName(1)));
    assertNotNull(desc);
  }

  @Test
  public final void testCtasWithStoreType() throws Exception {
    ResultSet res = executeFile("CtasWithStoreType.sql");
    res.close();

    ResultSet res2 = executeQuery();
    resultSetToString(res2);
    res2.close();

    TableDesc desc =  client.getTableDesc(CatalogUtil.normalizeIdentifier(res2.getMetaData().getTableName(1)));
    assertNotNull(desc);
    assertTrue("RCFILE".equalsIgnoreCase(desc.getMeta().getStoreType()));
  }

  @Test
  public final void testCtasWithTextFile() throws Exception {
    ResultSet res = executeFile("CtasWithTextFile.sql");
    res.close();

    ResultSet res2 = executeQuery();
    resultSetToString(res2);
    res2.close();

    TableDesc desc =  client.getTableDesc(CatalogUtil.normalizeIdentifier(res2.getMetaData().getTableName(1)));
    assertNotNull(desc);
    assertTrue("TEXT".equalsIgnoreCase(desc.getMeta().getStoreType()));
  }

  @Test
  public final void testCtasWithOptions() throws Exception {
    ResultSet res = executeFile("CtasWithOptions.sql");
    res.close();

    ResultSet res2 = executeQuery();
    resultSetToString(res2);
    res2.close();

    TableDesc desc =  client.getTableDesc(CatalogUtil.normalizeIdentifier(res2.getMetaData().getTableName(1)));
    assertNotNull(desc);
    assertTrue("CSV".equalsIgnoreCase(desc.getMeta().getStoreType()));


    KeyValueSet options = desc.getMeta().getOptions();
    assertNotNull(options);
    assertEquals(StringEscapeUtils.escapeJava("\u0001"), options.get(StorageConstants.TEXT_DELIMITER));
  }

  @Test
  public final void testCtasWithManagedTable() throws Exception {
    ResultSet res = executeFile("CtasWithManagedTable.sql");
    res.close();

    if (testingCluster.isHiveCatalogStoreRunning()) {
      assertTrue(client.existTable("managed_table1"));

      TableDesc desc =  client.getTableDesc("managed_table1");

      assertNotNull(desc);
      assertEquals("managed_table1", new Path(desc.getPath()).getName());
    } else {
      assertFalse(client.existTable("managed_Table1"));
      assertTrue(client.existTable("MANAGED_TABLE1"));

      TableDesc desc =  client.getTableDesc("MANAGED_TABLE1");

      assertNotNull(desc);
      assertEquals("MANAGED_TABLE1", new Path(desc.getPath()).getName());
    }
  }
}
