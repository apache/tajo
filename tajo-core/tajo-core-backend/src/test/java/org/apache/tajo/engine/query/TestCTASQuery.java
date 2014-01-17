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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Test CREATE TABLE AS SELECT statements
 */
@Category(IntegrationTest.class)
public class TestCTASQuery {
  private static TpchTestBase tpch;
  public TestCTASQuery() throws IOException {
    super();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = TpchTestBase.getInstance();
  }

  @Test
  public final void testCtasWithoutTableDefinition() throws Exception {
    String tableName ="testCtasWithoutTableDefinition";
    tpch.execute(
        "create table " + tableName
            + " partition by column(key float8) "
            + " as select l_orderkey as col1, l_partkey as col2, l_quantity as key from lineitem");

    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    TableDesc desc = catalog.getTableDesc(tableName);
    assertTrue(catalog.existsTable(tableName));
    assertTrue(desc.getSchema().containsByQualifiedName("testCtasWithoutTableDefinition.col1"));
    PartitionDesc partitionDesc = desc.getPartitions();
    assertEquals(partitionDesc.getPartitionsType(), CatalogProtos.PartitionsType.COLUMN);
    assertEquals("key", partitionDesc.getColumns().get(0).getColumnName());

    FileSystem fs = FileSystem.get(tpch.getTestingCluster().getConfiguration());
    Path path = desc.getPath();
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=36.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=49.0")));
    assertEquals(5, desc.getStats().getNumRows().intValue());

    ResultSet res = tpch.execute(
        "select distinct * from " + tableName + " where (key = 45.0 or key = 38.0)");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});

    int i = 0;
    while(res.next()) {
      assertEquals(resultRows1.get(res.getDouble(3))[0], res.getInt(1));
      assertEquals(resultRows1.get(res.getDouble(3))[1], res.getInt(2));
      i++;
    }
    res.close();
    assertEquals(2, i);
  }

  //@Test
  // TODO- to be enabled
  public final void testCtasWithColumnedPartition() throws Exception {
    String tableName ="testCtasWithColumnedPartition";
    tpch.execute(
        "create table " + tableName
            + " (col1 int4, col2 int4) partition by column(key float8) "
            + " as select l_orderkey as col1, l_partkey as col2, l_quantity as key from lineitem");

    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    TableDesc desc = catalog.getTableDesc(tableName);
    assertTrue(catalog.existsTable(tableName));
    PartitionDesc partitionDesc = desc.getPartitions();
    assertEquals(partitionDesc.getPartitionsType(), CatalogProtos.PartitionsType.COLUMN);
    assertEquals("key", partitionDesc.getColumns().get(0).getColumnName());

    FileSystem fs = FileSystem.get(tpch.getTestingCluster().getConfiguration());
    Path path = desc.getPath();
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=36.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=49.0")));
    assertEquals(5, desc.getStats().getNumRows().intValue());

    ResultSet res = tpch.execute(
        "select distinct * from " + tableName + " where (key = 45.0 or key = 38.0)");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});

    int i = 0;
    while(res.next()) {
      assertEquals(resultRows1.get(res.getDouble(3))[0], res.getInt(1));
      assertEquals(resultRows1.get(res.getDouble(3))[1], res.getInt(2));
      i++;
    }
    res.close();
    assertEquals(2, i);
  }

}
