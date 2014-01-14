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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;

import static org.junit.Assert.*;

public class TestTablePartitions {

  private static TpchTestBase tpch;
  public TestTablePartitions() throws IOException {
    super();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = TpchTestBase.getInstance();
  }

  @Test
  public final void testColumnPartitionedTableByOneColumn() throws Exception {
    String tableName ="testColumnPartitionedTableByOneColumn";
    ResultSet res = tpch.execute(
        "create table " + tableName +" (col1 int4, col2 int4, null_col int4) partition by column(key float8) ");
    res.close();
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));

    res = tpch.execute("insert overwrite into " + tableName
        + " (col1, col2, key) select l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();

    TableDesc desc = catalog.getTableDesc(tableName);
    Path path = desc.getPath();

    FileSystem fs = FileSystem.get(tpch.getTestingCluster().getConfiguration());
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=36.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/key=49.0")));
    assertEquals(5, desc.getStats().getNumRows().intValue());

    res = tpch.execute(
        "select distinct * from " + tableName + " where (key = 45.0 or key = 38.0) and null_col is null");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});

    int i = 0;
    while(res.next()) {
      assertEquals(resultRows1.get(res.getDouble(4))[0], res.getInt(1));
      assertEquals(resultRows1.get(res.getDouble(4))[1], res.getInt(2));
      i++;
    }
    res.close();
    assertEquals(2, i);
  }

  @Test
  public final void testColumnPartitionedTableByThreeColumns() throws Exception {
    String tableName ="testColumnPartitionedTableByThreeColumns";
    ResultSet res = tpch.execute(
        "create table " + tableName +" (col4 text) partition by column(col1 int4, col2 int4, col3 float8) ");
    res.close();
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));

    res = tpch.execute("insert overwrite into " + tableName
        + " select l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();

    TableDesc desc = catalog.getTableDesc(tableName);
    Path path = desc.getPath();

    FileSystem fs = FileSystem.get(tpch.getTestingCluster().getConfiguration());
    assertTrue(fs.isDirectory(path));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2/col3=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2/col3=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3/col3=49.0")));
    assertEquals(5, desc.getStats().getNumRows().intValue());

    res = tpch.execute("select * from " + tableName + " where col2 = 2");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});


    int i = 0;
    while(res.next()) {
      assertEquals(resultRows1.get(res.getDouble(4))[0], res.getInt(2));
      assertEquals(resultRows1.get(res.getDouble(4))[1], res.getInt(3));
      i++;
    }
    res.close();
    assertEquals(2, i);


    Map<Double, int []> resultRows2 = Maps.newHashMap();
    resultRows2.put(49.0d, new int[]{3, 3});
    resultRows2.put(45.0d, new int[]{3, 2});
    resultRows2.put(38.0d, new int[]{2, 2});

    res = tpch.execute("select * from " + tableName + " where (col1 = 2 or col1 = 3) and col2 >= 2");

    i = 0;
    while(res.next()) {
      assertEquals(resultRows2.get(res.getDouble(4))[0], res.getInt(2));
      assertEquals(resultRows2.get(res.getDouble(4))[1], res.getInt(3));
      i++;
    }
    res.close();
    assertEquals(3, i);
  }

  @Test
  public final void testColumnPartitionedTableByOneColumnsWithCompression() throws Exception {
    String tableName = "testColumnPartitionedTableByOneColumnsWithCompression";
    ResultSet res = tpch.execute(
        "create table " + tableName + " (col2 int4, col3 float8) USING csv " +
            "WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
            "PARTITION BY column(col1 int4)");
    res.close();
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));

    res = tpch.execute(
        "insert overwrite into " + tableName + " select  l_partkey, l_quantity, l_orderkey from lineitem");
    res.close();
    TableDesc desc = catalog.getTableDesc(tableName);
    assertEquals(5, desc.getStats().getNumRows().intValue());

    FileSystem fs = FileSystem.get(tpch.getTestingCluster().getConfiguration());
    assertTrue(fs.exists(desc.getPath()));
    CompressionCodecFactory factory = new CompressionCodecFactory(tpch.getTestingCluster().getConfiguration());

    Path path = desc.getPath();
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));

    for (FileStatus partition : fs.listStatus(path)){
      assertTrue(fs.isDirectory(partition.getPath()));
      for (FileStatus file : fs.listStatus(partition.getPath())) {
        CompressionCodec codec = factory.getCodec(file.getPath());
        assertTrue(codec instanceof DeflateCodec);
      }
    }
  }

  @Test
  public final void testColumnPartitionedTableByTwoColumnsWithCompression() throws Exception {
    String tableName = "testColumnPartitionedTableByTwoColumnsWithCompression";
    ResultSet res = tpch.execute("create table " + tableName + " (col3 float8, col4 text) USING csv " +
        "WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
        "PARTITION by column(col1 int4, col2 int4)");
    res.close();
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));

    res = tpch.execute(
        "insert overwrite into " + tableName +
            " select  l_quantity, l_returnflag, l_orderkey, l_partkey from lineitem");
    res.close();
    TableDesc desc = catalog.getTableDesc(tableName);
    assertEquals(5, desc.getStats().getNumRows().intValue());

    FileSystem fs = FileSystem.get(tpch.getTestingCluster().getConfiguration());
    assertTrue(fs.exists(desc.getPath()));
    CompressionCodecFactory factory = new CompressionCodecFactory(tpch.getTestingCluster().getConfiguration());

    Path path = desc.getPath();
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));

    for (FileStatus partition1 : fs.listStatus(path)){
      assertTrue(fs.isDirectory(partition1.getPath()));
      for (FileStatus partition2 : fs.listStatus(partition1.getPath())) {
        assertTrue(fs.isDirectory(partition2.getPath()));
        for (FileStatus file : fs.listStatus(partition2.getPath())) {
          CompressionCodec codec = factory.getCodec(file.getPath());
          assertTrue(codec instanceof DeflateCodec);
        }
      }
    }
  }

  @Test
  public final void testColumnPartitionedTableByThreeColumnsWithCompression() throws Exception {
    String tableName = "testColumnPartitionedTableByThreeColumnsWithCompression";
    ResultSet res = tpch.execute(
        "create table " + tableName + " (col4 text) USING csv " +
            "WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
            "partition by column(col1 int4, col2 int4, col3 float8)");
    res.close();
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));

    res = tpch.execute(
        "insert overwrite into " + tableName +
            " select  l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();
    TableDesc desc = catalog.getTableDesc(tableName);
    assertEquals(5, desc.getStats().getNumRows().intValue());

    FileSystem fs = FileSystem.get(tpch.getTestingCluster().getConfiguration());
    assertTrue(fs.exists(desc.getPath()));
    CompressionCodecFactory factory = new CompressionCodecFactory(tpch.getTestingCluster().getConfiguration());

    Path path = desc.getPath();
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2/col3=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2/col3=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3/col3=49.0")));

    for (FileStatus partition1 : fs.listStatus(path)){
      assertTrue(fs.isDirectory(partition1.getPath()));
      for (FileStatus partition2 : fs.listStatus(partition1.getPath())) {
        assertTrue(fs.isDirectory(partition2.getPath()));
        for (FileStatus partition3 : fs.listStatus(partition2.getPath())) {
          assertTrue(fs.isDirectory(partition3.getPath()));
          for (FileStatus file : fs.listStatus(partition3.getPath())) {
            CompressionCodec codec = factory.getCodec(file.getPath());
            assertTrue(codec instanceof DeflateCodec);
          }
        }
      }
    }

    res = tpch.execute("select * from " + tableName + " where col2 = 2");

    Map<Double, int []> resultRows1 = Maps.newHashMap();
    resultRows1.put(45.0d, new int[]{3, 2});
    resultRows1.put(38.0d, new int[]{2, 2});

    int i = 0;
    while (res.next()) {
      assertEquals(resultRows1.get(res.getDouble(4))[0], res.getInt(2));
      assertEquals(resultRows1.get(res.getDouble(4))[1], res.getInt(3));
      i++;
    }
    res.close();
    assertEquals(2, i);

    Map<Double, int []> resultRows2 = Maps.newHashMap();
    resultRows2.put(49.0d, new int[]{3, 3});
    resultRows2.put(45.0d, new int[]{3, 2});
    resultRows2.put(38.0d, new int[]{2, 2});

    res = tpch.execute("select * from " + tableName + " where (col1 = 2 or col1 = 3) and col2 >= 2");
    i = 0;
    while(res.next()) {
      assertEquals(resultRows2.get(res.getDouble(4))[0], res.getInt(2));
      assertEquals(resultRows2.get(res.getDouble(4))[1], res.getInt(3));
      i++;
    }

    res.close();
    assertEquals(3, i);
  }

  @Test
  public final void testColumnPartitionedTableNoMatchedPartition() throws Exception {
    String tableName = "testColumnPartitionedTableNoMatchedPartition";
    ResultSet res = tpch.execute(
        "create table " + tableName + " (col4 text) USING csv " +
            "WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec') " +
            "partition by column(col1 int4, col2 int4, col3 float8)");
    res.close();
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));

    res = tpch.execute(
        "insert overwrite into " + tableName +
            " select  l_returnflag, l_orderkey, l_partkey, l_quantity from lineitem");
    res.close();
    TableDesc desc = catalog.getTableDesc(tableName);
    assertEquals(5, desc.getStats().getNumRows().intValue());

    FileSystem fs = FileSystem.get(tpch.getTestingCluster().getConfiguration());
    assertTrue(fs.exists(desc.getPath()));
    CompressionCodecFactory factory = new CompressionCodecFactory(tpch.getTestingCluster().getConfiguration());

    Path path = desc.getPath();
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=1/col2=1/col3=17.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=2/col2=2/col3=38.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=2/col3=45.0")));
    assertTrue(fs.isDirectory(new Path(path.toUri() + "/col1=3/col2=3/col3=49.0")));

    for (FileStatus partition1 : fs.listStatus(path)){
      assertTrue(fs.isDirectory(partition1.getPath()));
      for (FileStatus partition2 : fs.listStatus(partition1.getPath())) {
        assertTrue(fs.isDirectory(partition2.getPath()));
        for (FileStatus partition3 : fs.listStatus(partition2.getPath())) {
          assertTrue(fs.isDirectory(partition3.getPath()));
          for (FileStatus file : fs.listStatus(partition3.getPath())) {
            CompressionCodec codec = factory.getCodec(file.getPath());
            assertTrue(codec instanceof DeflateCodec);
          }
        }
      }
    }

    res = tpch.execute("select * from " + tableName + " where col2 = 9");
    assertFalse(res.next());
    res.close();
  }
}
