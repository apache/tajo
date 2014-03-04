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

package org.apache.tajo.catalog.store;


import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * TestHCatalogStore. Test case for
 * {@link org.apache.tajo.catalog.store.HCatalogStore}
 */
public class TestHCatalogStore {
  private static HiveMetaStoreClient client;

  private static final String DB_NAME = "test_hive";
  private static final String CUSTOMER = "customer";
  private static final String NATION = "nation";
  private static final String REGION = "region";
  private static final String SUPPLIER = "supplier";

  private static CatalogStore store;
  private static int port;

  private static Path warehousePath;

  @BeforeClass
  public static void setUp() throws Exception {
    // delete metstore default path for successful unit tests
    deleteMetaStoreDirectory();

    // Set Hive MetaStore
    Database db = new Database();
    db.setName(DB_NAME);

    warehousePath = new Path(CommonTestingUtil.getTestDir(), DB_NAME);
    db.setLocationUri(warehousePath.toString());

    HiveConf conf = new HiveConf();
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehousePath.toString());

    // create hive configuration file for unit tests
    Path path = new Path(warehousePath.getParent(), "hive-site.xml");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    conf.writeXml(fs.create(path));

    // create database and tables on Hive MetaStore.
    client = new HiveMetaStoreClient(conf);
    client.createDatabase(db);

    // create local HCatalogStore.
    TajoConf tajoConf = new TajoConf();
    tajoConf.set(CatalogConstants.STORE_CLASS, HCatalogStore.class.getCanonicalName());
    tajoConf.setVar(TajoConf.ConfVars.CATALOG_ADDRESS, "127.0.0.1:0");
    tajoConf.addResource(path.toString());
    tajoConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehousePath.toString());

    store = new HCatalogStore(tajoConf);

  }

  private static void deleteMetaStoreDirectory() throws Exception {
    Path path = new Path("metastore_db");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    if(fs.exists(path)) {
      fs.delete(path, true);
    }
    fs.close();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    try {
      if (store.existTable(DB_NAME + "." + CUSTOMER))
        store.deleteTable(DB_NAME + "." + CUSTOMER);
      if (store.existTable(DB_NAME + "." + NATION))
        store.deleteTable(DB_NAME + "." + NATION);
      if (store.existTable(DB_NAME + "." + REGION))
        store.deleteTable(DB_NAME + "." + REGION);
      if (store.existTable(DB_NAME + "." + SUPPLIER))
        store.deleteTable(DB_NAME + "." + SUPPLIER);
      dropDatabase();
      client.close();
      store.close();
    } catch (Throwable e) {
      throw new IOException("Tajo cannot close Hive Metastore for unit tests.");
    }
  }

  private static void dropDatabase() throws Exception {
    try {
      client.dropDatabase(DB_NAME);
      deleteMetaStoreDirectory();
    } catch (NoSuchObjectException e) {
    } catch (InvalidOperationException e) {
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void testAddTable1() throws Exception {
    TableDesc table = new TableDesc();

    table.setName(DB_NAME + "." + CUSTOMER);

    Options options = new Options();
    options.put(HCatalogStore.CSVFILE_DELIMITER, "\u0001");
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.RCFILE, options);
    table.setMeta(meta);

    table.setPath(new Path(warehousePath, CUSTOMER));

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("c_custkey", TajoDataTypes.Type.INT4);
    schema.addColumn("c_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_address", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_nationkey", TajoDataTypes.Type.INT4);
    schema.addColumn("c_phone", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_acctbal", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("c_mktsegment", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_comment", TajoDataTypes.Type.TEXT);

    table.setSchema(schema);
    store.addTable(table.getProto());
  }

  @Test
  public void testAddTable2() throws Exception {
    TableDesc table = new TableDesc();

    table.setName(DB_NAME + "." + REGION);

    Options options = new Options();
    options.put(HCatalogStore.CSVFILE_DELIMITER, "|");
    options.put(HCatalogStore.CSVFILE_NULL, "\t");
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.CSV, options);
    table.setMeta(meta);

    table.setPath(new Path(warehousePath, REGION));

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("r_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("r_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("r_comment", TajoDataTypes.Type.TEXT);

    table.setSchema(schema);
    store.addTable(table.getProto());
  }

  @Test
  public void testAddTable3() throws Exception {
    TableDesc table = new TableDesc();

    table.setName(DB_NAME + "." + SUPPLIER);

    Options options = new Options();
    options.put(HCatalogStore.CSVFILE_DELIMITER, "\t");
    options.put(HCatalogStore.CSVFILE_NULL, "\u0002");
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.CSV, options);
    table.setMeta(meta);

    table.setPath(new Path(warehousePath, SUPPLIER));

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("s_suppkey", TajoDataTypes.Type.INT4);
    schema.addColumn("s_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("s_address", TajoDataTypes.Type.TEXT);
    schema.addColumn("s_nationkey", TajoDataTypes.Type.INT4);
    schema.addColumn("s_phone", TajoDataTypes.Type.TEXT);
    schema.addColumn("s_acctbal", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("s_comment", TajoDataTypes.Type.TEXT);

    table.setSchema(schema);
    store.addTable(table.getProto());
  }

  @Test
  public void testAddTableByPartition() throws Exception {
    TableDesc table = new TableDesc();

    table.setName(DB_NAME + "." + NATION);

    Options options = new Options();
    options.put(HCatalogStore.CSVFILE_DELIMITER, "\u0001");
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.CSV, options);
    table.setMeta(meta);

    table.setPath(new Path(warehousePath, NATION));

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("n_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("n_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("n_comment", TajoDataTypes.Type.TEXT);
    table.setSchema(schema);

    org.apache.tajo.catalog.Schema expressionSchema = new org.apache.tajo.catalog.Schema();
    expressionSchema.addColumn("n_nationkey", TajoDataTypes.Type.INT4);

    PartitionMethodDesc partitions = new PartitionMethodDesc(DB_NAME + "." + NATION,
        CatalogProtos.PartitionType.COLUMN, expressionSchema.getColumn(0).getQualifiedName(), expressionSchema);
    table.setPartitionMethod(partitions);

    store.addTable(table.getProto());
  }

  @Test
  public void testExistTable() throws Exception {
    assertTrue(store.existTable(DB_NAME + "." + CUSTOMER));
    assertTrue(store.existTable(DB_NAME + "." + NATION));
    assertTrue(store.existTable(DB_NAME + "." + REGION));
    assertTrue(store.existTable(DB_NAME + "." + SUPPLIER));
  }

  @Test
  public void testGetTable1() throws Exception {
    TableDesc table = new TableDesc(store.getTable(DB_NAME + "." + CUSTOMER));

    List<Column> columns = table.getSchema().getColumns();
    assertEquals(DB_NAME + "." + CUSTOMER, table.getName());
    assertEquals(8, columns.size());
    assertEquals("c_custkey", columns.get(0).getSimpleName());
    assertEquals(TajoDataTypes.Type.INT4, columns.get(0).getDataType().getType());
    assertEquals("c_name", columns.get(1).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(1).getDataType().getType());
    assertEquals("c_address", columns.get(2).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(2).getDataType().getType());
    assertEquals("c_nationkey", columns.get(3).getSimpleName());
    assertEquals(TajoDataTypes.Type.INT4, columns.get(3).getDataType().getType());
    assertEquals("c_phone", columns.get(4).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(4).getDataType().getType());
    assertEquals("c_acctbal", columns.get(5).getSimpleName());
    assertEquals(TajoDataTypes.Type.FLOAT8, columns.get(5).getDataType().getType());
    assertEquals("c_mktsegment", columns.get(6).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(6).getDataType().getType());
    assertEquals("c_comment", columns.get(7).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(7).getDataType().getType());

    assertNull(table.getPartitionMethod());

    assertEquals(table.getMeta().getStoreType().name(), CatalogProtos.StoreType.RCFILE.name());
  }

  @Test
  public void testGetTable2() throws Exception {
    TableDesc table = new TableDesc(store.getTable(DB_NAME + "." + NATION));

    List<Column> columns = table.getSchema().getColumns();
    assertEquals(DB_NAME + "." + NATION, table.getName());
    assertEquals(3, columns.size());
    assertEquals("n_name", columns.get(0).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(0).getDataType().getType());
    assertEquals("n_regionkey", columns.get(1).getSimpleName());
    assertEquals(TajoDataTypes.Type.INT4, columns.get(1).getDataType().getType());
    assertEquals("n_comment", columns.get(2).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(2).getDataType().getType());

    assertNotNull(table.getPartitionMethod());

    assertEquals("n_nationkey", table.getPartitionMethod().getExpressionSchema().getColumn(0).getSimpleName());
    assertEquals(CatalogProtos.PartitionType.COLUMN, table.getPartitionMethod().getPartitionType());

    assertEquals(table.getMeta().getStoreType().name(), CatalogProtos.StoreType.CSV.name());
    assertEquals(table.getMeta().getOption(HCatalogStore.CSVFILE_DELIMITER), StringEscapeUtils.escapeJava("\u0001"));
    assertEquals(table.getMeta().getOption(HCatalogStore.CSVFILE_NULL), StringEscapeUtils.escapeJava("\\N"));
  }

  @Test
  public void testGetTable3() throws Exception {
    TableDesc table = new TableDesc(store.getTable(DB_NAME + "." + REGION));

    List<Column> columns = table.getSchema().getColumns();
    assertEquals(DB_NAME + "." + REGION, table.getName());
    assertEquals(3, columns.size());
    assertEquals("r_regionkey", columns.get(0).getSimpleName());
    assertEquals(TajoDataTypes.Type.INT4, columns.get(0).getDataType().getType());
    assertEquals("r_name", columns.get(1).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(1).getDataType().getType());
    assertEquals("r_comment", columns.get(2).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(2).getDataType().getType());

    assertNull(table.getPartitionMethod());

    assertEquals(table.getMeta().getStoreType().name(), CatalogProtos.StoreType.CSV.name());
    assertEquals(table.getMeta().getOption(HCatalogStore.CSVFILE_DELIMITER), StringEscapeUtils.escapeJava("|"));
    assertEquals(table.getMeta().getOption(HCatalogStore.CSVFILE_NULL), StringEscapeUtils.escapeJava("\t"));
  }

  @Test
  public void testGetTable4() throws Exception {
    TableDesc table = new TableDesc(store.getTable(DB_NAME + "." + SUPPLIER));

    List<Column> columns = table.getSchema().getColumns();
    assertEquals(DB_NAME + "." + SUPPLIER, table.getName());
    assertEquals(7, columns.size());
    assertEquals("s_suppkey", columns.get(0).getSimpleName());
    assertEquals(TajoDataTypes.Type.INT4, columns.get(0).getDataType().getType());
    assertEquals("s_name", columns.get(1).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(1).getDataType().getType());
    assertEquals("s_address", columns.get(2).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(2).getDataType().getType());
    assertEquals("s_nationkey", columns.get(3).getSimpleName());
    assertEquals(TajoDataTypes.Type.INT4, columns.get(3).getDataType().getType());
    assertEquals("s_phone", columns.get(4).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(4).getDataType().getType());
    assertEquals("s_acctbal", columns.get(5).getSimpleName());
    assertEquals(TajoDataTypes.Type.FLOAT8, columns.get(5).getDataType().getType());
    assertEquals("s_comment", columns.get(6).getSimpleName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(6).getDataType().getType());

    assertNull(table.getPartitionMethod());

    assertEquals(table.getMeta().getStoreType().name(), CatalogProtos.StoreType.CSV.name());
    assertEquals(table.getMeta().getOption(HCatalogStore.CSVFILE_DELIMITER), StringEscapeUtils.escapeJava("\t"));
    assertEquals(table.getMeta().getOption(HCatalogStore.CSVFILE_NULL), StringEscapeUtils.escapeJava("\u0002"));
  }


  @Test
  public void testGetAllTableNames() throws Exception{
    Set<String> tables = new HashSet<String>(store.getAllTableNames());
    assertEquals(4, tables.size());
    assertTrue(tables.contains(DB_NAME + "." + CUSTOMER));
    assertTrue(tables.contains(DB_NAME + "." + NATION));
    assertTrue(tables.contains(DB_NAME + "." + REGION));
    assertTrue(tables.contains(DB_NAME + "." + SUPPLIER));
  }

  @Test
  public void testDeleteTable() throws Exception {
    TableDesc table = new TableDesc(store.getTable(DB_NAME + "." + CUSTOMER));
    Path customerPath = table.getPath();

    table = new TableDesc(store.getTable(DB_NAME + "." + NATION));
    Path nationPath = table.getPath();

    table = new TableDesc(store.getTable(DB_NAME + "." + REGION));
    Path regionPath = table.getPath();

    table = new TableDesc(store.getTable(DB_NAME + "." + SUPPLIER));
    Path supplierPath = table.getPath();

    store.deleteTable(DB_NAME + "." + CUSTOMER);
    store.deleteTable(DB_NAME + "." + NATION);
    store.deleteTable(DB_NAME + "." + REGION);
    store.deleteTable(DB_NAME + "." + SUPPLIER);

    FileSystem fs = FileSystem.getLocal(new Configuration());
    assertTrue(fs.exists(customerPath));
    assertTrue(fs.exists(nationPath));
    assertTrue(fs.exists(regionPath));
    assertTrue(fs.exists(supplierPath));
    fs.close();
  }
}
