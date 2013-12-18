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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

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
  private static CatalogStore store;
  private static int port;

  @BeforeClass
  public static void setUp() throws Exception {
    // Set Hive MetaStore
    Database db = new Database();
    db.setName(DB_NAME);

    Path warehousePath = new Path(CommonTestingUtil.getTestDir(), DB_NAME);
    port = MetaStoreUtils.findFreePort();
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());
    db.setLocationUri(warehousePath.toString());

    String metastoreUri = "thrift://localhost:" + port;

    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
    conf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    conf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    conf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    conf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    conf.set(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname, DummyListener.class.getName());

    SessionState.start(new CliSessionState(conf));

    // create database and tables on Hive MetaStore.
    client = new HiveMetaStoreClient(conf, null);

    client.createDatabase(db);
    createTable(NATION);
    createTable(CUSTOMER);

    DummyListener.notifyList.clear();

    // create local HCatalogStore.
    TajoConf tajoConf = new TajoConf();
    tajoConf.set(CatalogConstants.CATALOG_URI, metastoreUri);
    tajoConf.set(CatalogConstants.STORE_CLASS, HCatalogStore.class.getCanonicalName());
    tajoConf.setVar(TajoConf.ConfVars.CATALOG_ADDRESS, "127.0.0.1:0");

    store = new HCatalogStore(tajoConf);
  }

  private static void createTable(String tableName) throws Exception {
    Map<String, String> tableParams = new HashMap<String, String>();

    List<FieldSchema> cols = new ArrayList<FieldSchema>();

    if (tableName.equals(CUSTOMER)) {
      cols.add(new FieldSchema("c_custkey", "int", ""));
      cols.add(new FieldSchema("c_name", "string", ""));
      cols.add(new FieldSchema("c_address", "string", ""));
      cols.add(new FieldSchema("c_nationkey", "int", ""));
      cols.add(new FieldSchema("c_phone", "string", ""));
      cols.add(new FieldSchema("c_acctbal", "double", ""));
      cols.add(new FieldSchema("c_mktsegment", "string", ""));
      cols.add(new FieldSchema("c_comment", "string", ""));
    } else {
      cols.add(new FieldSchema("n_nationkey", "int", ""));
      cols.add(new FieldSchema("n_name", "string", ""));
      cols.add(new FieldSchema("n_regionkey", "int", ""));
      cols.add(new FieldSchema("n_comment", "string", ""));
    }

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setParameters(tableParams);
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tableName);
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().getParameters().put(serdeConstants.FIELD_DELIM, "|");
    sd.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class.getName());
    sd.setOutputFormat(org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat.class.getName());

    Table table = new Table();
    table.setDbName(DB_NAME);
    table.setTableName(tableName);
    table.setParameters(tableParams);
    table.setSd(sd);

    if (tableName.equals(NATION)) {
      table.addToPartitionKeys(new FieldSchema("type", "string", ""));
    }

    client.createTable(table);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    try {
      if (store.existTable(DB_NAME + "." + CUSTOMER))
        store.deleteTable(DB_NAME + "." + CUSTOMER);
      if (store.existTable(DB_NAME + "." + NATION))
        store.deleteTable(DB_NAME + "." + NATION);
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

      Path path = new Path("metastore_db");
      FileSystem fs = FileSystem.getLocal(new Configuration());
      if(fs.exists(path)) {
        fs.delete(path, true);
      }
    } catch (NoSuchObjectException e) {
    } catch (InvalidOperationException e) {
    } catch (Exception e) {
      throw e;
    }
  }

  // Current Hive MetaStoreClient doesn't support to set table location.
  // If Tajo update table path, ThriftHiveMetaStore make a MetaException.
  // So, it needs to disable until Hive support to set table location.
//  @Test
//  public void testAddTable() throws Exception {
//    TableDesc table = new TableDesc();
//
//    table.setName(CUSTOMER);
//
//    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
//    schema.addColumn("c_custkey", TajoDataTypes.Type.INT4);
//    schema.addColumn("c_name", TajoDataTypes.Type.TEXT);
//    schema.addColumn("c_address", TajoDataTypes.Type.TEXT);
//    schema.addColumn("c_nationkey", TajoDataTypes.Type.INT4);
//    schema.addColumn("c_phone", TajoDataTypes.Type.TEXT);
//    schema.addColumn("c_acctbal", TajoDataTypes.Type.FLOAT8);
//    schema.addColumn("c_mktsegment", TajoDataTypes.Type.TEXT);
//    schema.addColumn("c_comment", TajoDataTypes.Type.TEXT);
//
//    table.setSchema(schema);
//    store.addTable(table);
//  }
//  @Test
//  public void testAddTableByPartition() throws Exception {
//    TableDesc table = new TableDesc();
//
//    table.setName(NATION);
//
//    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
//    schema.addColumn("n_nationkey", TajoDataTypes.Type.INT4);
//    schema.addColumn("n_name", TajoDataTypes.Type.TEXT);
//    schema.addColumn("n_regionkey", TajoDataTypes.Type.INT4);
//    schema.addColumn("n_comment", TajoDataTypes.Type.TEXT);
//    table.setSchema(schema);
//
//    Partitions partitions = new Partitions();
//    partitions.addColumn("type", TajoDataTypes.Type.TEXT);
//    table.setPartitions(partitions);
//
//    store.addTable(table);
//  }

  @Test
  public void testExistTable() throws Exception {
    assertTrue(store.existTable(DB_NAME + "." + CUSTOMER));
    assertTrue(store.existTable(DB_NAME + "." + NATION));
  }

  @Test
  public void testGetTable() throws Exception {
    TableDesc table = store.getTable(DB_NAME + "." + CUSTOMER);

    List<Column> columns = table.getSchema().getColumns();
    assertEquals(CUSTOMER, table.getName());
    assertEquals(8, columns.size());
    assertEquals("c_custkey", columns.get(0).getColumnName());
    assertEquals(TajoDataTypes.Type.INT4, columns.get(0).getDataType().getType());
    assertEquals("c_name", columns.get(1).getColumnName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(1).getDataType().getType());
    assertEquals("c_address", columns.get(2).getColumnName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(2).getDataType().getType());
    assertEquals("c_nationkey", columns.get(3).getColumnName());
    assertEquals(TajoDataTypes.Type.INT4, columns.get(3).getDataType().getType());
    assertEquals("c_phone", columns.get(4).getColumnName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(4).getDataType().getType());
    assertEquals("c_acctbal", columns.get(5).getColumnName());
    assertEquals(TajoDataTypes.Type.FLOAT8, columns.get(5).getDataType().getType());
    assertEquals("c_mktsegment", columns.get(6).getColumnName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(6).getDataType().getType());
    assertEquals("c_comment", columns.get(7).getColumnName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(7).getDataType().getType());
    assertNull(table.getPartitions());

    table = store.getTable(DB_NAME + "." + NATION);
    columns = table.getSchema().getColumns();
    assertEquals(NATION, table.getName());
    assertEquals(5, columns.size());
    assertEquals("n_nationkey", columns.get(0).getColumnName());
    assertEquals(TajoDataTypes.Type.INT4, columns.get(0).getDataType().getType());
    assertEquals("n_name", columns.get(1).getColumnName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(1).getDataType().getType());
    assertEquals("n_regionkey", columns.get(2).getColumnName());
    assertEquals(TajoDataTypes.Type.INT4, columns.get(2).getDataType().getType());
    assertEquals("n_comment", columns.get(3).getColumnName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(3).getDataType().getType());
    assertEquals("type", columns.get(4).getColumnName());
    assertEquals(TajoDataTypes.Type.TEXT, columns.get(4).getDataType().getType());
    assertNotNull(table.getPartitions());
    assertEquals("type", table.getPartitions().getColumn(0).getColumnName());
    assertEquals(CatalogProtos.PartitionsType.COLUMN, table.getPartitions().getPartitionsType());
  }

  @Test
  public void testGetAllTableNames() throws Exception{
    Set<String> tables = new HashSet<String>(store.getAllTableNames());
    assertEquals(2, tables.size());
    assertTrue(tables.contains(DB_NAME + "." + CUSTOMER));
    assertTrue(tables.contains(DB_NAME + "." + NATION));
  }

  @Test
  public void testDeleteTable() throws Exception {
    store.deleteTable(DB_NAME + "." + CUSTOMER);
    store.deleteTable(DB_NAME + "." + NATION);
  }
}
