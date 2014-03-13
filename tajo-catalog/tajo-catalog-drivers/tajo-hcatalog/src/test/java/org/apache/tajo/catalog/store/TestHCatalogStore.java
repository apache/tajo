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
import org.apache.hadoop.hive.metastore.api.Database;
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
import java.util.List;

import static org.junit.Assert.*;

/**
 * TestHCatalogStore. Test case for
 * {@link org.apache.tajo.catalog.store.HCatalogStore}
 */

public class TestHCatalogStore {
  private static final String DB_NAME = "test_hive";
  private static final String CUSTOMER = "customer";
  private static final String NATION = "nation";
  private static final String REGION = "region";
  private static final String SUPPLIER = "supplier";

  private static HCatalogStore store;
  private static Path warehousePath;
  private static HCatalogStoreClientPool pool;

  @BeforeClass
  public static void setUp() throws Exception {
    Path testPath = CommonTestingUtil.getTestDir();
    warehousePath = new Path(testPath, DB_NAME);

    //create local hiveMeta
    HiveConf conf = new HiveConf();
    String jdbcUri = "jdbc:derby:;databaseName="+testPath.toUri().getPath()+"/metastore_db;create=true";
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehousePath.toUri().toString());
    conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, jdbcUri);

    // create local HCatalogStore.
    TajoConf tajoConf = new TajoConf(conf);
    Database db = new Database();
    db.setLocationUri(warehousePath.toUri().toString());
    db.setName(DB_NAME);
    pool = new HCatalogStoreClientPool(1, tajoConf);
    HCatalogStoreClientPool.HCatalogStoreClient client = pool.getClient();
    client.getHiveClient().createDatabase(db);
    client.release();

    store = new HCatalogStore(tajoConf, pool);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    try {
      HCatalogStoreClientPool.HCatalogStoreClient client = pool.getClient();
      client.getHiveClient().dropDatabase(DB_NAME);
      client.release();
    } catch (Exception e) {
      e.printStackTrace();
    }
    store.close();
  }

  @Test
  public void testTableUsingTextFile() throws Exception {
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.CSV, new Options());

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("c_custkey", TajoDataTypes.Type.INT4);
    schema.addColumn("c_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_address", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_nationkey", TajoDataTypes.Type.INT4);
    schema.addColumn("c_phone", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_acctbal", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("c_mktsegment", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_comment", TajoDataTypes.Type.TEXT);

    String tableName = DB_NAME + "." + CUSTOMER;
    TableDesc table = new TableDesc(tableName, schema, meta, warehousePath);
    store.addTable(table.getProto());
    assertTrue(store.existTable(tableName));

    TableDesc table1 = new TableDesc(store.getTable(table.getName()));
    assertEquals(table.getName(), table1.getName());
    assertEquals(new Path(table.getPath(), CUSTOMER), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StringEscapeUtils.escapeJava(CatalogConstants.CSVFILE_DELIMITER_DEFAULT),
        table1.getMeta().getOption(CatalogConstants.CSVFILE_DELIMITER));
    store.deleteTable(tableName);
  }

  @Test
  public void testTableUsingRCFileWithBinarySerde() throws Exception {
    Options options = new Options();
    options.put(CatalogConstants.RCFILE_SERDE, CatalogConstants.RCFILE_BINARY_SERDE);
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.RCFILE, options);

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("r_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("r_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("r_comment", TajoDataTypes.Type.TEXT);

    String tableName = DB_NAME + "." + REGION;
    TableDesc table = new TableDesc(tableName, schema, meta, warehousePath);
    store.addTable(table.getProto());
    assertTrue(store.existTable(tableName));

    TableDesc table1 = new TableDesc(store.getTable(table.getName()));
    assertEquals(table.getName(), table1.getName());
    assertEquals(new Path(table.getPath(), REGION), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(CatalogConstants.RCFILE_BINARY_SERDE,
        table1.getMeta().getOption(CatalogConstants.RCFILE_SERDE));
    store.deleteTable(tableName);
  }

  @Test
  public void testTableUsingRCFileWithTextSerde() throws Exception {
    Options options = new Options();
    options.put(CatalogConstants.RCFILE_SERDE, CatalogConstants.RCFILE_TEXT_SERDE);
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.RCFILE, options);

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("r_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("r_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("r_comment", TajoDataTypes.Type.TEXT);

    String tableName = DB_NAME + "." + REGION;
    TableDesc table = new TableDesc(tableName, schema, meta, warehousePath);
    store.addTable(table.getProto());
    assertTrue(store.existTable(tableName));

    TableDesc table1 = new TableDesc(store.getTable(table.getName()));
    assertEquals(table.getName(), table1.getName());
    assertEquals(new Path(table.getPath(), REGION), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(CatalogConstants.RCFILE_TEXT_SERDE, table1.getMeta().getOption(CatalogConstants.RCFILE_SERDE));
    store.deleteTable(tableName);
  }

  @Test
  public void testTableWithNullValue() throws Exception {
    Options options = new Options();
    options.put(CatalogConstants.CSVFILE_DELIMITER, StringEscapeUtils.escapeJava("\u0001"));
    options.put(CatalogConstants.CSVFILE_NULL, StringEscapeUtils.escapeJava("\\N"));
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.CSV, options);


    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("s_suppkey", TajoDataTypes.Type.INT4);
    schema.addColumn("s_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("s_address", TajoDataTypes.Type.TEXT);
    schema.addColumn("s_nationkey", TajoDataTypes.Type.INT4);
    schema.addColumn("s_phone", TajoDataTypes.Type.TEXT);
    schema.addColumn("s_acctbal", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("s_comment", TajoDataTypes.Type.TEXT);

    String tableName = DB_NAME + "." + SUPPLIER;
    TableDesc table = new TableDesc(tableName, schema, meta, warehousePath);


    store.addTable(table.getProto());
    assertTrue(store.existTable(tableName));

    TableDesc table1 = new TableDesc(store.getTable(table.getName()));
    assertEquals(table.getName(), table1.getName());
    assertEquals(new Path(table.getPath(), SUPPLIER), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(table.getMeta().getOption(CatalogConstants.CSVFILE_DELIMITER),
        table1.getMeta().getOption(CatalogConstants.CSVFILE_DELIMITER));

    assertEquals(table.getMeta().getOption(CatalogConstants.CSVFILE_NULL),
        table1.getMeta().getOption(CatalogConstants.CSVFILE_NULL));
    store.deleteTable(tableName);
  }

  @Test
  public void testAddTableByPartition() throws Exception {
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.CSV, new Options());

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("n_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("n_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("n_comment", TajoDataTypes.Type.TEXT);


    String tableName = DB_NAME + "." + NATION;
    TableDesc table = new TableDesc(tableName, schema, meta, warehousePath);

    org.apache.tajo.catalog.Schema expressionSchema = new org.apache.tajo.catalog.Schema();
    expressionSchema.addColumn("n_nationkey", TajoDataTypes.Type.INT4);

    PartitionMethodDesc partitions = new PartitionMethodDesc(table.getName(),
        CatalogProtos.PartitionType.COLUMN, expressionSchema.getColumn(0).getQualifiedName(), expressionSchema);
    table.setPartitionMethod(partitions);

    store.addTable(table.getProto());
    assertTrue(store.existTable(table.getName()));

    TableDesc table1 = new TableDesc(store.getTable(table.getName()));
    assertEquals(table.getName(), table1.getName());
    assertEquals(new Path(table.getPath(), NATION), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }


    Schema partitionSchema = table.getPartitionMethod().getExpressionSchema();
    Schema partitionSchema1 = table1.getPartitionMethod().getExpressionSchema();
    assertEquals(partitionSchema.size(), partitionSchema1.size());
    for (int i = 0; i < partitionSchema.size(); i++) {
      assertEquals(partitionSchema.getColumn(i).getSimpleName(), partitionSchema1.getColumn(i).getSimpleName());
    }

    store.deleteTable(tableName);
  }


  @Test
  public void testGetAllTableNames() throws Exception{
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.CSV, new Options());
    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("n_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("n_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("n_comment", TajoDataTypes.Type.TEXT);

    String[] tableNames = new String[]{"default.table1", "default.table2", "default.table3"};

    for(String tableName : tableNames){
      TableDesc table = new TableDesc(tableName, schema, meta, warehousePath);
      store.addTable(table.getProto());
    }

    List<String> tables = store.getAllTableNames();
    assertEquals(tableNames.length, tables.size());

    for(String tableName : tableNames){
      assertTrue(tables.contains(tableName));
    }

    for(String tableName : tableNames){
      store.deleteTable(tableName);
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.CSV, new Options());
    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("n_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("n_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("n_comment", TajoDataTypes.Type.TEXT);

    String tableName = "table1";
    TableDesc table = new TableDesc(DB_NAME + "." + tableName, schema, meta, warehousePath);
    store.addTable(table.getProto());
    assertTrue(store.existTable(table.getName()));

    TableDesc table1 = new TableDesc(store.getTable(table.getName()));
    FileSystem fs = FileSystem.getLocal(new Configuration());
    assertTrue(fs.exists(table1.getPath()));

    store.deleteTable(table1.getName());
    assertFalse(store.existTable(table1.getName()));
    fs.close();
  }
}
