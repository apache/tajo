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
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.partition.PartitionKey;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

/**
 * TestHiveCatalogStore. Test case for
 * {@link org.apache.tajo.catalog.store.HiveCatalogStore}
 */

public class TestHiveCatalogStore {
  private static final String DB_NAME = "test_hive";
  private static final String CUSTOMER = "customer";
  private static final String NATION = "nation";
  private static final String REGION = "region";
  private static final String SUPPLIER = "supplier";

  private static HiveCatalogStore store;
  private static Path warehousePath;

  @BeforeClass
  public static void setUp() throws Exception {
    Path testPath = CommonTestingUtil.getTestDir();
    warehousePath = new Path(testPath, "warehouse");

    //create local hiveMeta
    HiveConf conf = new HiveConf();
    String jdbcUri = "jdbc:derby:;databaseName="+testPath.toUri().getPath()+"metastore_db;create=true";
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehousePath.toUri().toString());
    conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, jdbcUri);
    conf.set(TajoConf.ConfVars.WAREHOUSE_DIR.varname, warehousePath.toUri().toString());

    // create local HiveCatalogStore.
    TajoConf tajoConf = new TajoConf(conf);
    store = new HiveCatalogStore(tajoConf);
    store.createDatabase(DB_NAME, null);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    store.close();
  }

  @Test
  public void testTableUsingTextFile() throws Exception {
    TableMeta meta = new TableMeta("CSV", new KeyValueSet());

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("c_custkey", TajoDataTypes.Type.INT4);
    schema.addColumn("c_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_address", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_nationkey", TajoDataTypes.Type.INT4);
    schema.addColumn("c_phone", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_acctbal", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("c_mktsegment", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_comment", TajoDataTypes.Type.TEXT);

    TableDesc table = new TableDesc(CatalogUtil.buildFQName(DB_NAME, CUSTOMER), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, CUSTOMER)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, CUSTOMER));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, CUSTOMER));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getPath(), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StringEscapeUtils.escapeJava(StorageConstants.DEFAULT_FIELD_DELIMITER),
        table1.getMeta().getOption(StorageConstants.TEXT_DELIMITER));
    store.dropTable(DB_NAME, CUSTOMER);
  }

  @Test
  public void testTableUsingRCFileWithBinarySerde() throws Exception {
    KeyValueSet options = new KeyValueSet();
    options.set(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_BINARY_SERDE);
    TableMeta meta = new TableMeta("RCFILE", options);

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("r_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("r_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("r_comment", TajoDataTypes.Type.TEXT);

    TableDesc table = new TableDesc(CatalogUtil.buildFQName(DB_NAME, REGION), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, REGION)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, REGION));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, REGION));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getPath(), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StorageConstants.DEFAULT_BINARY_SERDE,
        table1.getMeta().getOption(StorageConstants.RCFILE_SERDE));
    store.dropTable(DB_NAME, REGION);
  }

  @Test
  public void testTableUsingRCFileWithTextSerde() throws Exception {
    KeyValueSet options = new KeyValueSet();
    options.set(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_TEXT_SERDE);
    TableMeta meta = new TableMeta("RCFILE", options);

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("r_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("r_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("r_comment", TajoDataTypes.Type.TEXT);

    TableDesc table = new TableDesc(CatalogUtil.buildFQName(DB_NAME, REGION), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, REGION)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, REGION));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, REGION));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getPath(), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StorageConstants.DEFAULT_TEXT_SERDE, table1.getMeta().getOption(StorageConstants.RCFILE_SERDE));
    store.dropTable(DB_NAME, REGION);
  }

  @Test
  public void testTableWithNullValue() throws Exception {
    KeyValueSet options = new KeyValueSet();
    options.set(StorageConstants.TEXT_DELIMITER, StringEscapeUtils.escapeJava("\u0002"));
    options.set(StorageConstants.TEXT_NULL, StringEscapeUtils.escapeJava("\u0003"));
    TableMeta meta = new TableMeta("CSV", options);

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("s_suppkey", TajoDataTypes.Type.INT4);
    schema.addColumn("s_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("s_address", TajoDataTypes.Type.TEXT);
    schema.addColumn("s_nationkey", TajoDataTypes.Type.INT4);
    schema.addColumn("s_phone", TajoDataTypes.Type.TEXT);
    schema.addColumn("s_acctbal", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("s_comment", TajoDataTypes.Type.TEXT);

    TableDesc table = new TableDesc(CatalogUtil.buildFQName(DB_NAME, SUPPLIER), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, SUPPLIER)).toUri());

    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, SUPPLIER));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, SUPPLIER));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getPath(), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(table.getMeta().getOption(StorageConstants.TEXT_DELIMITER),
        table1.getMeta().getOption(StorageConstants.TEXT_DELIMITER));

    assertEquals(table.getMeta().getOption(StorageConstants.TEXT_NULL),
        table1.getMeta().getOption(StorageConstants.TEXT_NULL));

    assertEquals(table1.getMeta().getOption(StorageConstants.TEXT_DELIMITER),
        StringEscapeUtils.escapeJava("\u0002"));

    assertEquals(table1.getMeta().getOption(StorageConstants.TEXT_NULL),
        StringEscapeUtils.escapeJava("\u0003"));

    store.dropTable(DB_NAME, SUPPLIER);

  }

  @Test
  public void testAddTableByPartition() throws Exception {
    TableMeta meta = new TableMeta("CSV", new KeyValueSet());

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("n_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("n_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("n_comment", TajoDataTypes.Type.TEXT);


    TableDesc table = new TableDesc(CatalogUtil.buildFQName(DB_NAME, NATION), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, NATION)).toUri());

    org.apache.tajo.catalog.Schema expressionSchema = new org.apache.tajo.catalog.Schema();
    expressionSchema.addColumn("n_nationkey", TajoDataTypes.Type.INT4);
    expressionSchema.addColumn("n_date", TajoDataTypes.Type.TEXT);

    PartitionMethodDesc partitions = new PartitionMethodDesc(
        DB_NAME,
        NATION,
        CatalogProtos.PartitionType.COLUMN, "n_nationkey,n_date", expressionSchema);
    table.setPartitionMethod(partitions);

    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, NATION));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, NATION));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getPath(), table1.getPath());
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

    testAddPartition(table1.getPath(), NATION, "n_nationkey=10/n_date=20150101");
    testAddPartition(table1.getPath(), NATION, "n_nationkey=20/n_date=20150102");

    testDropPartition(NATION, "n_nationkey=10/n_date=20150101");
    testDropPartition(NATION, "n_nationkey=20/n_date=20150102");

    CatalogProtos.PartitionDescProto partition = store.getPartition(DB_NAME, NATION, "n_nationkey=10/n_date=20150101");
    assertNull(partition);

    partition = store.getPartition(DB_NAME, NATION, "n_nationkey=20/n_date=20150102");
    assertNull(partition);

    store.dropTable(DB_NAME, NATION);
  }


  private void testAddPartition(URI uri, String tableName, String partitionName) throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(DB_NAME + "." + tableName);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    Path path = new Path(uri.getPath(), partitionName);

    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setPartitionName(partitionName);

    List<PartitionKey> partitionKeyList = new ArrayList<PartitionKey>();
    String[] partitionNames = partitionName.split("/");
    for(int i = 0; i < partitionNames.length; i++) {
      String[] eachPartitionName = partitionNames[i].split("=");
      partitionKeyList.add(new PartitionKey(eachPartitionName[0], eachPartitionName[1]));
    }
    partitionDesc.setPartitionKeys(partitionKeyList);
    partitionDesc.setPath(path.toString());

    alterTableDesc.setPartitionDesc(partitionDesc);

    store.alterTable(alterTableDesc.getProto());

    CatalogProtos.PartitionDescProto resultDesc = store.getPartition(DB_NAME, NATION, partitionName);
    assertNotNull(resultDesc);
    assertEquals(resultDesc.getPartitionName(), partitionName);
    assertEquals(resultDesc.getPath(), uri.toString() + "/" + partitionName);
    assertEquals(resultDesc.getPartitionKeysCount(), 2);

    for (int i = 0; i < resultDesc.getPartitionKeysCount(); i++) {
      CatalogProtos.PartitionKeyProto keyProto = resultDesc.getPartitionKeys(i);
      String[] eachName = partitionNames[i].split("=");
      assertEquals(keyProto.getPartitionValue(), eachName[1]);
    }
  }


  private void testDropPartition(String tableName,  String partitionName) throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(DB_NAME + "." + tableName);
    alterTableDesc.setAlterTableType(AlterTableType.DROP_PARTITION);

    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setPartitionName(partitionName);

    alterTableDesc.setPartitionDesc(partitionDesc);

    store.alterTable(alterTableDesc.getProto());
  }

  @Test
  public void testGetAllTableNames() throws Exception{
    TableMeta meta = new TableMeta("CSV", new KeyValueSet());
    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("n_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("n_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("n_comment", TajoDataTypes.Type.TEXT);

    String[] tableNames = new String[]{"table1", "table2", "table3"};

    for(String tableName : tableNames){
      TableDesc table = new TableDesc(CatalogUtil.buildFQName("default", tableName), schema, meta,
          new Path(warehousePath, new Path(DB_NAME, tableName)).toUri());
      store.createTable(table.getProto());
    }

    List<String> tables = store.getAllTableNames("default");
    assertEquals(tableNames.length, tables.size());

    for(String tableName : tableNames){
      assertTrue(tables.contains(tableName));
    }

    for(String tableName : tableNames){
      store.dropTable("default", tableName);
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    TableMeta meta = new TableMeta("CSV", new KeyValueSet());
    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("n_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("n_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("n_comment", TajoDataTypes.Type.TEXT);

    String tableName = "table1";
    TableDesc table = new TableDesc(DB_NAME + "." + tableName, schema, meta, warehousePath.toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, tableName));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, tableName));
    FileSystem fs = FileSystem.getLocal(new Configuration());
    assertTrue(fs.exists(new Path(table1.getPath())));

    store.dropTable(DB_NAME, tableName);
    assertFalse(store.existTable(DB_NAME, tableName));
    fs.close();
  }

  @Test
  public void testTableUsingSequenceFileWithBinarySerde() throws Exception {
    KeyValueSet options = new KeyValueSet();
    options.set(StorageConstants.SEQUENCEFILE_SERDE, StorageConstants.DEFAULT_BINARY_SERDE);
    TableMeta meta = new TableMeta("SEQUENCEFILE", options);

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("r_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("r_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("r_comment", TajoDataTypes.Type.TEXT);

    TableDesc table = new TableDesc(CatalogUtil.buildFQName(DB_NAME, REGION), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, REGION)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, REGION));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, REGION));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getPath(), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StorageConstants.DEFAULT_BINARY_SERDE,
        table1.getMeta().getOption(StorageConstants.SEQUENCEFILE_SERDE));
    store.dropTable(DB_NAME, REGION);
  }

  @Test
  public void testTableUsingSequenceFileWithTextSerde() throws Exception {
    KeyValueSet options = new KeyValueSet();
    options.set(StorageConstants.SEQUENCEFILE_SERDE, StorageConstants.DEFAULT_TEXT_SERDE);
    TableMeta meta = new TableMeta("SEQUENCEFILE", options);

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("r_regionkey", TajoDataTypes.Type.INT4);
    schema.addColumn("r_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("r_comment", TajoDataTypes.Type.TEXT);

    TableDesc table = new TableDesc(CatalogUtil.buildFQName(DB_NAME, REGION), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, REGION)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, REGION));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, REGION));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getPath(), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StorageConstants.DEFAULT_TEXT_SERDE, table1.getMeta().getOption(StorageConstants.SEQUENCEFILE_SERDE));
    store.dropTable(DB_NAME, REGION);
  }


  @Test
  public void testTableUsingParquet() throws Exception {
    TableMeta meta = new TableMeta("PARQUET", new KeyValueSet());

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("c_custkey", TajoDataTypes.Type.INT4);
    schema.addColumn("c_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_address", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_nationkey", TajoDataTypes.Type.INT4);
    schema.addColumn("c_phone", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_acctbal", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("c_mktsegment", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_comment", TajoDataTypes.Type.TEXT);

    TableDesc table = new TableDesc(CatalogUtil.buildFQName(DB_NAME, CUSTOMER), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, CUSTOMER)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, CUSTOMER));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, CUSTOMER));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getPath(), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    store.dropTable(DB_NAME, CUSTOMER);
  }

  @Test
  public void testDataTypeCompatibility() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testDataTypeCompatibility");

    TableMeta meta = new TableMeta("CSV", new KeyValueSet());

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT4);
    schema.addColumn("col2", TajoDataTypes.Type.INT1);
    schema.addColumn("col3", TajoDataTypes.Type.INT2);
    schema.addColumn("col4", TajoDataTypes.Type.INT8);
    schema.addColumn("col5", TajoDataTypes.Type.BOOLEAN);
    schema.addColumn("col6", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col7", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col8", TajoDataTypes.Type.TEXT);
    schema.addColumn("col9", TajoDataTypes.Type.BLOB);
    schema.addColumn("col10", TajoDataTypes.Type.TIMESTAMP);
    schema.addColumn("col11", TajoDataTypes.Type.DATE);

    TableDesc table = new TableDesc(CatalogUtil.buildFQName(DB_NAME, tableName), schema, meta,
      new Path(warehousePath, new Path(DB_NAME, tableName)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, tableName));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, tableName));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getPath(), table1.getPath());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StringEscapeUtils.escapeJava(StorageConstants.DEFAULT_FIELD_DELIMITER),
      table1.getMeta().getOption(StorageConstants.TEXT_DELIMITER));
    store.dropTable(DB_NAME, tableName);
  }
}
