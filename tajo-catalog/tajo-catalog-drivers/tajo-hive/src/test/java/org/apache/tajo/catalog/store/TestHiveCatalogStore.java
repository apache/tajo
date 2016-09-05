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


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionKeyProto;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.schema.IdentifierUtil;
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
import java.util.Map;
import java.util.Set;

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
  private static StorageFormatFactory formatFactory;

  @BeforeClass
  public static void setUp() throws Exception {
    formatFactory = new StorageFormatFactory();
    Path testPath = CommonTestingUtil.getTestDir();
    warehousePath = new Path(testPath, "warehouse");

    //create local hiveMeta
    HiveConf conf = new HiveConf();
    String jdbcUri = "jdbc:derby:;databaseName="+testPath.toUri().getPath()+"metastore_db;create=true";
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehousePath.toUri().toString());
    conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, jdbcUri);
    conf.set(TajoConf.ConfVars.WAREHOUSE_DIR.varname, warehousePath.toUri().toString());
    conf.setBoolean("datanucleus.schema.autoCreateAll", true);

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
    TableMeta meta = new TableMeta(BuiltinStorages.TEXT, new KeyValueSet());

    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("c_custkey", TajoDataTypes.Type.INT4)
        .add("c_name", TajoDataTypes.Type.TEXT)
        .add("c_address", TajoDataTypes.Type.TEXT)
        .add("c_nationkey", TajoDataTypes.Type.INT4)
        .add("c_phone", TajoDataTypes.Type.TEXT)
        .add("c_acctbal", TajoDataTypes.Type.FLOAT8)
        .add("c_mktsegment", TajoDataTypes.Type.TEXT)
        .add("c_comment", TajoDataTypes.Type.TEXT)
        .build();

    TableDesc table = new TableDesc(IdentifierUtil.buildFQName(DB_NAME, CUSTOMER), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, CUSTOMER)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, CUSTOMER));

    StorageFormatDescriptor descriptor = formatFactory.get(IOConstants.TEXTFILE);
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = store.getHiveTable(DB_NAME, CUSTOMER);
    assertEquals(descriptor.getInputFormat(), hiveTable.getSd().getInputFormat());
    //IgnoreKeyTextOutputFormat was deprecated
    assertEquals(HiveIgnoreKeyTextOutputFormat.class.getName(), hiveTable.getSd().getOutputFormat());

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, CUSTOMER));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StringEscapeUtils.escapeJava(StorageConstants.DEFAULT_FIELD_DELIMITER),
        table1.getMeta().getProperty(StorageConstants.TEXT_DELIMITER));

    Map<String, String> expected = getProperties(DB_NAME, CUSTOMER);
    Map<String, String> toSet = new ImmutableMap.Builder<String, String>()
        .put("key1", "value1")
        .put("key2", "value2")
        .build();
    expected.putAll(toSet);

    setProperty(DB_NAME, CUSTOMER, toSet);
    Map<String, String> actual = getProperties(DB_NAME, CUSTOMER);
    assertEquals(actual.get(StorageConstants.TEXT_DELIMITER), expected.get(StorageConstants.TEXT_DELIMITER));
    assertEquals(actual.get("key1"), expected.get("key1"));
    assertEquals(actual.get("key2"), expected.get("key2"));

    Set<String> toUnset = Sets.newHashSet("key2", "key3");
    for (String key : toUnset) {
      expected.remove(key);
    }
    unSetProperty(DB_NAME, CUSTOMER, toUnset);
    actual = getProperties(DB_NAME, CUSTOMER);
    assertEquals(actual.get(StorageConstants.TEXT_DELIMITER), expected.get(StorageConstants.TEXT_DELIMITER));
    assertEquals(actual.get("key1"), expected.get("key1"));
    assertNull(actual.get("key2"));

    store.dropTable(DB_NAME, CUSTOMER);
  }

  @Test
  public void testTableUsingRCFileWithBinarySerde() throws Exception {
    KeyValueSet options = new KeyValueSet();
    options.set(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_BINARY_SERDE);
    TableMeta meta = new TableMeta(BuiltinStorages.RCFILE, options);

    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("r_regionkey", TajoDataTypes.Type.INT4)
        .add("r_name", TajoDataTypes.Type.TEXT)
        .add("r_comment", TajoDataTypes.Type.TEXT)
        .build();

    TableDesc table = new TableDesc(IdentifierUtil.buildFQName(DB_NAME, REGION), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, REGION)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, REGION));

    StorageFormatDescriptor descriptor = formatFactory.get(IOConstants.RCFILE);
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = store.getHiveTable(DB_NAME, REGION);
    assertEquals(descriptor.getInputFormat(), hiveTable.getSd().getInputFormat());
    assertEquals(descriptor.getOutputFormat(), hiveTable.getSd().getOutputFormat());

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, REGION));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StorageConstants.DEFAULT_BINARY_SERDE,
        table1.getMeta().getProperty(StorageConstants.RCFILE_SERDE));

    Map<String, String> expected = getProperties(DB_NAME, REGION);
    Map<String, String> toSet = new ImmutableMap.Builder<String, String>()
        .put("key1", "value1")
        .put("key2", "value2")
        .build();
    expected.putAll(toSet);

    setProperty(DB_NAME, REGION, toSet);
    Map<String, String> actual = getProperties(DB_NAME, REGION);
    assertEquals(actual.get(StorageConstants.TEXT_DELIMITER), expected.get(StorageConstants.TEXT_DELIMITER));
    assertEquals(actual.get("key1"), expected.get("key1"));
    assertEquals(actual.get("key2"), expected.get("key2"));

    Set<String> toUnset = Sets.newHashSet("key2", "key3");
    for (String key : toUnset) {
      expected.remove(key);
    }
    unSetProperty(DB_NAME, REGION, toUnset);
    actual = getProperties(DB_NAME, REGION);
    assertEquals(actual.get(StorageConstants.TEXT_DELIMITER), expected.get(StorageConstants.TEXT_DELIMITER));
    assertEquals(actual.get("key1"), expected.get("key1"));
    assertNull(actual.get("key2"));

    store.dropTable(DB_NAME, REGION);
  }

  @Test
  public void testTableUsingRCFileWithTextSerde() throws Exception {
    KeyValueSet options = new KeyValueSet();
    options.set(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_TEXT_SERDE);
    TableMeta meta = new TableMeta(BuiltinStorages.RCFILE, options);

    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("r_regionkey", TajoDataTypes.Type.INT4)
        .add("r_name", TajoDataTypes.Type.TEXT)
        .add("r_comment", TajoDataTypes.Type.TEXT)
        .build();

    TableDesc table = new TableDesc(IdentifierUtil.buildFQName(DB_NAME, REGION), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, REGION)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, REGION));

    StorageFormatDescriptor descriptor = formatFactory.get(IOConstants.RCFILE);
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = store.getHiveTable(DB_NAME, REGION);
    assertEquals(descriptor.getInputFormat(), hiveTable.getSd().getInputFormat());
    assertEquals(descriptor.getOutputFormat(), hiveTable.getSd().getOutputFormat());

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, REGION));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StorageConstants.DEFAULT_TEXT_SERDE, table1.getMeta().getProperty(StorageConstants.RCFILE_SERDE));

    Map<String, String> expected = getProperties(DB_NAME, REGION);
    Map<String, String> toSet = new ImmutableMap.Builder<String, String>()
            .put("key1", "value1")
            .put("key2", "value2")
            .build();
    expected.putAll(toSet);

    setProperty(DB_NAME, REGION, toSet);
    Map<String, String> actual = getProperties(DB_NAME, REGION);
    assertEquals(actual.get(StorageConstants.TEXT_DELIMITER), expected.get(StorageConstants.TEXT_DELIMITER));
    assertEquals(actual.get("key1"), expected.get("key1"));
    assertEquals(actual.get("key2"), expected.get("key2"));

    Set<String> toUnset = Sets.newHashSet("key2", "key3");
    for (String key : toUnset) {
      expected.remove(key);
    }
    unSetProperty(DB_NAME, REGION, toUnset);
    actual = getProperties(DB_NAME, REGION);
    assertEquals(actual.get(StorageConstants.TEXT_DELIMITER), expected.get(StorageConstants.TEXT_DELIMITER));
    assertEquals(actual.get("key1"), expected.get("key1"));
    assertNull(actual.get("key2"));

    store.dropTable(DB_NAME, REGION);
  }

  @Test
  public void testTableWithNullValue() throws Exception {
    KeyValueSet options = new KeyValueSet();
    options.set(StorageConstants.TEXT_DELIMITER, StringEscapeUtils.escapeJava("\u0002"));
    options.set(StorageConstants.TEXT_NULL, StringEscapeUtils.escapeJava("\u0003"));
    TableMeta meta = new TableMeta(BuiltinStorages.TEXT, options);

    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("s_suppkey", TajoDataTypes.Type.INT4)
        .add("s_name", TajoDataTypes.Type.TEXT)
        .add("s_address", TajoDataTypes.Type.TEXT)
        .add("s_nationkey", TajoDataTypes.Type.INT4)
        .add("s_phone", TajoDataTypes.Type.TEXT)
        .add("s_acctbal", TajoDataTypes.Type.FLOAT8)
        .add("s_comment", TajoDataTypes.Type.TEXT)
        .build();

    TableDesc table = new TableDesc(IdentifierUtil.buildFQName(DB_NAME, SUPPLIER), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, SUPPLIER)).toUri());

    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, SUPPLIER));

    StorageFormatDescriptor descriptor = formatFactory.get(IOConstants.TEXTFILE);
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = store.getHiveTable(DB_NAME, SUPPLIER);
    assertEquals(descriptor.getInputFormat(), hiveTable.getSd().getInputFormat());
    //IgnoreKeyTextOutputFormat was deprecated
    assertEquals(HiveIgnoreKeyTextOutputFormat.class.getName(), hiveTable.getSd().getOutputFormat());

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, SUPPLIER));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(table.getMeta().getProperty(StorageConstants.TEXT_DELIMITER),
        table1.getMeta().getProperty(StorageConstants.TEXT_DELIMITER));

    assertEquals(table.getMeta().getProperty(StorageConstants.TEXT_NULL),
        table1.getMeta().getProperty(StorageConstants.TEXT_NULL));

    assertEquals(table1.getMeta().getProperty(StorageConstants.TEXT_DELIMITER),
        StringEscapeUtils.escapeJava("\u0002"));

    assertEquals(table1.getMeta().getProperty(StorageConstants.TEXT_NULL),
        StringEscapeUtils.escapeJava("\u0003"));

    Map<String, String> expected = getProperties(DB_NAME, SUPPLIER);
    Map<String, String> toSet = new ImmutableMap.Builder<String, String>()
            .put("key1", "value1")
            .put("key2", "value2")
            .build();
    expected.putAll(toSet);

    setProperty(DB_NAME, SUPPLIER, toSet);
    Map<String, String> actual = getProperties(DB_NAME, SUPPLIER);
    assertEquals(actual.get(StorageConstants.TEXT_DELIMITER), expected.get(StorageConstants.TEXT_DELIMITER));
    assertEquals(actual.get("key1"), expected.get("key1"));
    assertEquals(actual.get("key2"), expected.get("key2"));

    Set<String> toUnset = Sets.newHashSet("key2", "key3");
    for (String key : toUnset) {
      expected.remove(key);
    }
    unSetProperty(DB_NAME, SUPPLIER, toUnset);
    actual = getProperties(DB_NAME, SUPPLIER);
    assertEquals(actual.get(StorageConstants.TEXT_DELIMITER), expected.get(StorageConstants.TEXT_DELIMITER));
    assertEquals(actual.get("key1"), expected.get("key1"));
    assertNull(actual.get("key2"));

    store.dropTable(DB_NAME, SUPPLIER);

  }

  // TODO: This should be added at TAJO-1891
  public void testAddTableByPartition() throws Exception {
    TableMeta meta = new TableMeta("TEXT", new KeyValueSet());

    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("n_name", TajoDataTypes.Type.TEXT)
        .add("n_regionkey", TajoDataTypes.Type.INT4)
        .add("n_comment", TajoDataTypes.Type.TEXT)
        .build();


    TableDesc table = new TableDesc(IdentifierUtil.buildFQName(DB_NAME, NATION), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, NATION)).toUri());

    org.apache.tajo.catalog.Schema expressionSchema = SchemaBuilder.builder()
        .add("n_nationkey", TajoDataTypes.Type.INT4)
        .add("n_date", TajoDataTypes.Type.TEXT)
        .build();

    PartitionMethodDesc partitions = new PartitionMethodDesc(
        DB_NAME,
        NATION,
        CatalogProtos.PartitionType.COLUMN, "n_nationkey,n_date", expressionSchema);
    table.setPartitionMethod(partitions);

    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, NATION));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, NATION));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
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

    testAddPartition(table1.getUri(), NATION, "n_nationkey=10/n_date=20150101");
    testAddPartition(table1.getUri(), NATION, "n_nationkey=10/n_date=20150102");
    testAddPartition(table1.getUri(), NATION, "n_nationkey=20/n_date=20150101");
    testAddPartition(table1.getUri(), NATION, "n_nationkey=20/n_date=20150102");
    testAddPartition(table1.getUri(), NATION, "n_nationkey=30/n_date=20150101");
    testAddPartition(table1.getUri(), NATION, "n_nationkey=30/n_date=20150102");

    List<String> partitionNames = new ArrayList<>();
    partitionNames.add("n_nationkey=40/n_date=20150801");
    partitionNames.add("n_nationkey=40/n_date=20150802");
    partitionNames.add("n_nationkey=50/n_date=20150801");
    partitionNames.add("n_nationkey=50/n_date=20150802");
    testAddPartitions(table1.getUri(), NATION, partitionNames);

    testGetPartitionsByAlgebra(DB_NAME, NATION);

    testDropPartition(NATION, "n_nationkey=10/n_date=20150101");
    testDropPartition(NATION, "n_nationkey=10/n_date=20150102");
    testDropPartition(NATION, "n_nationkey=20/n_date=20150101");
    testDropPartition(NATION, "n_nationkey=20/n_date=20150102");
    testDropPartition(NATION, "n_nationkey=30/n_date=20150101");
    testDropPartition(NATION, "n_nationkey=30/n_date=20150102");

    CatalogProtos.PartitionDescProto partition = store.getPartition(DB_NAME, NATION, "n_nationkey=10/n_date=20150101");
    assertNull(partition);

    partition = store.getPartition(DB_NAME, NATION, "n_nationkey=20/n_date=20150102");
    assertNull(partition);

    store.dropTable(DB_NAME, NATION);
  }

  private void testGetPartitionsByAlgebra(String databaseName, String tableName) throws Exception {
    String qfTableName = databaseName + "." + tableName;

    // Equals Operator
    CatalogProtos.PartitionsByAlgebraProto.Builder request = CatalogProtos.PartitionsByAlgebraProto.newBuilder();
    request.setDatabaseName(databaseName);
    request.setTableName(tableName);

    String algebra = "{\n" +
      "  \"LeftExpr\": {\n" +
      "    \"LeftExpr\": {\n" +
      "      \"Qualifier\": \"" + qfTableName + "\",\n" +
      "      \"ColumnName\": \"n_nationkey\",\n" +
      "      \"OpType\": \"Column\"\n" +
      "    },\n" +
      "    \"RightExpr\": {\n" +
      "      \"Value\": \"10\",\n" +
      "      \"ValueType\": \"Unsigned_Integer\",\n" +
      "      \"OpType\": \"Literal\"\n" +
      "    },\n" +
      "    \"OpType\": \"Equals\"\n" +
      "  },\n" +
      "  \"RightExpr\": {\n" +
      "    \"LeftExpr\": {\n" +
      "      \"Qualifier\": \"" + qfTableName + "\",\n" +
      "      \"ColumnName\": \"n_date\",\n" +
      "      \"OpType\": \"Column\"\n" +
      "    },\n" +
      "    \"RightExpr\": {\n" +
      "      \"Value\": \"20150101\",\n" +
      "      \"ValueType\": \"String\",\n" +
      "      \"OpType\": \"Literal\"\n" +
      "    },\n" +
      "    \"OpType\": \"Equals\"\n" +
      "  },\n" +
      "  \"OpType\": \"And\"\n" +
      "}";

    request.setAlgebra(algebra);

    List<CatalogProtos.PartitionDescProto> partitions = store.getPartitionsByAlgebra(request.build());
    assertNotNull(partitions);
    assertEquals(1, partitions.size());

    // OR
    algebra = "{\n" +
      "  \"LeftExpr\": {\n" +
      "    \"LeftExpr\": {\n" +
      "      \"Qualifier\": \"" + qfTableName + "\",\n" +
      "      \"ColumnName\": \"n_nationkey\",\n" +
      "      \"OpType\": \"Column\"\n" +
      "    },\n" +
      "    \"RightExpr\": {\n" +
      "      \"Value\": \"20\",\n" +
      "      \"ValueType\": \"Unsigned_Integer\",\n" +
      "      \"OpType\": \"Literal\"\n" +
      "    },\n" +
      "    \"OpType\": \"Equals\"\n" +
      "  },\n" +
      "  \"RightExpr\": {\n" +
      "    \"LeftExpr\": {\n" +
      "      \"Qualifier\": \"" + qfTableName + "\",\n" +
      "      \"ColumnName\": \"n_nationkey\",\n" +
      "      \"OpType\": \"Column\"\n" +
      "    },\n" +
      "    \"RightExpr\": {\n" +
      "      \"Value\": \"30\",\n" +
      "      \"ValueType\": \"Unsigned_Integer\",\n" +
      "      \"OpType\": \"Literal\"\n" +
      "    },\n" +
      "    \"OpType\": \"Equals\"\n" +
      "  },\n" +
      "  \"OpType\": \"Or\"\n" +
      "}";

    request.setAlgebra(algebra);

    partitions = store.getPartitionsByAlgebra(request.build());
    assertNotNull(partitions);
    assertEquals(4, partitions.size());
  }

  private void testAddPartition(URI uri, String tableName, String partitionName) throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(DB_NAME + "." + tableName);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    Path path = new Path(uri.getPath(), partitionName);

    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setPartitionName(partitionName);

    List<PartitionKeyProto> partitionKeyList = new ArrayList<>();
    String[] partitionNames = partitionName.split("/");
    for (String aName : partitionNames) {
      String[] eachPartitionName = aName.split("=");

      PartitionKeyProto.Builder builder = PartitionKeyProto.newBuilder();
      builder.setColumnName(eachPartitionName[0]);
      builder.setPartitionValue(eachPartitionName[1]);
      partitionKeyList.add(builder.build());
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

  private void testAddPartitions(URI uri, String tableName, List<String> partitionNames) throws Exception {
    List<CatalogProtos.PartitionDescProto> partitions = new ArrayList<>();
    for (String partitionName : partitionNames) {
      CatalogProtos.PartitionDescProto.Builder builder = CatalogProtos.PartitionDescProto.newBuilder();
      builder.setPartitionName(partitionName);
      Path path = new Path(uri.getPath(), partitionName);
      builder.setPath(path.toString());

      List<PartitionKeyProto> partitionKeyList = new ArrayList<>();
      String[] splits = partitionName.split("/");
      for (String aSplit : splits) {
        String[] eachPartitionName = aSplit.split("=");

        PartitionKeyProto.Builder keyBuilder = PartitionKeyProto.newBuilder();
        keyBuilder.setColumnName(eachPartitionName[0]);
        keyBuilder.setPartitionValue(eachPartitionName[1]);
        partitionKeyList.add(keyBuilder.build());
      }
      builder.addAllPartitionKeys(partitionKeyList);
      partitions.add(builder.build());
    }

    store.addPartitions(DB_NAME, tableName, partitions, true);

    for (String partitionName : partitionNames) {
      CatalogProtos.PartitionDescProto resultDesc = store.getPartition(DB_NAME, NATION, partitionName);
      assertNotNull(resultDesc);
      assertEquals(resultDesc.getPartitionName(), partitionName);
      assertEquals(resultDesc.getPath(), uri.toString() + "/" + partitionName);
      assertEquals(resultDesc.getPartitionKeysCount(), 2);

      String[] split = partitionName.split("/");
      for (int i = 0; i < resultDesc.getPartitionKeysCount(); i++) {
        CatalogProtos.PartitionKeyProto keyProto = resultDesc.getPartitionKeys(i);
        String[] eachName = split[i].split("=");
        assertEquals(keyProto.getPartitionValue(), eachName[1]);
      }
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

  private Map<String, String> getProperties(String dbName, String tableName) throws Exception {
    TableDesc tableDesc = new TableDesc(store.getTable(dbName, tableName));
    TableMeta tableMeta = tableDesc.getMeta();
    return tableMeta.toMap();
  }

  private void setProperty(String dbName, String tableName, Map<String, String> properties) throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(dbName + "." + tableName);
    alterTableDesc.setAlterTableType(AlterTableType.SET_PROPERTY);
    alterTableDesc.setProperties(new KeyValueSet(properties));

    store.alterTable(alterTableDesc.getProto());
  }

  private void unSetProperty(String dbName, String tableName, Set<String> propertyKeys) throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(dbName + "." + tableName);
    alterTableDesc.setAlterTableType(AlterTableType.UNSET_PROPERTY);
    alterTableDesc.setUnsetPropertyKey(propertyKeys);

    store.alterTable(alterTableDesc.getProto());
  }

  @Test
  public void testGetAllTableNames() throws Exception{
    TableMeta meta = new TableMeta(BuiltinStorages.TEXT, new KeyValueSet());
    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("n_name", TajoDataTypes.Type.TEXT)
        .add("n_regionkey", TajoDataTypes.Type.INT4)
        .add("n_comment", TajoDataTypes.Type.TEXT)
        .build();

    String[] tableNames = new String[]{"table1", "table2", "table3"};

    for(String tableName : tableNames){
      TableDesc table = new TableDesc(IdentifierUtil.buildFQName("default", tableName), schema, meta,
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
    TableMeta meta = new TableMeta(BuiltinStorages.TEXT, new KeyValueSet());
    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("n_name", TajoDataTypes.Type.TEXT)
        .add("n_regionkey", TajoDataTypes.Type.INT4)
        .add("n_comment", TajoDataTypes.Type.TEXT)
        .build();

    String tableName = "table1";
    TableDesc table = new TableDesc(DB_NAME + "." + tableName, schema, meta, warehousePath.toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, tableName));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, tableName));
    FileSystem fs = FileSystem.getLocal(new Configuration());
    assertTrue(fs.exists(new Path(table1.getUri())));

    store.dropTable(DB_NAME, tableName);
    assertFalse(store.existTable(DB_NAME, tableName));
    fs.close();
  }

  @Test
  public void testTableUsingSequenceFileWithBinarySerde() throws Exception {
    KeyValueSet options = new KeyValueSet();
    options.set(StorageConstants.SEQUENCEFILE_SERDE, StorageConstants.DEFAULT_BINARY_SERDE);
    TableMeta meta = new TableMeta(BuiltinStorages.SEQUENCE_FILE, options);

    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("r_regionkey", TajoDataTypes.Type.INT4)
        .add("r_name", TajoDataTypes.Type.TEXT)
        .add("r_comment", TajoDataTypes.Type.TEXT)
        .build();

    TableDesc table = new TableDesc(IdentifierUtil.buildFQName(DB_NAME, REGION), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, REGION)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, REGION));

    StorageFormatDescriptor descriptor = formatFactory.get(IOConstants.SEQUENCEFILE);
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = store.getHiveTable(DB_NAME, REGION);
    assertEquals(descriptor.getInputFormat(), hiveTable.getSd().getInputFormat());
    assertEquals(descriptor.getOutputFormat(), hiveTable.getSd().getOutputFormat());

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, REGION));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StorageConstants.DEFAULT_BINARY_SERDE,
      table1.getMeta().getProperty(StorageConstants.SEQUENCEFILE_SERDE));
    store.dropTable(DB_NAME, REGION);
  }

  @Test
  public void testTableUsingSequenceFileWithTextSerde() throws Exception {
    KeyValueSet options = new KeyValueSet();
    options.set(StorageConstants.SEQUENCEFILE_SERDE, StorageConstants.DEFAULT_TEXT_SERDE);
    options.set(StorageConstants.TEXT_DELIMITER, "\u0001");
    options.set(StorageConstants.TEXT_NULL, NullDatum.DEFAULT_TEXT);
    TableMeta meta = new TableMeta(BuiltinStorages.SEQUENCE_FILE, options);

    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("r_regionkey", TajoDataTypes.Type.INT4)
        .add("r_name", TajoDataTypes.Type.TEXT)
        .add("r_comment", TajoDataTypes.Type.TEXT)
        .build();

    TableDesc table = new TableDesc(IdentifierUtil.buildFQName(DB_NAME, REGION), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, REGION)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, REGION));

    StorageFormatDescriptor descriptor = formatFactory.get(IOConstants.SEQUENCEFILE);
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = store.getHiveTable(DB_NAME, REGION);
    assertEquals(descriptor.getInputFormat(), hiveTable.getSd().getInputFormat());
    assertEquals(descriptor.getOutputFormat(), hiveTable.getSd().getOutputFormat());

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, REGION));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StorageConstants.DEFAULT_TEXT_SERDE, table1.getMeta().getProperty(StorageConstants.SEQUENCEFILE_SERDE));
    assertEquals("\u0001", StringEscapeUtils.unescapeJava(table1.getMeta().getProperty(StorageConstants
      .TEXT_DELIMITER)));
    assertEquals(NullDatum.DEFAULT_TEXT, table1.getMeta().getProperty(StorageConstants.TEXT_NULL));
    store.dropTable(DB_NAME, REGION);
  }


  @Test
  public void testTableUsingParquet() throws Exception {
    TableMeta meta = new TableMeta("PARQUET", new KeyValueSet());

    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("c_custkey", TajoDataTypes.Type.INT4)
        .add("c_name", TajoDataTypes.Type.TEXT)
        .add("c_address", TajoDataTypes.Type.TEXT)
        .add("c_nationkey", TajoDataTypes.Type.INT4)
        .add("c_phone", TajoDataTypes.Type.TEXT)
        .add("c_acctbal", TajoDataTypes.Type.FLOAT8)
        .add("c_mktsegment", TajoDataTypes.Type.TEXT)
        .add("c_comment", TajoDataTypes.Type.TEXT)
        .build();

    TableDesc table = new TableDesc(IdentifierUtil.buildFQName(DB_NAME, CUSTOMER), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, CUSTOMER)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, CUSTOMER));

    StorageFormatDescriptor descriptor = formatFactory.get(IOConstants.PARQUET);
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = store.getHiveTable(DB_NAME, CUSTOMER);
    assertEquals(descriptor.getInputFormat(), hiveTable.getSd().getInputFormat());
    assertEquals(descriptor.getOutputFormat(), hiveTable.getSd().getOutputFormat());

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, CUSTOMER));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    store.dropTable(DB_NAME, CUSTOMER);
  }

  @Test
  public void testDataTypeCompatibility() throws Exception {
    String tableName = IdentifierUtil.normalizeIdentifier("testDataTypeCompatibility");

    TableMeta meta = new TableMeta(BuiltinStorages.TEXT, new KeyValueSet());

    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("col1", TajoDataTypes.Type.INT4)
        .add("col2", TajoDataTypes.Type.INT1)
        .add("col3", TajoDataTypes.Type.INT2)
        .add("col4", TajoDataTypes.Type.INT8)
        .add("col5", TajoDataTypes.Type.BOOLEAN)
        .add("col6", TajoDataTypes.Type.FLOAT4)
        .add("col7", TajoDataTypes.Type.FLOAT8)
        .add("col8", TajoDataTypes.Type.TEXT)
        .add("col9", TajoDataTypes.Type.BLOB)
        .add("col10", TajoDataTypes.Type.TIMESTAMP)
        .add("col11", TajoDataTypes.Type.DATE)
        .build();

    TableDesc table = new TableDesc(IdentifierUtil.buildFQName(DB_NAME, tableName), schema, meta,
      new Path(warehousePath, new Path(DB_NAME, tableName)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, tableName));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, tableName));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StringEscapeUtils.escapeJava(StorageConstants.DEFAULT_FIELD_DELIMITER),
      table1.getMeta().getProperty(StorageConstants.TEXT_DELIMITER));
    store.dropTable(DB_NAME, tableName);
  }

  @Test
  public void testTableUsingRegex() throws Exception {
    TableMeta meta = new TableMeta(BuiltinStorages.REGEX, new KeyValueSet());
    meta.putProperty(StorageConstants.TEXT_REGEX, "([^ ]*)");
    meta.putProperty(StorageConstants.TEXT_REGEX_OUTPUT_FORMAT_STRING, "%1$s");

    org.apache.tajo.catalog.Schema schema = SchemaBuilder.builder()
        .add("c_custkey", TajoDataTypes.Type.TEXT)
        .build();

    TableDesc table = new TableDesc(IdentifierUtil.buildFQName(DB_NAME, CUSTOMER), schema, meta,
        new Path(warehousePath, new Path(DB_NAME, CUSTOMER)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, CUSTOMER));

    org.apache.hadoop.hive.ql.metadata.Table hiveTable = store.getHiveTable(DB_NAME, CUSTOMER);
    assertEquals(TextInputFormat.class.getName(), hiveTable.getSd().getInputFormat());
    assertEquals(HiveIgnoreKeyTextOutputFormat.class.getName(), hiveTable.getSd().getOutputFormat());
    assertEquals(RegexSerDe.class.getName(), hiveTable.getSerializationLib());

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, CUSTOMER));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    store.dropTable(DB_NAME, CUSTOMER);
  }
}
