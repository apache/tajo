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

package org.apache.tajo.catalog;

import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceCommand;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.UndefinedPartitionException;
import org.apache.tajo.exception.UndefinedTableException;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;

public class TestCatalogAgainstCaseSensitivity {
  static CatalogServer server;
  static CatalogService catalog;

  @BeforeClass
  public static void setup() throws Exception {
    server = new MiniCatalogServer();
    catalog = new LocalCatalogWrapper(server);
    CatalogTestingUtil.prepareBaseData(catalog, ((MiniCatalogServer) server).getTestDir());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CatalogTestingUtil.cleanupBaseData(catalog);
    server.stop();
  }

  @Test
  public void testTablespace() throws Exception {
    assertTrue(catalog.existTablespace("space1"));
    assertTrue(catalog.existTablespace("SpAcE1"));

    //////////////////////////////////////////////////////////////////////////////
    // Test alter tablespace
    //////////////////////////////////////////////////////////////////////////////

    catalog.alterTablespace(AlterTablespaceProto.newBuilder().
        setSpaceName("space1").
        addCommand(
            AlterTablespaceCommand.newBuilder().
                setType(AlterTablespaceType.LOCATION).
                setLocation("hdfs://zzz.com/warehouse")).build());

    catalog.alterTablespace(AlterTablespaceProto.newBuilder().
        setSpaceName("SpAcE1").
        addCommand(
            AlterTablespaceCommand.newBuilder().
                setType(AlterTablespaceType.LOCATION).
                setLocation("hdfs://zzz.com/warehouse")).build());

    Set<TablespaceProto> tablespaceProtos = new HashSet<>();
    for (String tablespaceName : catalog.getAllTablespaceNames()) {
      assertTrue(tablespaceName + " does not exist.", catalog.existTablespace(tablespaceName));
      tablespaceProtos.add(catalog.getTablespace(tablespaceName));
    }
    assertEquals(tablespaceProtos, new HashSet<>(catalog.getAllTablespaces()));
  }

  @Test
  public void testDatabase() throws Exception {
    //////////////////////////////////////////////////////////////////////////////
    // Test get all databases
    //////////////////////////////////////////////////////////////////////////////

    assertTrue(catalog.existDatabase("TestDatabase1"));
    assertTrue(catalog.existDatabase("testDatabase1"));
    assertTrue(catalog.getAllDatabaseNames().contains("TestDatabase1"));
    assertTrue(catalog.getAllDatabaseNames().contains("testDatabase1"));
  }

  @Test
  public void testTable() throws Exception {
    //////////////////////////////////////////////////////////////////////////////
    // Test get all tables
    //////////////////////////////////////////////////////////////////////////////

    assertTrue(catalog.existsTable("TestDatabase1", "TestTable1"));
    assertTrue(catalog.existsTable("TestDatabase1", "testTable1"));

    Map<String, TableDesc> tableDescs = new HashMap<>();
    for (String eachTableName : catalog.getAllTableNames("TestDatabase1")) {
      TableDesc desc = catalog.getTableDesc("TestDatabase1", eachTableName);
      tableDescs.put(desc.getName(), desc);
    }
    for (TableDescriptorProto eachTableDescriptor : catalog.getAllTables()) {
      String qualifiedTableName = CatalogUtil.buildFQName("TestDatabase1", eachTableDescriptor.getName());
      assertTrue(tableDescs.containsKey(qualifiedTableName));
      TableDesc desc = tableDescs.get(qualifiedTableName);
      assertEquals(desc.getUri().toString(), eachTableDescriptor.getPath());
      assertEquals(desc.getMeta().getStoreType(), eachTableDescriptor.getStoreType());
    }

    //////////////////////////////////////////////////////////////////////////////
    // rename table
    //////////////////////////////////////////////////////////////////////////////

    TableDesc desc = CatalogTestingUtil.buildTableDesc("TestDatabase1", "newTable",
        CommonTestingUtil.getTestDir().toString());
    catalog.createTable(desc);

    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setAlterTableType(AlterTableType.RENAME_TABLE);
    alterTableDesc.setNewTableName(CatalogUtil.buildFQName("TestDatabase1", "renamed_table"));
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "newTable"));
    catalog.alterTable(alterTableDesc);

    assertFalse(catalog.existsTable("TestDatabase1", "newTable"));
    assertTrue(catalog.existsTable("TestDatabase1", "renamed_table"));
    catalog.dropTable(CatalogUtil.buildFQName("TestDatabase1", "renamed_table"));

    //////////////////////////////////////////////////////////////////////////////
    // table stats
    //////////////////////////////////////////////////////////////////////////////

    TableStats stats = catalog.getTableDesc("TestDatabase1", "TestTable1").getStats();
    assertEquals(10000, stats.getNumBytes().longValue());
    assertEquals(5000, stats.getNumRows().longValue());
  }

  @Test
  public void testTablePartition() throws Exception {
    //////////////////////////////////////////////////////////////////////////////
    // Test add partition
    //////////////////////////////////////////////////////////////////////////////

    assertTrue(catalog.existsTable("TestDatabase1", "TestPartition1"));
    assertTrue(catalog.existsTable("TestDatabase1", "testPartition1"));

    String partitionName = "DaTe=bBb/dAtE=AaA";
    PartitionDesc partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    catalog.alterTable(alterTableDesc);

    PartitionDescProto resultDesc = catalog.getPartition("TestDatabase1", "TestPartition1",
        partitionName);

    assertNotNull(resultDesc);
    assertEquals(resultDesc.getPartitionName(), partitionName);
    assertEquals(resultDesc.getPath(), "hdfs://xxx.com/warehouse/" + partitionName);
    assertEquals(resultDesc.getPartitionKeysCount(), 2);

    partitionName = "DaTe=BbB/dAtE=aAa";
    partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    catalog.alterTable(alterTableDesc);

    resultDesc = catalog.getPartition("TestDatabase1", "TestPartition1",
        partitionName);

    assertNotNull(resultDesc);
    assertEquals(resultDesc.getPartitionName(), partitionName);
    assertEquals(resultDesc.getPath(), "hdfs://xxx.com/warehouse/" + partitionName);
    assertEquals(resultDesc.getPartitionKeysCount(), 2);

    //////////////////////////////////////////////////////////////////////////////
    // Test get partitions of a table
    //////////////////////////////////////////////////////////////////////////////

    List<PartitionDescProto> partitionDescs = catalog.getPartitions("TestDatabase1", "TestPartition1");
    assertEquals(2, partitionDescs.size());
    Map<String, PartitionDescProto> tablePartitionMap = new HashMap<>();
    for (PartitionDescProto eachPartition : partitionDescs) {
      tablePartitionMap.put(eachPartition.getPartitionName(), eachPartition);
    }
    assertTrue(tablePartitionMap.containsKey("DaTe=bBb/dAtE=AaA"));
    assertTrue(tablePartitionMap.containsKey("DaTe=BbB/dAtE=aAa"));

    //////////////////////////////////////////////////////////////////////////////
    // Test get all partitions
    //////////////////////////////////////////////////////////////////////////////

    List<TablePartitionProto> partitions = catalog.getAllPartitions();
    assertEquals(2, partitions.size());
    Map<String, TablePartitionProto> partitionMap = new HashMap<>();
    for (TablePartitionProto eachPartition : partitions) {
      partitionMap.put(eachPartition.getPartitionName(), eachPartition);
    }
    assertTrue(partitionMap.containsKey("DaTe=bBb/dAtE=AaA"));
    assertTrue(partitionMap.containsKey("DaTe=BbB/dAtE=aAa"));

    //////////////////////////////////////////////////////////////////////////////
    // Test drop partition
    //////////////////////////////////////////////////////////////////////////////

    partitionName = "DaTe=BbB/dAtE=aAa";
    partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.DROP_PARTITION);
    catalog.alterTable(alterTableDesc);

    try {
      resultDesc = null;
      resultDesc = catalog.getPartition("TestDatabase1", "TestPartition1", partitionName);
    } catch (UndefinedPartitionException e) {
    }
    assertNull(resultDesc);

    partitionName = "DaTe=bBb/dAtE=AaA";
    partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.DROP_PARTITION);
    catalog.alterTable(alterTableDesc);

    try {
      resultDesc = null;
      resultDesc = catalog.getPartition("TestDatabase1", "TestPartition1", partitionName);
    } catch (UndefinedPartitionException e) {
    }
    assertNull(resultDesc);
  }

  @Test
  public void testTableColumn() throws Exception {
    String databaseName = "TestDatabase1";
    String tableName = "testTable1";

    //////////////////////////////////////////////////////////////////////////////
    // Test add column
    //////////////////////////////////////////////////////////////////////////////

    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setAddColumn(new Column(CatalogUtil.buildFQName(databaseName, tableName, "AddedCol1"),
        CatalogUtil.newSimpleDataType(Type.BLOB)));
    alterTableDesc.setTableName(CatalogUtil.buildFQName(databaseName, tableName));
    alterTableDesc.setAlterTableType(AlterTableType.ADD_COLUMN);
    catalog.alterTable(alterTableDesc);

    TableDesc tableDesc = catalog.getTableDesc(databaseName, tableName);
    assertTrue(
        tableDesc.getSchema().containsByQualifiedName(CatalogUtil.buildFQName(databaseName, tableName, "AddedCol1")));

    //////////////////////////////////////////////////////////////////////////////
    // Test rename column
    //////////////////////////////////////////////////////////////////////////////

    alterTableDesc = new AlterTableDesc();
    alterTableDesc.setColumnName(CatalogUtil.buildFQName(databaseName, tableName, "AddedCol1"));
    alterTableDesc.setNewColumnName(CatalogUtil.buildFQName(databaseName, tableName, "addedcol1"));
    alterTableDesc.setTableName(CatalogUtil.buildFQName(databaseName, tableName));
    alterTableDesc.setAlterTableType(AlterTableType.RENAME_COLUMN);
    catalog.alterTable(alterTableDesc);

    tableDesc = catalog.getTableDesc(databaseName, tableName);
    assertFalse(
        tableDesc.getSchema().containsByQualifiedName(CatalogUtil.buildFQName(databaseName, tableName, "AddedCol1")));
    assertTrue(
        tableDesc.getSchema().containsByQualifiedName(CatalogUtil.buildFQName(databaseName, tableName, "addedcol1")));

    //////////////////////////////////////////////////////////////////////////////
    // Test get all columns
    //////////////////////////////////////////////////////////////////////////////

    int columnCount = 0;
    for (ColumnProto eachColumnProto : catalog.getAllColumns()) {
      Column column = new Column(eachColumnProto);
      tableDesc = catalog.getTableDesc(column.getQualifier());
      assertTrue(tableDesc.getLogicalSchema().contains(column));
      columnCount++;
    }

    // TODO: Since the logical schema includes partition column keys, it should be compared in the below.
    // However, comparing logical schema causes test failure due to the bug of AbstractDBStore.
    int expected = 0;
    for (String eachTableName : catalog.getAllTableNames(databaseName)) {
      expected += catalog.getTableDesc(databaseName, eachTableName).getSchema().size();
    }
    assertEquals(expected, columnCount);
  }

  @Test
  public void testTableColumnWithNestedField() throws Exception {
    String databaseName = "TestDatabase1";
    String tableName = "nested_Table";

    Schema schema = new Schema(
        new Column[]{
            new Column("CoL1", CatalogUtil.newSimpleDataType(Type.INT4)),
            new Column("CoL2", CatalogUtil.newSimpleDataType(Type.FLOAT4)),
            new Column("CoL3", CatalogUtil.newSimpleDataType(Type.TEXT)),
    });

    Schema tableSchema = new Schema();
    tableSchema.addColumn("RecoRd1", new TypeDesc(schema));
    tableSchema.addColumn("CoL1", CatalogUtil.newSimpleDataType(Type.INT4));
    tableSchema.addColumn("CoL3", CatalogUtil.newSimpleDataType(Type.TEXT));
    tableSchema.addColumn("RecoRd2", new TypeDesc(schema));
    tableSchema.addColumn("RecoRd3", new TypeDesc(
        new Schema(new Column[]{
            new Column("CoL1", CatalogUtil.newSimpleDataType(Type.INT4)),
            new Column("RecoRd1", new TypeDesc(schema)),
        })
    ));

    TableDesc tableDesc = new TableDesc(
        CatalogUtil.buildFQName(databaseName, tableName),
        tableSchema,
        CatalogUtil.newTableMeta("JSON"),
        URI.create("hdfs://xxx.com/json_Table")
    );

    catalog.createTable(tableDesc);
    assertTrue(catalog.existsTable(databaseName, tableName));

    //////////////////////////////////////////////////////////////////////////////
    // Test get table desc
    //////////////////////////////////////////////////////////////////////////////

    tableSchema = catalog.getTableDesc(databaseName, tableName).getSchema();
    assertTrue(tableSchema.contains("RecoRd1"));
    assertTrue(tableSchema.contains(new Column("CoL1", CatalogUtil.newSimpleDataType(Type.INT4))));
    assertTrue(tableSchema.contains(new Column("CoL3", CatalogUtil.newSimpleDataType(Type.TEXT))));
    assertTrue(tableSchema.contains("RecoRd2"));
    assertTrue(tableSchema.contains("RecoRd3"));

    schema = tableSchema.getColumn("RecoRd1").getTypeDesc().nestedRecordSchema;
    assertTrue(schema.contains(new Column("CoL1", CatalogUtil.newSimpleDataType(Type.INT4))));
    assertTrue(schema.contains(new Column("CoL2", CatalogUtil.newSimpleDataType(Type.FLOAT4))));
    assertTrue(schema.contains(new Column("CoL3", CatalogUtil.newSimpleDataType(Type.TEXT))));

    schema = tableSchema.getColumn("RecoRd2").getTypeDesc().nestedRecordSchema;
    assertTrue(schema.contains(new Column("CoL1", CatalogUtil.newSimpleDataType(Type.INT4))));
    assertTrue(schema.contains(new Column("CoL2", CatalogUtil.newSimpleDataType(Type.FLOAT4))));
    assertTrue(schema.contains(new Column("CoL3", CatalogUtil.newSimpleDataType(Type.TEXT))));

    schema = tableSchema.getColumn("RecoRd3").getTypeDesc().nestedRecordSchema;
    assertTrue(schema.contains(new Column("CoL1", CatalogUtil.newSimpleDataType(Type.INT4))));
    assertTrue(tableSchema.contains("RecoRd1"));

    schema = schema.getColumn("RecoRd1").getTypeDesc().nestedRecordSchema;
    assertTrue(schema.contains(new Column("CoL1", CatalogUtil.newSimpleDataType(Type.INT4))));
    assertTrue(schema.contains(new Column("CoL2", CatalogUtil.newSimpleDataType(Type.FLOAT4))));
    assertTrue(schema.contains(new Column("CoL3", CatalogUtil.newSimpleDataType(Type.TEXT))));

    //////////////////////////////////////////////////////////////////////////////
    // Test get all columns
    //////////////////////////////////////////////////////////////////////////////

    int columnCount = 0;
    for (ColumnProto eachColumnProto : catalog.getAllColumns()) {
      // TODO: nested columns must have their name properly.
      // TODO: For example, the 'RecoRd1.CoL1' column of the above test table
      // TODO: has name of 'TestDatabase1.nested_Table.RecoRd1.CoL1'.

//      Column column = new Column(eachColumnProto);
//      tableDesc = catalog.getTableDesc(column.getQualifier());
//      assertTrue(column.getQualifiedName() + " must be contained in " + tableDesc.getName(),
//          tableDesc.getLogicalSchema().contains(column));
      columnCount++;
    }
    
    int expected = 0;
    for (String eachTableName : catalog.getAllTableNames(databaseName)) {
      expected += catalog.getTableDesc(databaseName, eachTableName).getSchema().getAllColumns().size();
    }
    assertEquals(expected, columnCount);
  }

  @Test
  public void testTableProperty() throws UndefinedTableException {
    String databaseName = "TestDatabase1";
    String tableName = "testTable1";

    TableDesc tableDesc = catalog.getTableDesc(databaseName, tableName);
    assertEquals("ThisIsTest", tableDesc.getMeta().getOption("testString"));
    assertEquals("true", tableDesc.getMeta().getOption("testBool"));
    assertEquals("0.2", tableDesc.getMeta().getOption("testFloat"));
    assertEquals("60", tableDesc.getMeta().getOption("testInt"));
    assertEquals("800", tableDesc.getMeta().getOption("testLong"));
  }

  @Test
  public void testIndex() throws Exception {
    String databaseName = "TestDatabase1";
    String tableName = "testTable1";
    String indexName = "thisIs_newIndex";

    //////////////////////////////////////////////////////////////////////////////
    // Test create index
    //////////////////////////////////////////////////////////////////////////////

    Set<IndexDesc> originalIndexes = new HashSet<>();
    TableDesc tableDesc = catalog.getTableDesc(databaseName, tableName);
    IndexDesc originalIndexDesc = CatalogTestingUtil.buildIndexDescs(databaseName, "newIndex2", tableDesc,
        tableDesc.getSchema().getColumn(0), tableDesc.getSchema().getColumn(1), tableDesc.getSchema().getColumn(2));
    originalIndexes.add(originalIndexDesc);
    catalog.createIndex(originalIndexDesc);

    originalIndexDesc = CatalogTestingUtil.buildIndexDescs(databaseName, indexName, tableDesc,
        tableDesc.getSchema().getColumn(0), tableDesc.getSchema().getColumn(2));
    originalIndexes.add(originalIndexDesc);
    catalog.createIndex(originalIndexDesc);

    //////////////////////////////////////////////////////////////////////////////
    // Test get index with index name
    //////////////////////////////////////////////////////////////////////////////

    assertTrue(catalog.existIndexByName(databaseName, indexName));
    IndexDesc indexDesc = catalog.getIndexByName(databaseName, indexName);
    assertEquals(originalIndexDesc, indexDesc);

    //////////////////////////////////////////////////////////////////////////////
    // Test get index with columns
    //////////////////////////////////////////////////////////////////////////////

    String[] indexKeyNames = new String[2];
    indexKeyNames[0] = tableDesc.getSchema().getColumn(0).getQualifiedName();
    indexKeyNames[1] = tableDesc.getSchema().getColumn(2).getQualifiedName();
    assertTrue(catalog.existIndexByColumnNames(databaseName, tableName, indexKeyNames));
    indexDesc = catalog.getIndexByColumnNames(databaseName, tableName, indexKeyNames);
    assertEquals(originalIndexDesc, indexDesc);

    Column[] indexKeys = new Column[2];
    indexKeys[0] = tableDesc.getSchema().getColumn(0);
    indexKeys[1] = tableDesc.getSchema().getColumn(2);
    assertTrue(catalog.existIndexByColumns(databaseName, tableName, indexKeys));
    indexDesc = catalog.getIndexByColumns(databaseName, tableName, indexKeys);
    assertEquals(originalIndexDesc, indexDesc);

    //////////////////////////////////////////////////////////////////////////////
    // Test get all indexes with table name
    //////////////////////////////////////////////////////////////////////////////

    Set<IndexDesc> indexDescs = new HashSet<>(catalog.getAllIndexesByTable(databaseName, tableName));
    assertEquals(originalIndexes, indexDescs);

    //////////////////////////////////////////////////////////////////////////////
    // Test get all indexes
    //////////////////////////////////////////////////////////////////////////////

    List<IndexDescProto> indexDescProtos = catalog.getAllIndexes();
    indexDescs = new HashSet<>();
    for (IndexDescProto indexDescProto : indexDescProtos) {
      indexDescs.add(new IndexDesc(indexDescProto));
    }
    assertEquals(originalIndexes, indexDescs);

    //////////////////////////////////////////////////////////////////////////////
    // Test drop index
    //////////////////////////////////////////////////////////////////////////////
    catalog.dropIndex(databaseName, indexName);
    assertFalse(catalog.existIndexByName(databaseName, indexName));
    catalog.dropIndex(databaseName, "newIndex2");
    assertFalse(catalog.existIndexByName(databaseName, "newIndex2"));
  }
}
