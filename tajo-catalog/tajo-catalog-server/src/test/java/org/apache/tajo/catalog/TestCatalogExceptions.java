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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceCommand;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceType;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.catalog.proto.CatalogProtos.TableStatsProto;
import org.apache.tajo.catalog.proto.CatalogProtos.UpdateTableStatsProto;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.*;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;

public class TestCatalogExceptions {

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
  public void testCreateTablespaceWithWrongUri() throws Exception {
    // TODO: currently, wrong uri does not occur any exception.
    catalog.createTablespace("wrong", "hdfs:");
  }

  @Test(expected = DuplicateTablespaceException.class)
  public void testCreateDuplicateTablespace() throws Exception {
    catalog.createTablespace("space1", "hdfs://xxx.com/warehouse");
  }

  @Test(expected = UndefinedTablespaceException.class)
  public void testDropUndefinedTablespace() throws Exception {
    catalog.dropTablespace("undefined");
  }

  @Test(expected = InsufficientPrivilegeException.class)
  public void testDropDefaultTablespace() throws Exception {
    catalog.dropTablespace(TajoConstants.DEFAULT_TABLESPACE_NAME);
  }

  @Test(expected = TajoInternalError.class)
  public void testAlterTablespaceWithWrongUri() throws Exception {
    catalog.alterTablespace(AlterTablespaceProto.newBuilder().
        setSpaceName("space1").
        addCommand(
            AlterTablespaceCommand.newBuilder().
                setType(AlterTablespaceType.LOCATION).
                setLocation("hdfs:")).build());
  }

  @Test(expected = UndefinedTablespaceException.class)
  public void testAlterUndefinedTablespace() throws Exception {
    catalog.alterTablespace(AlterTablespaceProto.newBuilder().
        setSpaceName("undefined").
        addCommand(
            AlterTablespaceCommand.newBuilder().
                setType(AlterTablespaceType.LOCATION).
                setLocation("hdfs://zzz.com/warehouse")).build());
  }

  @Test(expected = DuplicateDatabaseException.class)
  public void testCreateDuplicateDatabase() throws Exception {
    catalog.createDatabase("TestDatabase1", "space1");
  }

  @Test(expected = UndefinedDatabaseException.class)
  public void testDropUndefinedDatabase() throws Exception {
    catalog.dropDatabase("undefined");
  }

  @Test(expected = InsufficientPrivilegeException.class)
  public void testDropDefaultDatabase() throws Exception {
    catalog.dropDatabase(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test()
  public void testCreateTableWithWrongUri() throws Exception {
    // TODO: currently, wrong uri does not occur any exception.
    String tableName = "wrongUri";
    Schema schema = new Schema();
    schema.addColumn(CatalogUtil.buildFQName(tableName, "Column"), Type.BLOB);
    schema.addColumn(CatalogUtil.buildFQName(tableName, "column"), Type.INT4);
    schema.addColumn(CatalogUtil.buildFQName(tableName, "cOlumn"), Type.INT8);
    Path path = new Path(CommonTestingUtil.getTestDir(), tableName);
    catalog.createTable(
        new TableDesc(
            CatalogUtil.buildFQName("TestDatabase1", tableName),
            schema,
            new TableMeta("TEXT", new KeyValueSet()),
            path.toUri(), true));
  }

  @Test(expected = DuplicateTableException.class)
  public void testCreateDuplicateTable() throws Exception {
    catalog.createTable(CatalogTestingUtil.buildTableDesc("TestDatabase1", "TestTable1",
        CommonTestingUtil.getTestDir().toString()));
  }

  @Test(expected = UndefinedDatabaseException.class)
  public void dropTableOfUndefinedDatabase() throws Exception {
    catalog.dropTable(CatalogUtil.buildFQName("undefined", "testPartition1"));
  }

  @Test(expected = UndefinedTableException.class)
  public void dropUndefinedTable() throws Exception {
    catalog.dropTable(CatalogUtil.buildFQName("TestDatabase1", "undefined"));
  }

  @Test(expected = UndefinedTableException.class)
  public void testUpdateTableStatsOfUndefinedTable() throws Exception {
    catalog.updateTableStats(
        UpdateTableStatsProto.newBuilder().
            setTableName(CatalogUtil.buildFQName("TestDatabase1", "undefined")).
            setStats(
                TableStatsProto.newBuilder().
                    setNumRows(0).
                    setNumBytes(0).
                    build()).
            build());
  }

  @Test
  public void testAddPartitionWithWrongUri() throws Exception {
    // TODO: currently, wrong uri does not occur any exception.
    String partitionName = "DaTe=/=AaA";
    PartitionDesc partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    catalog.alterTable(alterTableDesc);
  }

  @Test(expected = DuplicatePartitionException.class)
  public void testAddDuplicatePartition() throws Exception {
    String partitionName = "DaTe=bBb/dAtE=AaA";
    PartitionDesc partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    catalog.alterTable(alterTableDesc);

    partitionName = "DaTe=bBb/dAtE=AaA";
    partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    catalog.alterTable(alterTableDesc);
  }

  @Test(expected = UndefinedTableException.class)
  public void testAddPartitionToUndefinedTable() throws Exception {
    String partitionName = "DaTe=bBb/dAtE=AaA";
    PartitionDesc partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "undefined"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    catalog.alterTable(alterTableDesc);
  }

  @Test(expected = UndefinedPartitionException.class)
  public void testDropUndefinedPartition() throws Exception {
    String partitionName = "DaTe=undefined/dAtE=undefined";
    PartitionDesc partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.DROP_PARTITION);
    catalog.alterTable(alterTableDesc);
  }

  @Test(expected = UndefinedTableException.class)
  public void testRenameUndefinedTable() throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setAlterTableType(AlterTableType.RENAME_TABLE);
    alterTableDesc.setNewTableName(CatalogUtil.buildFQName("TestDatabase1", "renamed_table"));
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "undefined"));
    catalog.alterTable(alterTableDesc);
  }

  @Test(expected = UndefinedTableException.class)
  public void testRenameColumnOfUndefinedTable() throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setColumnName(CatalogUtil.buildFQName("TestDatabase1", "undefined", "AddedCol1"));
    alterTableDesc.setNewColumnName(CatalogUtil.buildFQName("TestDatabase1", "undefined", "addedcol1"));
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "undefined"));
    alterTableDesc.setAlterTableType(AlterTableType.RENAME_COLUMN);
    catalog.alterTable(alterTableDesc);
  }

  @Test(expected = UndefinedColumnException.class)
  public void testRenameUndefinedColumn() throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setColumnName(CatalogUtil.buildFQName("TestDatabase1", "TestTable1", "undefined"));
    alterTableDesc.setNewColumnName(CatalogUtil.buildFQName("TestDatabase1", "TestTable1", "undefined"));
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestTable1"));
    alterTableDesc.setAlterTableType(AlterTableType.RENAME_COLUMN);
    catalog.alterTable(alterTableDesc);
  }

  @Test(expected = DuplicateColumnException.class)
  public void testAddDuplicateColumn() throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setAddColumn(new Column(CatalogUtil.buildFQName("TestDatabase1", "TestTable1", "cOlumn"),
        CatalogUtil.newSimpleDataType(Type.BLOB)));
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestTable1"));
    alterTableDesc.setAlterTableType(AlterTableType.ADD_COLUMN);
    catalog.alterTable(alterTableDesc);
  }

  @Test(expected = DuplicateIndexException.class)
  public void testCreateDuplicateIndex() throws Exception {
    TableDesc tableDesc = catalog.getTableDesc("TestDatabase1", "testTable1");
    SortSpec[] colSpecs = new SortSpec[1];
    colSpecs[0] = new SortSpec(tableDesc.getSchema().getColumn(0), true, false);
    catalog.createIndex(
        new IndexDesc("TestDatabase1", "testTable1",
            "new_index", new URI("idx_test"), colSpecs,
            IndexMethod.TWO_LEVEL_BIN_TREE, true, true, tableDesc.getSchema()));

    tableDesc = catalog.getTableDesc("TestDatabase1", "testTable1");
    colSpecs = new SortSpec[1];
    colSpecs[0] = new SortSpec(tableDesc.getSchema().getColumn(0), true, false);
    catalog.createIndex(
        new IndexDesc("TestDatabase1", "testTable1",
            "new_index", new URI("idx_test"), colSpecs,
            IndexMethod.TWO_LEVEL_BIN_TREE, true, true, tableDesc.getSchema()));
  }

  @Test(expected = UndefinedDatabaseException.class)
  public void testDropIndexOfUndefinedDatabase() throws Exception {
    catalog.dropIndex("undefined", "undefined");
  }

  @Test(expected = UndefinedIndexException.class)
  public void testDropUndefinedIndex() throws Exception {
    catalog.dropIndex("TestDatabase1", "undefined");
  }
}
