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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.util.TUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestAlterTable extends QueryTestCaseBase {

  @Test
  public final void testAlterTableName() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1.tbl", "ABC");
    assertTableExists(createdNames.get(0));
    executeDDL("alter_table_rename_table_ddl.sql", null);
    assertTableExists("DEF");
  }

  @Test
  public final void testAlterTableColumnName() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1.tbl", "XYZ");
    executeDDL("alter_table_rename_column_ddl.sql", null);
    assertColumnExists(createdNames.get(0), "renum");
  }

  @Test
  public final void testAlterTableAddNewColumn() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1.tbl", "EFG");
    executeDDL("alter_table_add_new_column_ddl.sql", null);
    assertColumnExists(createdNames.get(0),"cool");
  }

  @Test
  public final void testAlterTableSetProperty() throws Exception {
    executeDDL("table2_ddl.sql", "table2.tbl", "ALTX");
    ResultSet before_res = executeQuery();
    assertResultSet(before_res, "before_set_property_delimiter.result");
    cleanupQuery(before_res);

    executeDDL("alter_table_set_property_delimiter.sql", null);

    ResultSet after_res = executeQuery();
    assertResultSet(after_res, "after_set_property_delimiter.result");
    cleanupQuery(after_res);
  }

  @Test
  public final void testAlterTableAddPartition() throws Exception {
    executeDDL("create_partitioned_table.sql", null);

    String tableName = CatalogUtil.buildFQName("TestAlterTable", "partitioned_table");
    assertTrue(catalog.existsTable(tableName));

    TableDesc retrieved = catalog.getTableDesc(tableName);
    assertEquals(retrieved.getName(), tableName);
    assertEquals(retrieved.getPartitionMethod().getPartitionType(), CatalogProtos.PartitionType.COLUMN);
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getAllColumns().size(), 2);
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getColumn(0).getSimpleName(), "col3");
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getColumn(1).getSimpleName(), "col4");

    executeDDL("alter_table_add_partition1.sql", null);
    executeDDL("alter_table_add_partition2.sql", null);

    List<CatalogProtos.PartitionDescProto> partitions = catalog.getPartitions("TestAlterTable", "partitioned_table");
    assertNotNull(partitions);
    assertEquals(partitions.size(), 1);
    assertEquals(partitions.get(0).getPartitionName(), "col3=1/col4=2");
    assertEquals(partitions.get(0).getPartitionKeysList().get(0).getColumnName(), "col3");
    assertEquals(partitions.get(0).getPartitionKeysList().get(0).getPartitionValue(), "1");
    assertEquals(partitions.get(0).getPartitionKeysList().get(1).getColumnName(), "col4");
    assertEquals(partitions.get(0).getPartitionKeysList().get(1).getPartitionValue(), "2");

    assertNotNull(partitions.get(0).getPath());
    Path partitionPath = new Path(partitions.get(0).getPath());
    FileSystem fs = partitionPath.getFileSystem(conf);
    assertTrue(fs.exists(partitionPath));
    assertTrue(partitionPath.toString().indexOf("col3=1/col4=2") > 0);

    List<CatalogProtos.DatabaseProto> allDatabases = catalog.getAllDatabases();
    int dbId = -1;
    for (CatalogProtos.DatabaseProto database : allDatabases) {
      if (database.getName().equals("TestAlterTable")) {
        dbId = database.getId();
      }
    }
    assertNotEquals(dbId, -1);

    int tableId = -1;
    List<CatalogProtos.TableDescriptorProto>  allTables = catalog.getAllTables();
    for(CatalogProtos.TableDescriptorProto table : allTables) {
      if (table.getDbId() == dbId && table.getName().equals("partitioned_table")) {
        tableId = table.getTid();
      }
    }
    assertNotEquals(tableId, -1);

    List<CatalogProtos.TablePartitionProto> allPartitions = catalog.getAllPartitions();
    List<CatalogProtos.TablePartitionProto> resultPartitions = TUtil.newList();

    for (CatalogProtos.TablePartitionProto partition : allPartitions) {
      if (partition.getTid() == tableId) {
        resultPartitions.add(partition);
      }
    }
    assertEquals(resultPartitions.size(), 1);
    assertEquals(resultPartitions.get(0).getPartitionName(), "col3=1/col4=2");

    List<CatalogProtos.TablePartitionKeysProto> tablePartitionKeys = catalog.getAllPartitionKeys();
    assertEquals(tablePartitionKeys.size(), 2);
    assertEquals(tablePartitionKeys.get(0).getColumnName(), "col3");
    assertEquals(tablePartitionKeys.get(0).getPartitionValue(), "1");
    assertEquals(tablePartitionKeys.get(1).getColumnName(), "col4");
    assertEquals(tablePartitionKeys.get(1).getPartitionValue(), "2");

    ResultSet resultSet = executeString("SELECT partition_name FROM INFORMATION_SCHEMA.PARTITIONS");
    String actualResult = resultSetToString(resultSet);
    String expectedResult = "partition_name\n" +
      "-------------------------------\n" +
      "col3=1/col4=2\n";
    assertEquals(expectedResult, actualResult);

    resultSet = executeString("SELECT * FROM INFORMATION_SCHEMA.PARTITION_KEYS");
    actualResult = resultSetToString(resultSet);
    expectedResult = "partition_id,column_name,partition_value\n" +
      "-------------------------------\n" +
      "0,col3,1\n" +
      "0,col4,2\n";
    assertEquals(expectedResult, actualResult);

    executeDDL("alter_table_drop_partition1.sql", null);
    executeDDL("alter_table_drop_partition2.sql", null);

    partitions = catalog.getPartitions("TestAlterTable", "partitioned_table");
    assertNotNull(partitions);
    assertEquals(partitions.size(), 0);
    assertFalse(fs.exists(partitionPath));
  }
}
