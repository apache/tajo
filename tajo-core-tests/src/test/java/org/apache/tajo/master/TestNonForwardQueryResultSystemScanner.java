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

package org.apache.tajo.master;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.util.TUtil;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestNonForwardQueryResultSystemScanner extends QueryTestCaseBase {
  @Test
  public void testGetNextRowsForAggregateFunction() throws Exception {
    assertQueryStr("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
        "WHERE TABLE_NAME = 'lineitem' OR TABLE_NAME = 'nation' OR TABLE_NAME = 'customer'");
  }

  @Test
  public void testGetNextRowsForTable() throws Exception {
    assertQueryStr("SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES " +
        "WHERE TABLE_NAME = 'lineitem' OR TABLE_NAME = 'nation' OR TABLE_NAME = 'customer'");
  }

  @Test
  public void testGetClusterDetails() throws Exception {
    assertQueryStr("SELECT TYPE FROM INFORMATION_SCHEMA.CLUSTER");
  }

  @Test
  public final void testGetInformationSchema() throws Exception {
    executeDDL("create_partitioned_table.sql", null);

    String simpleTableName = "information_schema_test_table";
    String tableName = CatalogUtil.buildFQName(getCurrentDatabase(), simpleTableName);
    assertTrue(catalog.existsTable(tableName));

    TableDesc retrieved = catalog.getTableDesc(tableName);

    executeDDL("alter_table_add_partition1.sql", null);
    executeDDL("alter_table_add_partition2.sql", null);

    List<CatalogProtos.DatabaseProto> allDatabases = catalog.getAllDatabases();
    int dbId = -1;
    for (CatalogProtos.DatabaseProto database : allDatabases) {
      if (database.getName().equals(getCurrentDatabase())) {
        dbId = database.getId();
      }
    }
    assertNotEquals(dbId, -1);

    int tableId = -1;
    List<CatalogProtos.TableDescriptorProto>  allTables = catalog.getAllTables();
    for(CatalogProtos.TableDescriptorProto table : allTables) {
      if (table.getDbId() == dbId && table.getName().equals(simpleTableName)) {
        tableId = table.getTid();
      }
    }
    assertNotEquals(tableId, -1);

    List<CatalogProtos.PartitionDescProto> allPartitions = catalog.getAllPartitions();
    List<CatalogProtos.PartitionDescProto> resultPartitions = TUtil.newList();

    int partitionId = 0;
    for (CatalogProtos.PartitionDescProto partition : allPartitions) {
      if (partition.getTid() == tableId
        && partition.getPartitionName().equals("col3=1/col4=2")
        && partition.getPath().equals(retrieved.getUri().toString() + "/col3=1/col4=2")
        ){
        resultPartitions.add(partition);
        partitionId = partition.getPartitionId();
      }
    }
    assertEquals(resultPartitions.size(), 1);
    assertEquals(resultPartitions.get(0).getPartitionName(), "col3=1/col4=2");

    List<CatalogProtos.PartitionKeyProto> tablePartitionKeys = catalog.getAllPartitionKeys();
    List<CatalogProtos.PartitionKeyProto> resultPartitionKeys = TUtil.newList();

    for (CatalogProtos.PartitionKeyProto partitionKey: tablePartitionKeys) {
      if (partitionKey.getPartitionId() == partitionId
        && (partitionKey.getColumnName().equals("col3") && partitionKey.getPartitionValue().equals("1")
        || partitionKey.getColumnName().equals("col4") && partitionKey.getPartitionValue().equals("2"))) {
        resultPartitionKeys.add(partitionKey);
      }
    }
    assertEquals(resultPartitionKeys.size(), 2);
    assertEquals(resultPartitionKeys.get(0).getColumnName(), "col3");
    assertEquals(resultPartitionKeys.get(0).getPartitionValue(), "1");
    assertEquals(resultPartitionKeys.get(1).getColumnName(), "col4");
    assertEquals(resultPartitionKeys.get(1).getPartitionValue(), "2");

    ResultSet resultSet = executeString("SELECT partition_name FROM INFORMATION_SCHEMA.PARTITIONS "
      + " WHERE partition_id = " + partitionId);

    String actualResult = resultSetToString(resultSet);
    String expectedResult = "partition_name\n" +
      "-------------------------------\n" +
      "col3=1/col4=2\n";
    assertEquals(expectedResult, actualResult);

    resultSet = executeString("SELECT column_name,partition_value FROM INFORMATION_SCHEMA.PARTITION_KEYS" +
      " WHERE partition_id = " + partitionId);

    actualResult = resultSetToString(resultSet);
    expectedResult = "column_name,partition_value\n" +
      "-------------------------------\n" +
      "col3,1\n" +
      "col4,2\n";
    assertEquals(expectedResult, actualResult);

    executeDDL("drop_partitioned_table.sql", null);
  }
}
