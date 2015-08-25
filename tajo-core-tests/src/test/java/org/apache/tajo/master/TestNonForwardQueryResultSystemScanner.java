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

    ResultSet resultSet = null;
    String actualResult, expectedResult;
    String simpleTableName = "information_schema_test_table";

    executeDDL("alter_table_add_partition1.sql", null);
    executeDDL("alter_table_add_partition2.sql", null);

    resultSet = executeString("SELECT tid, table_name FROM INFORMATION_SCHEMA.TABLES "
    + " WHERE TABLE_NAME = '" + simpleTableName + "'");
    int tid = -1;
    while (resultSet.next()) {
      tid = resultSet.getInt("tid");
    }
    resultSet.close();
    assertTrue(tid > -1);

    int partitionId = -1;
    resultSet = executeString("SELECT partition_id, partition_name FROM INFORMATION_SCHEMA.PARTITIONS "
      + " WHERE tid = " + tid);
    while (resultSet.next()) {
      partitionId = resultSet.getInt("partition_id");
    }
    resultSet.close();
    assertTrue(partitionId > -1);

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
