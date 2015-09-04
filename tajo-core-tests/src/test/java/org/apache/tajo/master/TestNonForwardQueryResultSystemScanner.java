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
import org.junit.Test;

import java.sql.ResultSet;

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
    String simpleTableName = "information_schema_test_table";
    int totalCount = 1000;

    for (int i = 0; i < totalCount; i++) {
      executeString("ALTER TABLE information_schema_test_table ADD PARTITION (col3 = " + i
        + ", col4 = " + (i + 1) + ")");
    }

    resultSet = executeString("SELECT tid, table_name FROM INFORMATION_SCHEMA.TABLES "
    + " WHERE TABLE_NAME = '" + simpleTableName + "'");
    int tid = -1;
    while (resultSet.next()) {
      tid = resultSet.getInt("tid");
    }
    resultSet.close();
    assertTrue(tid > -1);

    int resultPartitionCount = 0;
    resultSet = executeString("SELECT count(*) FROM INFORMATION_SCHEMA.PARTITIONS "
      + " WHERE tid = " + tid);
    while (resultSet.next()) {
      resultPartitionCount = resultSet.getInt(1);
    }
    resultSet.close();
    assertEquals(totalCount, resultPartitionCount);

    // Setting select statement for getting partition keys.
    String selectPartitionKeys = "SELECT count(*) FROM INFORMATION_SCHEMA.PARTITION_KEYS WHERE partition_id IN (";

    resultSet = executeString("SELECT partition_id FROM INFORMATION_SCHEMA.PARTITIONS WHERE tid = " + tid);
    int i = 0;
    while (resultSet.next()) {
      if (i > 0) {
        selectPartitionKeys += ",";
      }
      selectPartitionKeys +=  resultSet.getInt(1);
      i++;
    }
    selectPartitionKeys += ")";
    resultSet.close();

    resultSet = executeString(selectPartitionKeys);
    int resultPartitionKeyCount = 0;
    while (resultSet.next()) {
      resultPartitionKeyCount = resultSet.getInt(1);
    }
    assertEquals(totalCount * 2, resultPartitionKeyCount);

    executeDDL("drop_partitioned_table.sql", null);
  }
}
