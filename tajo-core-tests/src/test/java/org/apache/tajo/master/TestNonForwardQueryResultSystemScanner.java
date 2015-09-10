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
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.CatalogConstants.INFORMATION_SCHEMA_TABLES;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
  public void testGetTables() throws Exception {
    List<String> tables = client.getTableList(CatalogConstants.INFORMATION_SCHEMA_DB_NAME);
    assertEquals(10, tables.size());
    assertTrue(tables.contains(INFORMATION_SCHEMA_TABLES.TABLESPACE.name().toLowerCase()));
    assertTrue(tables.contains(INFORMATION_SCHEMA_TABLES.DATABASES.name().toLowerCase()));
    assertTrue(tables.contains(INFORMATION_SCHEMA_TABLES.TABLES.name().toLowerCase()));
    assertTrue(tables.contains(INFORMATION_SCHEMA_TABLES.COLUMNS.name().toLowerCase()));
    assertTrue(tables.contains(INFORMATION_SCHEMA_TABLES.INDEXES.name().toLowerCase()));
    assertTrue(tables.contains(INFORMATION_SCHEMA_TABLES.TABLE_OPTIONS.name().toLowerCase()));
    assertTrue(tables.contains(INFORMATION_SCHEMA_TABLES.TABLE_STATS.name().toLowerCase()));
    assertTrue(tables.contains(INFORMATION_SCHEMA_TABLES.PARTITIONS.name().toLowerCase()));
    assertTrue(tables.contains(INFORMATION_SCHEMA_TABLES.CLUSTER.name().toLowerCase()));
    assertTrue(tables.contains(INFORMATION_SCHEMA_TABLES.SESSION.name().toLowerCase()));
  }
}
