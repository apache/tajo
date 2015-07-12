/*
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

import org.apache.tajo.QueryTestCaseBase;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;

public class TestNetTypes extends QueryTestCaseBase {

  @Before
  public final void setUp() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      executeDDL("table1_ddl.sql", "table1");
      executeDDL("table2_ddl.sql", "table2");
    }
  }

  @Test
  public final void testSelect() throws Exception {
    // Skip all tests when HiveCatalogStore is used.
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      // select name, addr from table1;
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    }
  }

  @Test
  public final void testGroupby() throws Exception {
    // Skip all tests when HiveCatalogStore is used.
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      // select name, addr, count(1) from table1 group by name, addr;
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    }
  }

  @Test
  public final void testGroupby2() throws Exception {
    // Skip all tests when HiveCatalogStore is used.
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      // select addr, count(*) from table1 group by addr;
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    }
  }

  @Test
  public final void testSort() throws Exception {
    // Skip all tests when HiveCatalogStore is used.
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      // select * from table1 order by addr;
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    }
  }

  @Test
  public final void testSort2() throws Exception {
    // Skip all tests when HiveCatalogStore is used.
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      // select addr from table2 order by addr;
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    }
  }

  @Test
  public final void testJoin() throws Exception {
    // Skip all tests when HiveCatalogStore is used.
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      // select * from table1 as t1, table2 as t2 where t1.addr = t2.addr;
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    }
  }
}
