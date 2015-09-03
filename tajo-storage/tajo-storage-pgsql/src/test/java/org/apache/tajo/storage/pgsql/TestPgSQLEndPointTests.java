/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.pgsql;

import com.google.common.collect.Sets;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.exception.UndefinedTableException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPgSQLEndPointTests extends QueryTestCaseBase {
  private static final String jdbcUrl = PgSQLTestServer.getInstance().getJdbcUrlForAdmin();
  private static TajoClient client;


  @BeforeClass
  public static void setUp() throws Exception {
    QueryTestCaseBase.testingCluster.getMaster().refresh();
    client = QueryTestCaseBase.testingCluster.newTajoClient();
  }

  @AfterClass
  public static void tearDown() {
    client.close();
  }

  @Test(timeout = 1000)
  public void testGetAllDatabaseNames() {
    Set<String> retrieved = Sets.newHashSet(client.getAllDatabaseNames());
    assertTrue(retrieved.contains(PgSQLTestServer.DATABASE_NAME));
  }

  @Test(timeout = 1000)
  public void testGetTableList() {
    final Set<String> expected = Sets.newHashSet(PgSQLTestServer.TPCH_TABLES);
    expected.add("datetime_types");
    final Set<String> retrieved = Sets.newHashSet(client.getTableList("tpch"));

    assertEquals(expected, retrieved);
  }

  @Test(timeout = 1000)
  public void testGetTable() throws UndefinedTableException {
    for (String tableName: PgSQLTestServer.TPCH_TABLES) {
      TableDesc retrieved = client.getTableDesc(PgSQLTestServer.DATABASE_NAME + "." + tableName);
      assertEquals(PgSQLTestServer.DATABASE_NAME + "." + tableName, retrieved.getName());
      assertEquals(jdbcUrl + "&table=" + tableName, retrieved.getUri().toASCIIString());
    }
  }
}
