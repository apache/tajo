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

package org.apache.tajo.storage.mysql;

import net.minidev.json.JSONObject;
import org.apache.tajo.catalog.MetadataProvider;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UndefinedTablespaceException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.jdbc.JdbcTablespace;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

public class TestMySQLSQLJdbcTableSpace {
  private static String jdbcUrl = MySQLTestServer.getInstance().getJdbcUrl();

  @Test(timeout = 1000)
  public void testTablespaceHandler() throws Exception {
    assertTrue((TablespaceManager.getByName("mysql_cluster")) instanceof MySQLTablespace);
    assertEquals("mysql_cluster", (TablespaceManager.getByName("mysql_cluster").getName()));

    assertTrue((TablespaceManager.get(jdbcUrl)) instanceof MySQLTablespace);
    assertTrue((TablespaceManager.get(jdbcUrl + "&table=tb1")) instanceof MySQLTablespace);

    assertEquals(jdbcUrl, TablespaceManager.get(jdbcUrl).getUri().toASCIIString());
    assertTrue(TablespaceManager.get(jdbcUrl).getMetadataProvider() instanceof MySQLMetadataProvider);
  }

  @Test(timeout = 1000, expected = TajoRuntimeException.class)
  public void testCreateTable() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName("mysql_cluster");
    space.createTable(null, false);
  }

  @Test(timeout = 1000, expected = TajoRuntimeException.class)
  public void testDropTable() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName("mysql_cluster");
    space.purgeTable(null);
  }

  @Test(timeout = 1000)
  public void testGetSplits() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName("mysql_cluster");
    MetadataProvider provider = space.getMetadataProvider();
    TableDesc table = provider.getTableDesc(null, "lineitem");
    List<Fragment> fragments = space.getSplits("lineitem", table, false, null);
    assertNotNull(fragments);
    assertEquals(1, fragments.size());
  }

  @Test
  public void testConnProperties() throws Exception {
    Map<String, String> connProperties = new HashMap<>();
    connProperties.put("user", "mysql");
    connProperties.put("password", "mysql");

    String uri = MySQLTestServer.getInstance().getJdbcUrl().split("\\?")[0];
    Tablespace space = new MySQLTablespace("t1", URI.create(uri), getJsonTablespace(connProperties));
    try {
      space.init(new TajoConf());
    } finally {
      space.close();
    }
  }

  @Test
  public void testConnPropertiesNegative() throws Exception {
    Map<String, String> connProperties = new HashMap<>();
    connProperties.put("user", "mysqlX");
    connProperties.put("password", "");

    String uri = MySQLTestServer.getInstance().getJdbcUrl().split("\\?")[0];
    Tablespace space = new MySQLTablespace("t1", URI.create(uri), getJsonTablespace(connProperties));
    try {
      space.init(new TajoConf());
      fail("Access denied for user 'mysqlX'@'localhost' (using password: NO)");
    } catch (IOException ioe) {
      assertTrue(ioe.getCause() instanceof SQLException);
    } finally {
      space.close();
    }
  }

  public static JSONObject getJsonTablespace(Map<String, String> connProperties)
      throws IOException {
    JSONObject configElements = new JSONObject();
    configElements.put(JdbcTablespace.CONFIG_KEY_MAPPED_DATABASE, MySQLTestServer.DATABASE_NAME);

    JSONObject connPropertiesJson = new JSONObject();
    for (Map.Entry<String, String> entry : connProperties.entrySet()) {
      connPropertiesJson.put(entry.getKey(), entry.getValue());
    }
    configElements.put(JdbcTablespace.CONFIG_KEY_CONN_PROPERTIES, connPropertiesJson);

    return configElements;
  }

  @Test
  public void testTemporaryTablespace() {
    Optional<Tablespace> ts = TablespaceManager.removeTablespaceForTest("mysql_cluster");
    assertTrue(ts.isPresent());

    Tablespace tempTs = TablespaceManager.get(jdbcUrl);
    assertNotNull(tempTs);

    TablespaceManager.addTableSpaceForTest(ts.get());
  }

  @Test
  public void testGetTableVolume() throws UndefinedTablespaceException, UnsupportedException {
    Tablespace space = TablespaceManager.getByName("mysql_cluster");
    MetadataProvider provider = space.getMetadataProvider();
    TableDesc table = provider.getTableDesc(null, "lineitem");
    long volume = space.getTableVolume(table, Optional.empty());
    assertEquals(16384L, volume);
  }
}
