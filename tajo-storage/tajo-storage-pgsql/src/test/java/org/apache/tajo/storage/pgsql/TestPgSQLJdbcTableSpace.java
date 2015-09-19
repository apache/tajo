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

package org.apache.tajo.storage.pgsql;

import net.minidev.json.JSONObject;
import org.apache.tajo.catalog.MetadataProvider;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.jdbc.JdbcTablespace;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestPgSQLJdbcTableSpace {
  static String jdbcUrl = PgSQLTestServer.getInstance().getJdbcUrlForAdmin();

  @Test(timeout = 1000)
  public void testTablespaceHandler() throws Exception {
    assertTrue((TablespaceManager.getByName("pgsql_cluster").get()) instanceof PgSQLTablespace);
    assertEquals("pgsql_cluster", (TablespaceManager.getByName("pgsql_cluster").get().getName()));

    assertTrue((TablespaceManager.get(jdbcUrl).get()) instanceof PgSQLTablespace);
    assertTrue((TablespaceManager.get(jdbcUrl + "&table=tb1").get()) instanceof PgSQLTablespace);

    assertEquals(jdbcUrl, TablespaceManager.get(jdbcUrl).get().getUri().toASCIIString());
    assertTrue(TablespaceManager.get(jdbcUrl).get().getMetadataProvider() instanceof PgSQLMetadataProvider);
  }

  @Test(timeout = 1000, expected = TajoRuntimeException.class)
  public void testCreateTable() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName("pgsql_cluster").get();
    space.createTable(null, false);
  }

  @Test(timeout = 1000, expected = TajoRuntimeException.class)
  public void testDropTable() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName("pgsql_cluster").get();
    space.purgeTable(null);
  }

  @Test(timeout = 1000)
  public void testGetSplits() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName("pgsql_cluster").get();
    MetadataProvider provider = space.getMetadataProvider();
    TableDesc table = provider.getTableDesc(null, "lineitem");
    List<Fragment> fragments = space.getSplits("lineitem", table, null);
    assertNotNull(fragments);
    assertEquals(1, fragments.size());
  }

  @Test
  public void testConnProperties() throws Exception {
    Map<String, String> connProperties = new HashMap<>();
    connProperties.put("user", "postgres");
    connProperties.put("password", "");

    String uri = PgSQLTestServer.getInstance().getJdbcUrl().split("\\?")[0];
    Tablespace space = new PgSQLTablespace("t1", URI.create(uri), getJsonTablespace(connProperties));
    try {
      space.init(new TajoConf());
    } finally {
      space.close();
    }
  }

  @Test
  public void testConnPropertiesNegative() throws Exception {
    Map<String, String> connProperties = new HashMap<>();
    connProperties.put("user", "postgresX");
    connProperties.put("password", "");

    String uri = PgSQLTestServer.getInstance().getJdbcUrl().split("\\?")[0];
    Tablespace space = new PgSQLTablespace("t1", URI.create(uri), getJsonTablespace(connProperties));
    try {
      space.init(new TajoConf());
      fail("Must be failed");
    } catch (IOException ioe) {
      assertTrue(ioe.getCause() instanceof PSQLException);
    } finally {
      space.close();
    }
  }

  public static JSONObject getJsonTablespace(Map<String, String> connProperties)
      throws IOException {
    String uri = PgSQLTestServer.getInstance().getJdbcUrl().split("\\?")[0];

    JSONObject configElements = new JSONObject();
    configElements.put(JdbcTablespace.CONFIG_KEY_MAPPED_DATABASE, PgSQLTestServer.DATABASE_NAME);

    JSONObject connPropertiesJson = new JSONObject();
    for (Map.Entry<String, String> entry : connProperties.entrySet()) {
      connPropertiesJson.put(entry.getKey(), entry.getValue());
    }
    configElements.put(JdbcTablespace.CONFIG_KEY_CONN_PROPERTIES, connPropertiesJson);

    return configElements;
  }
}
