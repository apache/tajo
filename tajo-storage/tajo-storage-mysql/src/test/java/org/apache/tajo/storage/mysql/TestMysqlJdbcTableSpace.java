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

import com.google.common.collect.ImmutableSet;
import io.airlift.testing.mysql.TestingMySqlServer;
import org.apache.tajo.storage.TablespaceManager;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMysqlJdbcTableSpace {
  static String jdbcUrl = EmbedMySQLServer.getInstance().getJdbcUrl();


  @BeforeClass
  public static void setUp() throws Exception {
  }

  @Test
  public void testGeneral() {
    TestingMySqlServer server = EmbedMySQLServer.getInstance().getServer();
    assertTrue(server.isRunning());
    assertTrue(server.isReadyForConnections());
    assertEquals(server.getMySqlVersion(), "5.5.9");
    assertEquals(server.getDatabases(), ImmutableSet.of("tpch"));
    assertEquals(server.getUser(), "testuser");
    assertEquals(server.getPassword(), "testpass");
    assertEquals(server.getJdbcUrl().substring(0, 5), "jdbc:");
    assertEquals(server.getPort(), URI.create(server.getJdbcUrl().substring(5)).getPort());
  }

  @Test
  public void testTablespace() throws Exception {
    assertTrue((TablespaceManager.getByName("mysql_cluster").get()) instanceof MySQLTablespace);
    assertEquals("mysql_cluster", (TablespaceManager.getByName("mysql_cluster").get().getName()));

    assertTrue((TablespaceManager.get(jdbcUrl).get()) instanceof MySQLTablespace);
    assertTrue((TablespaceManager.get(jdbcUrl + "&table=tb1").get()) instanceof MySQLTablespace);

    assertEquals(jdbcUrl, TablespaceManager.get(jdbcUrl).get().getUri().toASCIIString());
    assertTrue(TablespaceManager.get(jdbcUrl).get().getMetadataProvider() instanceof MySQLMetadataProvider);
  }
}
