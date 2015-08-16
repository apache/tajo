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

package org.apache.tajo.storage.jdbc;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.testing.mysql.TestingMySqlServer;
import org.apache.tajo.catalog.MetadataProvider;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.mysql.MySQLTablespace;
import org.apache.tajo.util.FileUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMysqlJdbcTableSpace extends JdbcTablespaceTestBase {

  static TestingMySqlServer server;

  public TestMysqlJdbcTableSpace() throws Exception {
    server = new TestingMySqlServer("testuser", "testpass",
        "tpch"
    );

    MySQLTablespace mysqlTablespace = new MySQLTablespace("mysql_cluster", URI.create(server.getJdbcUrl()), null);
    mysqlTablespace.init(new TajoConf());
    TablespaceManager.addTableSpaceForTest(mysqlTablespace);
  }

  @BeforeClass
  public static void setUp() throws IOException, SQLException {
    prepareTables();
  }

  @Test
  public void testGeneral() {
    assertTrue(server.isRunning());
    assertTrue(server.isReadyForConnections());
    assertEquals(server.getMySqlVersion(), "5.5.9");
    assertEquals(server.getDatabases(), ImmutableSet.of("basic"));
    assertEquals(server.getUser(), "testuser");
    assertEquals(server.getPassword(), "testpass");
    assertEquals(server.getJdbcUrl().substring(0, 5), "jdbc:");
    assertEquals(server.getPort(), URI.create(server.getJdbcUrl().substring(5)).getPort());
  }

  @Test
  public void testTablespaceHandler() throws Exception {
    assertTrue((TablespaceManager.getByName("mysql_cluster").get()) instanceof MySQLTablespace);
    assertEquals("mysql_cluster", (TablespaceManager.getByName("mysql_cluster").get().getName()));

    assertTrue((TablespaceManager.get(server.getJdbcUrl()).get()) instanceof MySQLTablespace);
    assertTrue((TablespaceManager.get(server.getJdbcUrl() + "&table=tb1").get()) instanceof MySQLTablespace);

    assertEquals(server.getJdbcUrl(), TablespaceManager.get(server.getJdbcUrl()).get().getUri().toASCIIString());
  }

  static final String [] TPCH_TABLES = {
      "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"
  };

  private static void prepareTables() throws SQLException, IOException {
    try (Connection connection = DriverManager.getConnection(server.getJdbcUrl())) {
      connection.setCatalog("tpch");
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate(FileUtil.readTextFileFromResource("tpch/" + TPCH_TABLES + ".sql"));
      }
    }
  }

  @Test
  public void testMetadataProvider() throws Exception {
    MySQLTablespace tablespace = (MySQLTablespace) TablespaceManager.get(server.getJdbcUrl()).get();
    MetadataProvider provider = tablespace.getMetadataProvider();

    Set<String> tables = Sets.newHashSet(provider.getTables(null, null));
    assertEquals(Sets.newHashSet(TPCH_TABLES), tables);

//    TableDesc desc = provider.getTableDescriptor("", "t2");
//    assertEquals(tablespace.getUri() + "&table=t2", desc.getUri().toASCIIString());
//
//    Schema schema = desc.getSchema();
//    System.out.println(">>> " + schema);
  }
}
