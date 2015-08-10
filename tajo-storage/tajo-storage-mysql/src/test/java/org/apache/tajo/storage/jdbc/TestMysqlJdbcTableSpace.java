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
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMysqlJdbcTableSpace {
  @BeforeClass
  public static void setUp() throws IOException {
    String mysqlUri = "jdbc:mysql://host1:2171/db1";
    MySQLTablespace mysqlTablespace = new MySQLTablespace("cluster2", URI.create(mysqlUri), null);
    mysqlTablespace.init(new TajoConf());
    TablespaceManager.addTableSpaceForTest(mysqlTablespace);
  }

  @Test
  public void testTablespaceHandler() throws Exception {
    assertTrue((TablespaceManager.getByName("cluster2").get()) instanceof MySQLTablespace);
    assertEquals("cluster2", (TablespaceManager.getByName("cluster2").get().getName()));
    assertTrue((TablespaceManager.get(URI.create("jdbc:mysql://host1:2171/db1")).get()) instanceof MySQLTablespace);
    assertTrue((TablespaceManager.get(URI.create("jdbc:mysql://host1:2171/db1?table=xyz")).get())
        instanceof MySQLTablespace);

    assertEquals(URI.create("jdbc:mysql://host1:2171/db1"),
        TablespaceManager.get(URI.create("jdbc:mysql://host1:2171/db1")).get().getUri());

    assertTrue((TablespaceManager.getByName("cluster3").get()) instanceof MySQLTablespace);
    assertEquals("cluster3", (TablespaceManager.getByName("cluster3").get().getName()));
    assertTrue((TablespaceManager.get(URI.create("jdbc:postgres://host1:2615/db2")).get()) instanceof MySQLTablespace);
    assertTrue((TablespaceManager.get(URI.create("jdbc:postgres://host1:2615/db2?table=xyz")).get())
        instanceof MySQLTablespace);

    assertEquals(URI.create("jdbc:postgres://host1:2615/db2"),
        TablespaceManager.get(URI.create("jdbc:postgres://host1:2615/db2")).get().getUri());
  }

  @Test
  public void test() throws Exception {
    try (TestingMySqlServer server = new TestingMySqlServer("testuser", "testpass", "db1", "db2")) {
      assertTrue(server.isRunning());
      assertTrue(server.isReadyForConnections());
      assertEquals(server.getMySqlVersion(), "5.5.9");
      assertEquals(server.getDatabases(), ImmutableSet.of("db1", "db2"));
      assertEquals(server.getUser(), "testuser");
      assertEquals(server.getPassword(), "testpass");
      assertEquals(server.getJdbcUrl().substring(0, 5), "jdbc:");
      assertEquals(server.getPort(), URI.create(server.getJdbcUrl().substring(5)).getPort());

      try (Connection connection = DriverManager.getConnection(server.getJdbcUrl())) {
        connection.setCatalog("db1");

        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE t1 (c1 bigint PRIMARY KEY)");
          statement.execute("CREATE TABLE t2 (c1 int PRIMARY KEY, c2 VARCHAR(20), c3 TIME)");
        }
      }

      System.out.println(server.getJdbcUrl());
      MySQLTablespace tablespace = new MySQLTablespace("mysql", URI.create(server.getJdbcUrl()), null);

      URI uri = tablespace.getTableUri("abc", "table1");
      JdbcConnectionInfo c1 = JdbcConnectionInfo.fromURI(uri);
      assertEquals("table1", c1.tableName);

      MetadataProvider provider = tablespace.getMetadataProvider();
      Set<String> tables = Sets.newHashSet(provider.getTables(null, null));
      assertEquals(Sets.newHashSet("t1", "t2"), tables);

      TableDesc desc = provider.getTableDescriptor("", "t2");
      assertEquals(tablespace.getUri() + "&table=t2", desc.getUri().toASCIIString());

      Schema schema = desc.getSchema();
      System.out.println(">>> " + schema);
    }
  }
}
