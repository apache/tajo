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

import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.*;

public class TestPgSQLJdbcTableSpace {
  @BeforeClass
  public static void setUp() throws IOException {
    String mysqlUri = "jdbc:mysql://host1:2171/db1";
    PgSQLTablespace mysqlTablespace = new PgSQLTablespace("cluster2", URI.create(mysqlUri), null);
    mysqlTablespace.init(new TajoConf());
    TablespaceManager.addTableSpaceForTest(mysqlTablespace);

    String pgsqlUri = "jdbc:postgres://host1:2615/db2";
    PgSQLTablespace pgSQLTablespace = new PgSQLTablespace("cluster3", URI.create(pgsqlUri), null);
    pgSQLTablespace.init(new TajoConf());
    TablespaceManager.addTableSpaceForTest(pgSQLTablespace);
  }

  @Test
  public void testTablespaceHandler() throws Exception {
    assertTrue((TablespaceManager.getByName("cluster2").get()) instanceof PgSQLTablespace);
    assertEquals("cluster2", (TablespaceManager.getByName("cluster2").get().getName()));
    assertTrue((TablespaceManager.get(URI.create("jdbc:mysql://host1:2171/db1")).get()) instanceof PgSQLTablespace);
    assertEquals(URI.create("jdbc:mysql://host1:2171/db1"),
        TablespaceManager.get(URI.create("jdbc:mysql://host1:2171/db1")).get().getUri());

    assertTrue((TablespaceManager.getByName("cluster3").get()) instanceof PgSQLTablespace);
    assertEquals("cluster3", (TablespaceManager.getByName("cluster3").get().getName()));
    assertTrue((TablespaceManager.get(URI.create("jdbc:postgres://host1:2615/db2")).get()) instanceof PgSQLTablespace);

    assertEquals(URI.create("jdbc:postgres://host1:2615/db2"),
        TablespaceManager.get(URI.create("jdbc:postgres://host1:2615/db2")).get().getUri());
  }

  @Test
  public void test() throws Exception {
    try (TestingPostgreSqlServer server = new TestingPostgreSqlServer("testuser", "testdb")) {
      assertEquals(server.getUser(), "testuser");
      assertEquals(server.getDatabase(), "testdb");
      assertEquals(server.getJdbcUrl().substring(0, 5), "jdbc:");
      assertEquals(server.getPort(), URI.create(server.getJdbcUrl().substring(5)).getPort());

      try (Connection connection = DriverManager.getConnection(server.getJdbcUrl())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE test_table (c1 bigint PRIMARY KEY)");
          statement.execute("INSERT INTO test_table (c1) VALUES (1)");
          try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM test_table")) {
            assertTrue(resultSet.next());
            assertEquals(resultSet.getLong(1), 1L);
            assertFalse(resultSet.next());
          }
        }
      }
    }
  }
}
