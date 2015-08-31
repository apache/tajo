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

import io.airlift.testing.mysql.TestingMySqlServer;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.JavaResourceUtil;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class EmbedMySQLServer {
  private static final Log LOG = LogFactory.getLog(EmbedMySQLServer.class);

  private static EmbedMySQLServer instance;

  public static final String [] TPCH_TABLES = {
      "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"
  };

  public static final String SPACENAME = "mysql_cluster";
  public static final String DATABASE_NAME = "tpch";

  private TestingMySqlServer server;

  static {
    try {
      instance = new EmbedMySQLServer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static EmbedMySQLServer getInstance() {
    return instance;
  }

  private EmbedMySQLServer() throws Exception {
    server = new TestingMySqlServer("testuser", "testpass",
        "tpch"
    );

    loadTPCHTables();
    registerTablespace();
  }

  private void loadTPCHTables() throws SQLException, IOException {
    try (Connection connection = DriverManager.getConnection(server.getJdbcUrl())) {
      connection.setCatalog("tpch");

      try (Statement statement = connection.createStatement()) {
        for (String tableName : TPCH_TABLES) {
          String sql = JavaResourceUtil.readTextFromResource("tpch/" + tableName + ".sql");
          statement.addBatch(sql);
        }

        try {
          statement.executeBatch();
        } catch (SQLException e) {
          LOG.error(e);
        }
      }
    }
  }

  private void registerTablespace() throws IOException {
    JSONObject configElements = new JSONObject();
    configElements.put("database", DATABASE_NAME);

    Map<String, JSONObject> configMap = new HashMap<>();
    configMap.put(TablespaceManager.TABLESPACE_SPEC_CONFIGS_KEY, configElements);
    JSONObject config = new JSONObject(configMap);

    MySQLTablespace mysqlTablespace = new MySQLTablespace(SPACENAME, URI.create(server.getJdbcUrl()), config);
    mysqlTablespace.init(new TajoConf());

    TablespaceManager.addTableSpaceForTest(mysqlTablespace);
  }

  public boolean isRunning() {
    return server.isRunning();
  }

  public String getJdbcUrl() {
    return server.getJdbcUrl();
  }

  public TestingMySqlServer getServer() {
    return server;
  }
}
