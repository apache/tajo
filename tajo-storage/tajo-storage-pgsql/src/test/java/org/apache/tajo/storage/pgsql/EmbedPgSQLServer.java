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
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.JavaResourceUtil;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class EmbedPgSQLServer {
  private static final Log LOG = LogFactory.getLog(EmbedPgSQLServer.class);

  private static EmbedPgSQLServer instance;

  public static final String [] TPCH_TABLES = {
      "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"
  };

  public static final String SPACENAME = "pgsql_cluster";
  public static final String DATABASE_NAME = "tpch";

  private TestingPostgreSqlServer server;

  static {
    try {
      instance = new EmbedPgSQLServer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static EmbedPgSQLServer getInstance() {
    return instance;
  }

  private EmbedPgSQLServer() throws Exception {
    server = new TestingPostgreSqlServer("testuser",
        "tpch"
    );

    loadTPCHTables();
    registerTablespace();
  }

  private void loadTPCHTables() throws SQLException, IOException {
    Path testPath = CommonTestingUtil.getTestDir();

    try (Connection connection = DriverManager.getConnection(getJdbcUrlForAdmin(), "postgres", null)) {
      connection.setCatalog("tpch");

      try (Statement statement = connection.createStatement()) {

        for (String tableName : TPCH_TABLES) {
          String sql = JavaResourceUtil.readTextFromResource("tpch/pgsql/" + tableName + ".sql");
          statement.addBatch(sql);
        }

        statement.executeBatch();

        for (String tableName : TPCH_TABLES) {
          String table = JavaResourceUtil.readTextFromResource("tpch/" + tableName + ".tbl");
          Path filePath = new Path(testPath, tableName + ".tbl");
          FileUtil.writeTextToFile(table, filePath);

          String copyCommand =
              "COPY " + tableName + " FROM '" + filePath.toUri().getPath() + "' WITH (FORMAT csv, DELIMITER '|');";
          statement.executeUpdate(copyCommand);
        }

      } catch (Throwable t) {
        t.printStackTrace();
        throw t;
      }
    }
  }

  private void registerTablespace() throws IOException {
    JSONObject configElements = new JSONObject();
    configElements.put("database", DATABASE_NAME);

    Map<String, JSONObject> configMap = new HashMap<>();
    configMap.put(TablespaceManager.TABLESPACE_SPEC_CONFIGS_KEY, configElements);
    JSONObject config = new JSONObject(configMap);

    PgSQLTablespace tablespace = new PgSQLTablespace(SPACENAME, URI.create(getJdbcUrlForAdmin()), config);
    tablespace.init(new TajoConf());

    TablespaceManager.addTableSpaceForTest(tablespace);
  }

  public String getJdbcUrl() {
    return server.getJdbcUrl() + "&connectTimeout=5&socketTimeout=5";
  }

  public String getJdbcUrlForAdmin() {
    String url = server.getJdbcUrl().split("\\?")[0];
    url += "?user=postgres&connectTimeout=5&socketTimeout=5";
    return url;
  }

  public TestingPostgreSqlServer getServer() {
    return server;
  }
}
