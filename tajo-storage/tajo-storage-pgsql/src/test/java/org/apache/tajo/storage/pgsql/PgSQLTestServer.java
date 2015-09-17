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

import com.google.common.base.Optional;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.jdbc.JdbcTablespace;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.JavaResourceUtil;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class PgSQLTestServer {
  private static final Log LOG = LogFactory.getLog(PgSQLTestServer.class);

  private static PgSQLTestServer instance;

  public static final String [] TPCH_TABLES = {
      "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"
  };

  public static final String SPACENAME = "pgsql_cluster";
  public static final String DATABASE_NAME = "tpch";
  public static final String USERNAME = "testuser";
  public static final String PASSWORD = "";

  private final TestingPostgreSqlServer server;

  static {
    try {
      instance = new PgSQLTestServer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static PgSQLTestServer getInstance() {
    return instance;
  }

  private PgSQLTestServer() throws Exception {
    server = new TestingPostgreSqlServer(USERNAME,
        "tpch"
    );

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          server.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }));

    loadTPCHTables();
    registerTablespace();
  }

  Path testPath = CommonTestingUtil.getTestDir();

  private void loadTPCHTables() throws SQLException, IOException {


    try (Connection connection = DriverManager.getConnection(getJdbcUrlForAdmin(), "postgres", null)) {
      connection.setCatalog("tpch");

      try (Statement statement = connection.createStatement()) {

        for (String tableName : TPCH_TABLES) {
          String sql = JavaResourceUtil.readTextFromResource("pgsql/" + tableName + ".sql");
          statement.executeUpdate(sql);

          // restore the table contents into a file stored in a local file system for PgSQL COPY command
          String path = restoreTableContents(tableName);
          String copyCommand = genLoadStatement(tableName, path);
          statement.executeUpdate(copyCommand);
        }

        // load DATETIME_TYPES table
        String sql = JavaResourceUtil.readTextFromResource("pgsql/datetime_types.sql");
        statement.executeUpdate(sql);
        Path filePath = new Path(testPath, "datetime_types.txt");
        storeTableContents("pgsql/datetime_types.txt", filePath);
        String copyCommand = genLoadStatement("datetime_types", filePath.toUri().getPath());
        LOG.info(copyCommand);
        statement.executeUpdate(copyCommand);

      } catch (Throwable t) {
        t.printStackTrace();
        throw t;
      }
    }
  }

  private String genLoadStatement(String tableName, String path) {
    return  "COPY " + tableName + " FROM '" + path + "' WITH (FORMAT csv, DELIMITER '|');";
  }

  private void storeTableContents(String resource, Path path) throws IOException {
    String csvTable = JavaResourceUtil.readTextFromResource(resource);
    String fixedCsvTable = fixExtraColumn(csvTable);
    FileUtil.writeTextToFile(fixedCsvTable, path);
  }

  private String restoreTableContents(String tableName) throws IOException {
    Path filePath = new Path(testPath, tableName + ".tbl");
    storeTableContents("tpch/" + tableName + ".tbl", filePath);
    return filePath.toUri().getPath();
  }

  private String fixExtraColumn(String csvTable) {
    final String [] lines = csvTable.split("\n");
    final StringBuilder rewritten = new StringBuilder();

    for (String l : lines) {
      if (l.charAt(l.length() - 1) == '|') {
        rewritten.append(l.substring(0, l.length() - 1));
      } else {
        rewritten.append(l.substring(0, l.length()));
      }
      rewritten.append("\n");
    }

    return rewritten.toString();
  }

  private void registerTablespace() throws IOException {
    JSONObject configElements = new JSONObject();
    configElements.put(JdbcTablespace.CONFIG_KEY_MAPPED_DATABASE, DATABASE_NAME);

    PgSQLTablespace tablespace = new PgSQLTablespace(SPACENAME, URI.create(getJdbcUrlForAdmin()), configElements);
    tablespace.init(new TajoConf());

    TablespaceManager.addTableSpaceForTest(tablespace);
  }

  /**
   * get JDBC URL for a created user
   *
   * @return JDBC URL for the created user
   */
  public String getJdbcUrl() {
    return server.getJdbcUrl() + "&connectTimeout=5&socketTimeout=5";
  }

  /**
   * get JDBC URL for the Admin user
   *
   * @return JDBC URL for the Admin user
   */
  public String getJdbcUrlForAdmin() {
    // replace 'user' by postgres (admin)
    String url = server.getJdbcUrl().split("\\?")[0];
    url += "?user=postgres&connectTimeout=5&socketTimeout=5";
    return url;
  }

  public TestingPostgreSqlServer getServer() {
    return server;
  }

  public static Optional<Tablespace> resetAllParamsAndSetConnProperties(Map<String, String> connProperties)
      throws IOException {
    String uri = PgSQLTestServer.getInstance().getJdbcUrl().split("\\?")[0];

    JSONObject configElements = new JSONObject();
    configElements.put(JdbcTablespace.CONFIG_KEY_MAPPED_DATABASE, PgSQLTestServer.DATABASE_NAME);

    JSONObject connPropertiesJson = new JSONObject();
    for (Map.Entry<String, String> entry : connProperties.entrySet()) {
      connPropertiesJson.put(entry.getKey(), entry.getValue());
    }
    configElements.put(JdbcTablespace.CONFIG_KEY_CONN_PROPERTIES, connPropertiesJson);

    PgSQLTablespace tablespace = new PgSQLTablespace(PgSQLTestServer.SPACENAME, URI.create(uri), configElements);
    tablespace.init(new TajoConf());
    return TablespaceManager.addTableSpaceForTest(tablespace);
  }
}
