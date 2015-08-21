package org.apache.tajo.storage.pgsql;

import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.FileUtil;

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
    try (Connection connection = DriverManager.getConnection(server.getJdbcUrl())) {
      connection.setCatalog("tpch");

      try (Statement statement = connection.createStatement()) {
        for (String tableName : TPCH_TABLES) {
          String sql = FileUtil.readTextFileFromResource("tpch/" + tableName + ".sql");
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

    PgSQLTablespace tablespace = new PgSQLTablespace(SPACENAME, URI.create(server.getJdbcUrl()), config);
    tablespace.init(new TajoConf());

    TablespaceManager.addTableSpaceForTest(tablespace);
  }

  public String getJdbcUrl() {
    return server.getJdbcUrl();
  }

  public TestingPostgreSqlServer getServer() {
    return server;
  }
}
