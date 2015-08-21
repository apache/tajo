package org.apache.tajo.storage.pgsql;

import com.google.common.collect.Sets;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.exception.UndefinedTableException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPgSQLEndPointTests extends QueryTestCaseBase {
  private static final String jdbcUrl = EmbedPgSQLServer.getInstance().getJdbcUrl();
  private static TajoClient client;


  @BeforeClass
  public static void setUp() throws Exception {
    QueryTestCaseBase.testingCluster.getMaster().refresh();
    client = QueryTestCaseBase.testingCluster.newTajoClient();
  }

  @AfterClass
  public static void tearDown() {
    client.close();
  }

  @Test
  public void testGetAllDatabaseNames() {
    Set<String> retrieved = Sets.newHashSet(client.getAllDatabaseNames());
    assertTrue(retrieved.contains(EmbedPgSQLServer.DATABASE_NAME));
  }

  @Test
  public void testGetTableList() {
    Set<String> retrieved = Sets.newHashSet(client.getTableList("tpch"));
    assertEquals(Sets.newHashSet(EmbedPgSQLServer.TPCH_TABLES), retrieved);
  }

  @Test
  public void testGetTable() throws UndefinedTableException {
    for (String tableName: EmbedPgSQLServer.TPCH_TABLES) {
      TableDesc retrieved = client.getTableDesc(EmbedPgSQLServer.DATABASE_NAME + "." + tableName);
      assertEquals(EmbedPgSQLServer.DATABASE_NAME + "." + tableName, retrieved.getName());
      assertEquals(jdbcUrl + "&table=" + tableName, retrieved.getUri().toASCIIString());
    }
  }
}
