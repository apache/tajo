package tajo.client;

import com.google.common.collect.Sets;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import tajo.BackendTestingUtil;
import tajo.IntegrationTest;
import tajo.TajoTestingCluster;
import tajo.catalog.TableDesc;
import tajo.conf.TajoConf;
import tajo.storage.StorageUtil;
import tajo.util.CommonTestingUtil;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestTajoClient {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static TajoClient tajo;
  private static String TEST_PATH = "target/test-data/"
      + TestTajoClient.class.getName();
  private static Path testDir;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startMiniCluster(1);
    conf = util.getConfiguration();
    Thread.sleep(3000);
    tajo = new TajoClient(conf);

    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownMiniCluster();
    tajo.close();
  }

  private static Path writeTmpTable(String tableName) throws IOException {
    Path tablePath = StorageUtil.concatPath(testDir, tableName);
    BackendTestingUtil.writeTmpTable(conf, testDir, tableName, true);
    return tablePath;
  }

  @Test
  public final void testAttachTable() throws IOException, ServiceException {
    final String tableName = "attach";
    Path tablePath = writeTmpTable(tableName);
    assertFalse(tajo.existTable(tableName));
    tajo.attachTable(tableName, tablePath);
    assertTrue(tajo.existTable(tableName));
    tajo.detachTable(tableName);
    assertFalse(tajo.existTable(tableName));
  }

  @Test
  public final void testUpdateQuery() throws IOException, ServiceException {
    final String tableName = "testUpdateQuery";
    Path tablePath = writeTmpTable(tableName);

    assertFalse(tajo.existTable(tableName));
    String tql =
        "create external table " + tableName + " (deptname string, score int) "
            + "using csv location '" + tablePath + "'";
    tajo.updateQuery(tql);
    assertTrue(tajo.existTable(tableName));
  }

  @Test
  public final void testCreateAndDropTable()
      throws IOException, ServiceException {
    final String tableName = "testCreateAndDropTable";
    Path tablePath = writeTmpTable(tableName);

    assertFalse(tajo.existTable(tableName));
    tajo.createTable(tableName, tablePath, BackendTestingUtil.mockupMeta);
    assertTrue(tajo.existTable(tableName));
    tajo.dropTable(tableName);
    assertFalse(tajo.existTable(tableName));
    FileSystem fs = tablePath.getFileSystem(conf);
    assertFalse(fs.exists(tablePath));
  }

  @Test
  public final void testDDLByExecuteQuery() throws IOException, ServiceException {
    TajoConf conf = util.getConfiguration();
    final String tableName = "testDDLByExecuteQuery";
    BackendTestingUtil.writeTmpTable(conf, "file:///tmp", tableName, false);

    assertFalse(tajo.existTable(tableName));
    String tql =
        "create external table " + tableName + " (deptname string, score int) "
            + "using csv location 'file:///tmp/" + tableName + "'";
    tajo.executeQueryAndGetResult(tql);
    assertTrue(tajo.existTable(tableName));
  }

  // disabled
  public final void testGetClusterInfo() throws IOException, InterruptedException {
    assertEquals(1,tajo.getClusterInfo().size());
  }

  @Test
  public final void testGetTableList() throws IOException, ServiceException {
    final String tableName1 = "table1";
    final String tableName2 = "table2";
    Path table1Path = writeTmpTable(tableName1);
    Path table2Path = writeTmpTable(tableName2);

    assertFalse(tajo.existTable(tableName1));
    assertFalse(tajo.existTable(tableName2));
    tajo.attachTable(tableName1, table1Path);
    assertTrue(tajo.existTable(tableName1));
    Set<String> tables = Sets.newHashSet(tajo.getTableList());
    assertTrue(tables.contains(tableName1));
    tajo.attachTable(tableName2, table2Path);
    assertTrue(tajo.existTable(tableName2));
    tables = Sets.newHashSet(tajo.getTableList());
    assertTrue(tables.contains(tableName1));
    assertTrue(tables.contains(tableName2));
  }

  @Test
  public final void testGetTableDesc() throws IOException, ServiceException {
    final String tableName1 = "table3";
    Path tablePath = writeTmpTable(tableName1);
    assertFalse(tajo.existTable(tableName1));
    tajo.attachTable(tableName1, tablePath);
    assertTrue(tajo.existTable(tableName1));
    TableDesc desc = tajo.getTableDesc(tableName1);
    assertNotNull(desc);
    assertEquals(tableName1, desc.getId());
    assertTrue(desc.getMeta().getStat().getNumBytes() > 0);
  }
}
