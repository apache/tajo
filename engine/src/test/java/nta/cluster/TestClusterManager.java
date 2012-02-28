package nta.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import nta.catalog.CatalogService;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.NtaEngineMaster;
import nta.engine.NtaTestingUtility;
import nta.engine.cluster.ClusterManager;
import nta.engine.cluster.ClusterManager.DiskInfo;
import nta.engine.cluster.ClusterManager.WorkerInfo;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.storage.CSVFile2;
import nta.util.FileUtil;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClusterManager {
  private static ClusterManager cm;
  private static NtaTestingUtility util;
  private static WorkerCommunicator wc;
  private List<String> workers;

  final static int CLUST_NUM = 4;
  final static int tbNum = 5;

  static String TEST_PATH = "target/test-data/TestCatalog";

  @BeforeClass
  public static void setUp() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniCluster(CLUST_NUM);
    util.startCatalogCluster();
    Thread.sleep(4000);

    NtaEngineMaster master = util.getMiniNtaEngineCluster().getMaster();
    assertNotNull(master);
    wc = master.getWorkerCommunicator();
    cm = master.getClusterManager();
    assertNotNull(wc);
    assertNotNull(cm);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    util.shutdownCatalogCluster();
    util.shutdownMiniCluster();
  }

  @Test
  public void testGetOnlineWorker() throws Exception {
    workers = cm.getOnlineWorker();
    assertEquals(workers.size(), CLUST_NUM);
  }

  @Test
  public void testGetWorkerInfo() throws Exception {
    workers = cm.getOnlineWorker();
    for (String worker : workers) {

      WorkerInfo wi = cm.getWorkerInfo(worker);
      assertNotNull(wi.availableProcessors);
      assertNotNull(wi.freeMemory);
      assertNotNull(wi.totalMemory);

      List<DiskInfo> disks = wi.disks;
      for (DiskInfo di : disks) {
        assertNotNull(di.freeSpace);
        assertNotNull(di.totalSpace);
      }
    }
  }

  @Test
  public void testGetFragAndWorker() throws Exception {
    CatalogService local = util.getMiniCatalogCluster().getCatalog();

    int i, j;
    FSDataOutputStream fos;
    Path tbPath;

    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.INT);
    schema.addColumn("name", DataType.STRING);

    TableMeta meta;

    String[] tuples = { "1,32,hyunsik", "2,29,jihoon", "3,28,jimin",
        "4,24,haemi" };

    FileSystem fs = util.getMiniDFSCluster().getFileSystem();

    Random random = new Random();
    int tupleNum;

    for (i = 0; i < tbNum; i++) {
      tbPath = new Path(TEST_PATH + "/HostsByTable" + i);
      if (fs.exists(tbPath)) {
        fs.delete(tbPath, true);
      }
      fs.mkdirs(tbPath);
      fos = fs.create(new Path(tbPath, ".meta"));
      meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
      meta.putOption(CSVFile2.DELIMITER, ",");
      FileUtil.writeProto(fos, meta.getProto());
      fos.close();

      fos = fs.create(new Path(tbPath, "data/table.csv"));
      tupleNum = random.nextInt(49) + 100001;
      for (j = 0; j < tupleNum; j++) {
        fos.writeBytes(tuples[0] + "\n");
      }
      fos.close();

      TableDesc desc = new TableDescImpl("HostsByTable" + i, meta, tbPath);
      local.addTable(desc);
    }

    cm.updateAllFragmentServingInfo(cm.getOnlineWorker());
    workers = cm.getOnlineWorker();

    List<Set<Fragment>> frags = new ArrayList<Set<Fragment>>();

    int quotient = tbNum / CLUST_NUM;
    int remainder = tbNum % CLUST_NUM;

    for (int n = 0; n < CLUST_NUM; n++) {
      frags.add(cm.getFragbyWorker(workers.get(n)));
      int isRemain = (n < remainder) ? 1 : 0;
      assertEquals(frags.get(n).size(), quotient + isRemain);
    }

    for (int n = 0; n < CLUST_NUM; n++) {
      for (Fragment frag : frags.get(n)) {
        String workerName = cm.getWorkerbyFrag(frag);
        assertNotNull(workerName);
      }
    }
  }
}
