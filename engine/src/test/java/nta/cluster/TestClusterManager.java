package nta.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClusterManager {
  static final Log LOG = LogFactory.getLog(TestClusterManager.class);
  
  private static ClusterManager cm;
  private static NtaTestingUtility util;
  private static WorkerCommunicator wc;
  private Collection<List<String>> workersCollection;

  final static int CLUST_NUM = 4;
  final static int tbNum = 5;

  static String TEST_PATH = "target/test-data/TestCatalog";

  @BeforeClass
  public static void setUp() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniCluster(CLUST_NUM);
    Thread.sleep(4000);

    NtaEngineMaster master = util.getMiniTajoCluster().getMaster();
    assertNotNull(master);
    wc = master.getWorkerCommunicator();
    cm = master.getClusterManager();
    assertNotNull(wc);
    assertNotNull(cm);
    
    cm.updateOnlineWorker();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    util.shutdownMiniCluster();
  }

  @Test
  public void testGetOnlineWorker() throws Exception {
    int i = 0;
    for (List<String> workers : cm.getOnlineWorkers().values()) {
      i += workers.size();
    }
    assertEquals(i, CLUST_NUM);
  }

  @Test
  public void testGetWorkerInfo() throws Exception {
    workersCollection = cm.getOnlineWorkers().values();
    for (List<String> worker : workersCollection) {
      for (String w : worker) {
        WorkerInfo wi = cm.getWorkerInfo(w);
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
  }

  @Test
  public void testGetFragAndWorker() throws Exception {
    CatalogService local = util.getMiniTajoCluster().getMaster()
        .getCatalog();

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
      tupleNum = random.nextInt(49) + 10000001;
      for (j = 0; j < tupleNum; j++) {
        fos.writeBytes(tuples[0] + "\n");
      }
      fos.close();

      TableDesc desc = new TableDescImpl("HostsByTable" + i, meta, tbPath);
      local.addTable(desc);
      
      cm.updateFragmentServingInfo(desc.getId());
    }

    workersCollection = cm.getOnlineWorkers().values();

    List<Set<Fragment>> frags = new ArrayList<Set<Fragment>>();
    i = 0;
    for (List<String> workers : workersCollection) {
      i+= workers.size();
      for (String w : workers) {
        LOG.info(">>>>> " + cm.getFragbyWorker(w).size());
        frags.add(cm.getFragbyWorker(w));
      }
    }
    assertEquals(CLUST_NUM, i);

    for (int n = 0; n < CLUST_NUM; n++) {
      for (Fragment frag : frags.get(n)) {
        String workerName = cm.getWorkerbyFrag(frag);
        assertNotNull(workerName);
      }
    }
  }
}
