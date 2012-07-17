package nta.cluster;

import nta.catalog.*;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.*;
import nta.engine.cluster.ClusterManager;
import nta.engine.cluster.ClusterManager.DiskInfo;
import nta.engine.cluster.ClusterManager.WorkerInfo;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.GlobalOptimizer;
import nta.engine.planner.global.MasterPlan;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.query.GlobalPlanner;
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

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestClusterManager {
  static final Log LOG = LogFactory.getLog(TestClusterManager.class);
  
  private static ClusterManager cm;
  private static NtaTestingUtility util;
  private static WorkerCommunicator wc;
  private Collection<List<String>> workersCollection;
  private static CatalogService local;
  private static NtaEngineMaster master;

  final static int CLUST_NUM = 4;
  final static int tbNum = 5;

  static String TEST_PATH = "target/test-data/TestCatalog";

  @BeforeClass
  public static void setUp() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniCluster(CLUST_NUM);
    Thread.sleep(4000);

    master = util.getMiniTajoCluster().getMaster();
    assertNotNull(master);
    wc = master.getWorkerCommunicator();
    cm = master.getClusterManager();
    assertNotNull(wc);
    assertNotNull(cm);
    
    cm.updateOnlineWorker();

    local = util.getMiniTajoCluster().getMaster()
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
    workersCollection = cm.getOnlineWorkers().values();

    List<Set<Fragment>> frags = new ArrayList<Set<Fragment>>();

    int i = 0;
    for (List<String> workers : workersCollection) {
      i+= workers.size();
      for (String w : workers) {
        if (cm.getFragbyWorker(w) != null) {
          frags.add(cm.getFragbyWorker(w));
        }
      }
    }

    String prevName;
    for (Set<Fragment> fragmentSet : frags) {
      prevName = "";
      for (Fragment fragment : fragmentSet) {
        String workerName = cm.getWorkerbyFrag(fragment);
        assertNotNull(workerName);
        if (!prevName.equals("")) {
          assertEquals(prevName, workerName);
        } else {
          prevName = workerName;
        }
      }
    }
  }

  @Test
  public void testGetProperHost() throws Exception {
    QueryAnalyzer analyzer = new QueryAnalyzer(local);
    QueryContext.Factory factory = new QueryContext.Factory(local);
    String query = "select id, age, name from HostsByTable0";
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, query);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, tree);
    plan = LogicalOptimizer.optimize(ctx, plan);

    // build the master plan
    GlobalPlanner globalPlanner = new GlobalPlanner(
        master.getStorageManager(), master.getQueryManager(), local);
    GlobalOptimizer globalOptimizer = new GlobalOptimizer();
    QueryId qid = QueryIdFactory.newQueryId();
    SubQueryId subId = QueryIdFactory.newSubQueryId(qid);
    MasterPlan globalPlan = globalPlanner.build(subId, plan);
    globalPlan = globalOptimizer.optimize(globalPlan.getRoot());
    QueryUnit[] units = globalPlanner.localize(globalPlan.getRoot(),
        CLUST_NUM);
    ClusterManager cm = master.getClusterManager();
    for (QueryUnit unit : units) {
      assertNotNull(cm.getProperHost(unit));
    }
  }
}
