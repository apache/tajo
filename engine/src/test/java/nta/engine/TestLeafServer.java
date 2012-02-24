package nta.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import nta.catalog.CatalogService;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.DatumFactory;
import nta.engine.QueryUnitProtos.InProgressStatus;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.query.QueryUnitRequestImpl;
import nta.storage.Appender;
import nta.storage.Scanner;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.log.Log;

import com.google.common.collect.Lists;

/**
 * 
 * @author Hyunsik Choi
 * 
 */
public class TestLeafServer {

  private static Configuration conf;
  private static NtaTestingUtility util;
  private static String TEST_PATH = "target/test-data/TestLeafServer";
  private static StorageManager sm;
  private static CatalogService catalog;
  private static QueryAnalyzer analyzer;
  private static QueryContext.Factory qcFactory;
  
  private static int tupleNum = 10000;

  @BeforeClass
  public static void setUp() throws Exception {
    EngineTestingUtils.buildTestDir(TEST_PATH);
    util = new NtaTestingUtility();
    util.startMiniCluster(2);
    catalog = util.getMiniNtaEngineCluster().getMaster().getCatalog();
    conf = util.getConfiguration();
    sm = StorageManager.get(conf);
    QueryIdFactory.reset();
    analyzer = new QueryAnalyzer(catalog);
    qcFactory = new QueryContext.Factory(catalog);
    
    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("deptName", DataType.STRING);
    TableMeta employeeMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);    
    sm.initTableBase(employeeMeta, "employee");

    Appender appender = sm.getAppender(employeeMeta, "employee", "employee");
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());

    for (int i = 0; i < tupleNum; i++) {
      tuple.put(
          DatumFactory.createString("name_" + i),
          DatumFactory.createInt(i),
          DatumFactory.createString("dept_" + i));
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
    
    TableDesc desc = TCatUtil.newTableDesc("employee", employeeMeta, sm.getTablePath("employee")); 
    catalog.addTable(desc);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public final void testRequestSubQuery() throws Exception {
    Fragment[] frags = sm.split("employee", 40000);
    for (Fragment frag : frags) {
      Log.info(frag.toString());
    }
    
    int splitIdx = (int) Math.ceil(frags.length / 2.f);
   
    LeafServer leaf1 = util.getMiniNtaEngineCluster().getLeafServer(0);
    LeafServer leaf2 = util.getMiniNtaEngineCluster().getLeafServer(1);    
    QueryUnitId qid1 = QueryIdFactory.newQueryUnitId();
    QueryUnitId qid2 = QueryIdFactory.newQueryUnitId();
    
    QueryContext ctx = qcFactory.create();
    QueryBlock query = analyzer.parse(ctx, 
        "testLeafServer := select name, empId, deptName from employee");
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);    
    
    sm.initTableBase(frags[0].getMeta(), "testLeafServer");
    QueryUnitRequest req1 = new QueryUnitRequestImpl(
        qid1, Lists.newArrayList(Arrays.copyOfRange(frags, 0, splitIdx)),
        "", false, plan.toJSON());
    
    QueryUnitRequest req2 = new QueryUnitRequestImpl(
        qid2, Lists.newArrayList(Arrays.copyOfRange(frags, splitIdx,
            frags.length)), "", false, plan.toJSON());

    assertNotNull(leaf1.requestQueryUnit(req1.getProto()));
    Thread.sleep(1000);
    assertNotNull(leaf2.requestQueryUnit(req2.getProto()));
    
    // for the report sending test
    NtaEngineMaster master = util.getMiniNtaEngineCluster().getMaster();
    Collection<InProgressStatus> list = master.getProgressQueries();
    Set<QueryUnitId> reported = new HashSet<QueryUnitId>();
    Set<QueryUnitId> submitted = new HashSet<QueryUnitId>();
    submitted.add(req1.getId());
    submitted.add(req2.getId());

    int i = 0;
    while (i < 10) { // waiting for the report messages 
      Log.info("Waiting for receiving the report messages");
      Thread.sleep(1000);
      list = master.getProgressQueries();
      for (InProgressStatus ips : list) {
        Log.info(ips.toString());
      }
      reported.clear();
      for (InProgressStatus ips : list) {
        reported.add(new QueryUnitId(ips.getId()));
      }
      if (reported.containsAll(submitted))
        break;
      
      i++;
    }
    Log.info("reported: " + reported);
    Log.info("submitted: " + submitted);
    assertTrue(reported.containsAll(submitted));
    
    Scanner scanner = sm.getTableScanner("testLeafServer");
    int j = 0;
    @SuppressWarnings("unused")
    Tuple tuple = null;
    while ((tuple = scanner.next()) != null) {
      j++;
    }

    assertEquals(tupleNum, j);
  }  
}
