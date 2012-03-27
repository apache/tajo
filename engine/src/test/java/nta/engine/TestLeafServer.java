package nta.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nta.catalog.CatalogService;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.statistics.StatSet;
import nta.datum.DatumFactory;
import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.MasterInterfaceProtos.Partition;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.TCommonProtos.StatType;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.query.QueryUnitRequestImpl;
import nta.storage.Appender;
import nta.storage.Scanner;
import nta.storage.StorageManager;
import nta.storage.StorageUtil;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.log.Log;

import tajo.datachannel.Fetcher;

import com.google.common.collect.Lists;

/**
 * @author Hyunsik Choi
 */
public class TestLeafServer {

  private Configuration conf;
  private NtaTestingUtility util;
  private String TEST_PATH = "target/test-data/TestLeafServer";
  private StorageManager sm;
  private CatalogService catalog;
  private QueryAnalyzer analyzer;
  private QueryContext.Factory qcFactory;
  private static int tupleNum = 10000;

  @Before
  public void setUp() throws Exception {
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
    
    TableDesc desc = TCatUtil.newTableDesc("employee", employeeMeta, 
        sm.getTablePath("employee")); 
    catalog.addTable(desc);
    
    // For fetch
    TableMeta meta2 = TCatUtil.newTableMeta(schema, StoreType.CSV);    
    sm.initTableBase(new Path(TEST_PATH, "remote"), meta2);

    appender = sm.getAppender(meta2, StorageUtil.concatPath(TEST_PATH, 
        "remote", "data", "remote"));
    tuple = new VTuple(meta2.getSchema().getColumnNum());

    for (int i = 0; i < tupleNum; i++) {
      tuple.put(
          DatumFactory.createString("name_" + i),
          DatumFactory.createInt(i),
          DatumFactory.createString("dept_" + i));
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
    
    desc = TCatUtil.newTableDesc("remote", meta2, sm.getTablePath("remote"));
    catalog.addTable(desc);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public final void testRequestSubQuery() throws Exception {
    Fragment[] frags = sm.split("employee", 40000);
        
    int splitIdx = (int) Math.ceil(frags.length / 2.f);
    QueryIdFactory.reset();
    LeafServer leaf1 = util.getMiniNtaEngineCluster().getLeafServer(0);
    LeafServer leaf2 = util.getMiniNtaEngineCluster().getLeafServer(1);    
    QueryUnitId qid1 = QueryIdFactory.newQueryUnitId();
    QueryUnitId qid2 = QueryIdFactory.newQueryUnitId();
    
    QueryContext ctx = qcFactory.create();
    ParseTree query = analyzer.parse(ctx, 
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
      reported.clear();
      for (InProgressStatus ips : list) {
        // Because this query is to store, it should have the statistics info 
        // of the store data. The below 'assert' examines the existence of 
        // the statistics info.
        if (ips.getStatus() == QueryStatus.FINISHED) {
          reported.add(new QueryUnitId(ips.getId()));
          assertTrue(ips.hasStats());   
        }
      }

      if (reported.containsAll(submitted)) {
        break;
      }
      
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
  
  @Test
  public final void testInterQuery() throws Exception {
    Fragment[] frags = sm.split("employee", 40000);
    for (Fragment frag : frags) {
      Log.info(frag.toString());
    }
    
    int splitIdx = (int) Math.ceil(frags.length / 2.f);
    QueryIdFactory.reset();
    LeafServer leaf1 = util.getMiniNtaEngineCluster().getLeafServer(0);
    LeafServer leaf2 = util.getMiniNtaEngineCluster().getLeafServer(1);    
    QueryUnitId qid1 = QueryIdFactory.newQueryUnitId();
    QueryUnitId qid2 = QueryIdFactory.newQueryUnitId();
    
    QueryContext ctx = qcFactory.create();
    ParseTree query = analyzer.parse(ctx, 
        "select deptName, sum(empId) as merge from employee group by deptName");
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    int numPartitions = 2;
    Column key1 = new Column("employee.deptName", DataType.STRING);
    StoreTableNode storeNode = new StoreTableNode("testInterQuery");
    storeNode.setPartitions(new Column[] { key1 }, numPartitions);
    PlannerUtil.insertNode(plan, storeNode);
    LogicalOptimizer.optimize(ctx, plan);   
    
    sm.initTableBase(frags[0].getMeta(), "testInterQuery");
    QueryUnitRequest req1 = new QueryUnitRequestImpl(
        qid1, Lists.newArrayList(Arrays.copyOfRange(frags, 0, splitIdx)),
        "", false, plan.toJSON());
    req1.setInterQuery();
    
    QueryUnitRequest req2 = new QueryUnitRequestImpl(
        qid2, Lists.newArrayList(Arrays.copyOfRange(frags, splitIdx,
            frags.length)), "", false, plan.toJSON());
    req2.setInterQuery();

    assertNotNull(leaf1.requestQueryUnit(req1.getProto()));
    Thread.sleep(1000);
    assertNotNull(leaf2.requestQueryUnit(req2.getProto()));
    
    // for the report sending test
    NtaEngineMaster master = util.getMiniNtaEngineCluster().getMaster();
    Collection<InProgressStatus> list = master.getProgressQueries();
    Set<QueryUnitId> submitted = new HashSet<QueryUnitId>();
    submitted.add(req1.getId());
    submitted.add(req2.getId());
    
    assertSubmittedAndReported(master, submitted);
    
    Schema secSchema = plan.getOutputSchema();
    TableMeta secMeta = TCatUtil.newTableMeta(secSchema, StoreType.CSV);
    Path secData = sm.initLocalTableBase(new Path(TEST_PATH + "/sec"), secMeta);
    
    for (InProgressStatus ips : list) {
      if (ips.getStatus() == QueryStatus.FINISHED) {
        Log.info(">>>>> InProgress: " + ips.getId());
        long sum = 0;
        List<Partition> partitions = ips.getPartitionsList();
        assertEquals(2, partitions.size());
        for (Partition part : partitions) {
          File out = new File(secData.toString() + "/" + part.getPartitionKey());
          if (out.exists()) {
            out.delete();
          }
          Fetcher fetcher = new Fetcher(URI.create(part.getFileName()), out);
          File fetched = fetcher.get();
          Log.info(">>>>> Fetched: partition" + "(" + part.getPartitionKey()
              + ") " + fetched.getAbsolutePath() + " (size: "
              + fetched.length() + " bytes)");
          assertNotNull(fetched);
          sum += fetched.length();
        }
        StatSet stat = new StatSet(ips.getStats());
        assertEquals(stat.getStat(StatType.TABLE_NUM_BYTES).getValue(), sum);
      }
    }    
    
    // Receiver
    Schema newSchema = new Schema();
    newSchema.addColumn("col1", DataType.STRING);
    newSchema.addColumn("col2", DataType.INT);
    TableMeta newMeta = TCatUtil.newTableMeta(newSchema, StoreType.CSV);
    catalog.addTable(TCatUtil.newTableDesc("interquery", newMeta, new Path("/")));
    QueryUnitId qid3 = QueryIdFactory.newQueryUnitId();
    ctx = qcFactory.create();
    query = analyzer.parse(ctx, 
        "select col1, col2 from interquery");
    plan = LogicalPlanner.createPlan(ctx, query);    
    storeNode = new StoreTableNode("final");
    PlannerUtil.insertNode(plan, storeNode);
    LogicalOptimizer.optimize(ctx, plan);
    sm.initTableBase(TCatUtil.newTableMeta(plan.getOutputSchema(), StoreType.CSV), 
        "final");
    Fragment emptyFrag = new Fragment("interquery", new Path("/"), newMeta, 0l, 0l);
    QueryUnitRequest req3 = new QueryUnitRequestImpl(
        qid3, Lists.newArrayList(emptyFrag),
        "", false, plan.toJSON());
    for (InProgressStatus ips : list) {
      for (Partition part : ips.getPartitionsList()) {
        if (part.getPartitionKey() == 0)
          req3.addFetch("interquery", URI.create(part.getFileName()));
      }
    }
    
    submitted.clear();
    submitted.add(req3.getId());
    assertNotNull(leaf1.requestQueryUnit(req3.getProto()));
    assertSubmittedAndReported(master, submitted);
  }
  
  public void assertSubmittedAndReported(NtaEngineMaster master, 
      Set<QueryUnitId> submitted) throws InterruptedException {
    Set<QueryUnitId> reported = new HashSet<QueryUnitId>();
    Collection<InProgressStatus> list = master.getProgressQueries();
    int i = 0;
    while (i < 10) { // waiting for the report messages 
      Log.info("Waiting for receiving the report messages");
      Thread.sleep(1000);
      list = master.getProgressQueries();
      reported.clear();
      for (InProgressStatus ips : list) {
        // Because this query is to store, it should have the statistics info 
        // of the store data. The below 'assert' examines the existence of 
        // the statistics info.
        if (ips.getStatus() == QueryStatus.FINISHED) {
          reported.add(new QueryUnitId(ips.getId()));
        }
      }

      if (reported.containsAll(submitted)) {
        break;
      }
      
      i++;
    }
    Log.info("reported: " + reported);
    Log.info("submitted: " + submitted);
    assertTrue(reported.containsAll(submitted));
  }
}
