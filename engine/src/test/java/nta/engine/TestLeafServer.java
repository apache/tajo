package nta.engine;

import com.google.common.collect.Lists;
import nta.catalog.*;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.statistics.TableStat;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.MasterInterfaceProtos.InProgressStatusProto;
import nta.engine.MasterInterfaceProtos.Partition;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.query.QueryUnitRequestImpl;
import nta.storage.*;
import nta.storage.Scanner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import tajo.datachannel.Fetcher;

import java.io.File;
import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Hyunsik Choi
 */
public class TestLeafServer {
  private final Log LOG = LogFactory.getLog(TestLeafServer.class);
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
    WorkerTestingUtil.buildTestDir(TEST_PATH);
    util = new NtaTestingUtility();
    util.startMiniCluster(2);
    catalog = util.getMiniTajoCluster().getMaster().getCatalog();
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
      tuple.put(new Datum[] {
          DatumFactory.createString("name_" + i),
          DatumFactory.createInt(i),
          DatumFactory.createString("dept_" + i)});
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
      tuple.put(new Datum[] {
          DatumFactory.createString("name_" + i),
          DatumFactory.createInt(i),
          DatumFactory.createString("dept_" + i)});
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

//  @Test
  public final void testRequestSubQuery() throws Exception {
    Fragment[] frags = sm.split("employee", 40000);
        
    int splitIdx = (int) Math.ceil(frags.length / 2.f);
    QueryIdFactory.reset();

    LeafServer leaf1 = util.getMiniTajoCluster().getLeafServer(0);
    LeafServer leaf2 = util.getMiniTajoCluster().getLeafServer(1);
    
    ScheduleUnitId sid = QueryIdFactory.newScheduleUnitId(
        QueryIdFactory.newSubQueryId(
            QueryIdFactory.newQueryId()));
    QueryUnitId qid1 = QueryIdFactory.newQueryUnitId(sid);
    QueryUnitId qid2 = QueryIdFactory.newQueryUnitId(sid);
    
    QueryContext ctx = qcFactory.create();
    ParseTree query = analyzer.parse(ctx, 
        "testLeafServer := select name, empId, deptName from employee");
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    plan = LogicalOptimizer.optimize(ctx, plan);
    
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
    Thread.sleep(1000);
    
    // for the report sending test
    NtaEngineMaster master = util.getMiniTajoCluster().getMaster();
    Set<QueryUnitId> reported = new HashSet<QueryUnitId>();
    Set<QueryUnitId> submitted = new HashSet<QueryUnitId>();
    submitted.add(req1.getId());
    submitted.add(req2.getId());

    assertSubmittedAndReported(master, submitted);
    
    Scanner scanner = sm.getTableScanner("testLeafServer");
    int j = 0;
    @SuppressWarnings("unused")
    Tuple tuple = null;
    while (scanner.next() != null) {
      j++;
    }

    assertEquals(tupleNum, j);
  }
  
//  @Test
  public final void testInterQuery() throws Exception {
    Fragment[] frags = sm.split("employee", 40000);
    for (Fragment frag : frags) {
      LOG.info(frag.toString());
    }
    
    int splitIdx = (int) Math.ceil(frags.length / 2.f);
    QueryIdFactory.reset();

    LeafServer leaf1 = util.getMiniTajoCluster().getLeafServer(0);
    LeafServer leaf2 = util.getMiniTajoCluster().getLeafServer(1);
    ScheduleUnitId sid = QueryIdFactory.newScheduleUnitId(
        QueryIdFactory.newSubQueryId(
            QueryIdFactory.newQueryId()));
    QueryUnitId qid1 = QueryIdFactory.newQueryUnitId(sid);
    QueryUnitId qid2 = QueryIdFactory.newQueryUnitId(sid);
    
    QueryContext ctx = qcFactory.create();
    ParseTree query = analyzer.parse(ctx, 
        "select deptName, sum(empId) as merge from employee group by deptName");
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    int numPartitions = 2;
    Column key1 = new Column("employee.deptName", DataType.STRING);
    StoreTableNode storeNode = new StoreTableNode("testInterQuery");
    storeNode.setPartitions(ScheduleUnit.PARTITION_TYPE.HASH, new Column[] { key1 }, numPartitions);
    PlannerUtil.insertNode(plan, storeNode);
    plan = LogicalOptimizer.optimize(ctx, plan);   
    
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
    Thread.sleep(1000);
    // for the report sending test
    NtaEngineMaster master = util.getMiniTajoCluster().getMaster();
    Collection<InProgressStatusProto> list = master.getProgressQueries();
    Set<QueryUnitId> submitted = Sets.newHashSet();
    submitted.add(req1.getId());
    submitted.add(req2.getId());
    
    assertSubmittedAndReported(master, submitted);
    
    Schema secSchema = plan.getOutputSchema();
    TableMeta secMeta = TCatUtil.newTableMeta(secSchema, StoreType.CSV);
    Path secData = sm.initLocalTableBase(new Path(TEST_PATH + "/sec"), secMeta);
    
    for (InProgressStatusProto ips : list) {
      if (ips.getStatus() == QueryStatus.FINISHED) {
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
          LOG.info(">>>>> Fetched: partition" + "(" + part.getPartitionKey()
              + ") " + fetched.getAbsolutePath() + " (size: "
              + fetched.length() + " bytes)");
          assertNotNull(fetched);
          sum += fetched.length();
        }
        TableStat stat = new TableStat(ips.getResultStats());
        assertEquals(sum, stat.getNumBytes().longValue());
      }
    }    
    
    // Receiver
    Schema newSchema = new Schema();
    newSchema.addColumn("col1", DataType.STRING);
    newSchema.addColumn("col2", DataType.INT);
    TableMeta newMeta = TCatUtil.newTableMeta(newSchema, StoreType.CSV);
    catalog.addTable(TCatUtil.newTableDesc("interquery", newMeta, new Path("/")));
    QueryUnitId qid3 = QueryIdFactory.newQueryUnitId(sid);
    ctx = qcFactory.create();
    query = analyzer.parse(ctx, 
        "select col1, col2 from interquery");
    plan = LogicalPlanner.createPlan(ctx, query);    
    storeNode = new StoreTableNode("final");
    PlannerUtil.insertNode(plan, storeNode);
    plan = LogicalOptimizer.optimize(ctx, plan);
    sm.initTableBase(TCatUtil.newTableMeta(plan.getOutputSchema(), StoreType.CSV), 
        "final");
    Fragment emptyFrag = new Fragment("interquery", new Path("/"), newMeta, 0l, 0l);
    QueryUnitRequest req3 = new QueryUnitRequestImpl(
        qid3, Lists.newArrayList(emptyFrag),
        "", false, plan.toJSON());
    assertTrue("InProgress list must be positive.", list.size() != 0);
    for (InProgressStatusProto ips : list) {
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
    Collection<InProgressStatusProto> list = master.getProgressQueries();
    int i = 0;
    while (i < 10) { // waiting for the report messages 
      LOG.info("Waiting for receiving the report messages");
      Thread.sleep(1000);
      list = master.getProgressQueries();
      reported.clear();
      for (InProgressStatusProto ips : list) {
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
    LOG.info("reported: " + reported);
    LOG.info("submitted: " + submitted);
    assertTrue(reported.containsAll(submitted));
  }
}
