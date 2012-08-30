package tajo.worker;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.*;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.MasterInterfaceProtos.*;
import tajo.engine.cluster.QueryManager;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.ipc.protocolrecords.QueryUnitRequest;
import tajo.engine.parser.ParseTree;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalOptimizer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.global.QueryUnit;
import tajo.engine.planner.global.QueryUnitAttempt;
import tajo.engine.planner.global.ScheduleUnit;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.logical.StoreTableNode;
import tajo.engine.query.QueryUnitRequestImpl;
import tajo.engine.utils.TUtil;
import tajo.master.Query;
import tajo.master.SubQuery;
import tajo.master.TajoMaster;
import tajo.storage.*;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Hyunsik Choi
 */
public class TestLeafServer {
  private final Log LOG = LogFactory.getLog(TestLeafServer.class);
  private TajoConf conf;
  private TajoTestingUtility util;
  private String TEST_PATH = "target/test-data/TestLeafServer";
  private StorageManager sm;
  private CatalogService catalog;
  private QueryAnalyzer analyzer;
  private QueryContext.Factory qcFactory;
  private static int tupleNum = 10000;

  @Before
  public void setUp() throws Exception {
    WorkerTestingUtil.buildTestDir(TEST_PATH);
    util = new TajoTestingUtility();
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
        QueryIdFactory.newQueryUnitAttemptId(qid1, 0),
        Lists.newArrayList(Arrays.copyOfRange(frags, 0, splitIdx)),
        "", false, plan.toJSON());
    
    QueryUnitRequest req2 = new QueryUnitRequestImpl(
        QueryIdFactory.newQueryUnitAttemptId(qid2, 0),
        Lists.newArrayList(Arrays.copyOfRange(frags, splitIdx,
            frags.length)), "", false, plan.toJSON());

    assertNotNull(leaf1.requestQueryUnit(req1.getProto()));
    Thread.sleep(1000);
    assertNotNull(leaf2.requestQueryUnit(req2.getProto()));
    Thread.sleep(1000);
    
    // for the report sending test
    TajoMaster master = util.getMiniTajoCluster().getMaster();
    Set<QueryUnitAttemptId> submitted = Sets.newHashSet();
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

  @Test
  public final void testCommand() throws Exception {
    Fragment[] frags = sm.split("employee", 40000);
    for (Fragment f : frags) {
      f.setDistCached();
    }

    for (Fragment f : frags) {
      System.out.println(f);
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
        "testLeafServer := select name, empId, deptName from employee");
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    plan = LogicalOptimizer.optimize(ctx, plan);

    TajoMaster master = util.getMiniTajoCluster().getMaster();
    QueryManager qm = master.getQueryManager();
    Query q = new Query(qid1.getQueryId(),
        "testLeafServer := select name, empId, deptName from employee");
    qm.addQuery(q);
    SubQuery subQuery = new SubQuery(qid1.getSubQueryId());
    qm.addSubQuery(subQuery);
    ScheduleUnit su = new ScheduleUnit(qid1.getScheduleUnitId());
    qm.addScheduleUnit(su);
    sm.initTableBase(frags[0].getMeta(), "testLeafServer");
    QueryUnit [] qu = new QueryUnit[2];
    qu[0] = new QueryUnit(qid1);
    qu[1] = new QueryUnit(qid2);
    su.setQueryUnits(qu);
    qu[0].setStatus(QueryStatus.QUERY_INITED);
    qu[1].setStatus(QueryStatus.QUERY_INITED);
    QueryUnitAttempt attempt0 = qu[0].newAttempt();
    QueryUnitAttempt attempt1 = qu[1].newAttempt();
    QueryUnitRequest req1 = new QueryUnitRequestImpl(
        attempt0.getId(),
        Lists.newArrayList(Arrays.copyOfRange(frags, 0, splitIdx)),
        "", false, plan.toJSON());

    QueryUnitRequest req2 = new QueryUnitRequestImpl(
        attempt1.getId(),
        Lists.newArrayList(Arrays.copyOfRange(frags, splitIdx,
        frags.length)), "", false, plan.toJSON());

    assertNotNull(leaf1.requestQueryUnit(req1.getProto()));
    Thread.sleep(1000);
    assertNotNull(leaf2.requestQueryUnit(req2.getProto()));
    Thread.sleep(1000);

    // for the report sending test
    Set<QueryUnitAttemptId> submitted = Sets.newHashSet();
    submitted.add(req1.getId());
    submitted.add(req2.getId());

    QueryStatus s1, s2;
    do {
      s1 = leaf1.getTask(req1.getId()).getStatus();
      s2 = leaf2.getTask(req2.getId()).getStatus();
    } while (s1 != QueryStatus.QUERY_FINISHED
        && s2 != QueryStatus.QUERY_FINISHED);

    Command.Builder cmd = Command.newBuilder();
    cmd.setId(req1.getId().getProto()).setType(CommandType.FINALIZE);
    leaf1.requestCommand(CommandRequestProto.newBuilder().
        addCommand(cmd.build()).build());
    cmd = Command.newBuilder();
    cmd.setId(req2.getId().getProto()).setType(CommandType.FINALIZE);
    leaf2.requestCommand(CommandRequestProto.newBuilder().
        addCommand(cmd.build()).build());

    assertNull(leaf1.getTask(req1.getId()));
    assertNull(leaf2.getTask(req2.getId()));

    Scanner scanner = sm.getTableScanner("testLeafServer");
    int j = 0;
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
        TUtil.newQueryUnitAttemptId(),
        Lists.newArrayList(Arrays.copyOfRange(frags, 0, splitIdx)),
        "", false, plan.toJSON());
    req1.setInterQuery();
    
    QueryUnitRequest req2 = new QueryUnitRequestImpl(
        TUtil.newQueryUnitAttemptId(),
        Lists.newArrayList(Arrays.copyOfRange(frags, splitIdx,
            frags.length)), "", false, plan.toJSON());
    req2.setInterQuery();

    assertNotNull(leaf1.requestQueryUnit(req1.getProto()));
    Thread.sleep(1000);
    assertNotNull(leaf2.requestQueryUnit(req2.getProto()));
    Thread.sleep(1000);
    // for the report sending test
    TajoMaster master = util.getMiniTajoCluster().getMaster();
    Collection<InProgressStatusProto> list = master.getProgressQueries();
    Set<QueryUnitAttemptId> submitted = Sets.newHashSet();
    submitted.add(req1.getId());
    submitted.add(req2.getId());
    
    assertSubmittedAndReported(master, submitted);
    
    Schema secSchema = plan.getOutputSchema();
    TableMeta secMeta = TCatUtil.newTableMeta(secSchema, StoreType.CSV);
    Path secData = sm.initLocalTableBase(new Path(TEST_PATH + "/sec"), secMeta);
    
    for (InProgressStatusProto ips : list) {
      if (ips.getStatus() == QueryStatus.QUERY_FINISHED) {
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
        TUtil.newQueryUnitAttemptId(), Lists.newArrayList(emptyFrag),
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
  
  public void assertSubmittedAndReported(TajoMaster master,
      Set<QueryUnitAttemptId> submitted) throws InterruptedException {
    Set<QueryUnitAttemptId> reported = Sets.newHashSet();
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
        if (ips.getStatus() == QueryStatus.QUERY_FINISHED) {
          reported.add(new QueryUnitAttemptId(ips.getId()));
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
