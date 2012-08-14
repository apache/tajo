package nta.engine.planner.physical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import nta.catalog.CatalogService;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.WorkerTestingUtil;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryIdFactory;
import nta.engine.SubqueryContext;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.PhysicalPlanner;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.utils.TUtil;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBNLJoinExec {
  private Configuration conf;
  private final String TEST_PATH = "target/test-data/TestNLJoinExec";
  private NtaTestingUtility util;
  private CatalogService catalog;
  private QueryAnalyzer analyzer;
  private SubqueryContext.Factory factory;
  private StorageManager sm;

  private static int OUTER_TUPLE_NUM = 1000;
  private static int INNER_TUPLE_NUM = 1000;

  @Before
  public void setUp() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniZKCluster();
    catalog = util.startCatalogCluster().getCatalog();
    WorkerTestingUtil.buildTestDir(TEST_PATH);
    conf = util.getConfiguration();
    sm = StorageManager.get(conf, TEST_PATH);

    Schema schema = new Schema();
    schema.addColumn("managerId", DataType.INT);
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("memId", DataType.INT);
    schema.addColumn("deptName", DataType.STRING);

    TableMeta employeeMeta = TCatUtil.newTableMeta(schema, StoreType.RAW);
    sm.initTableBase(employeeMeta, "employee");
    Appender appender = sm.getAppender(employeeMeta, "employee", "employee");
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());
    for (int i = 0; i < OUTER_TUPLE_NUM; i++) {
      tuple.put(new Datum[] { DatumFactory.createInt(i),
          DatumFactory.createInt(i), DatumFactory.createInt(10 + i),
          DatumFactory.createString("dept_" + i) });
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
    TableDesc employee = TCatUtil.newTableDesc("employee", employeeMeta,
        sm.getTablePath("people"));
    catalog.addTable(employee);

    Schema peopleSchema = new Schema();
    peopleSchema.addColumn("empId", DataType.INT);
    peopleSchema.addColumn("fk_memId", DataType.INT);
    peopleSchema.addColumn("name", DataType.STRING);
    peopleSchema.addColumn("age", DataType.INT);
    TableMeta peopleMeta = TCatUtil.newTableMeta(peopleSchema, StoreType.RAW);
    sm.initTableBase(peopleMeta, "people");
    appender = sm.getAppender(peopleMeta, "people", "people");
    tuple = new VTuple(peopleMeta.getSchema().getColumnNum());
    for (int i = 1; i < INNER_TUPLE_NUM; i += 2) {
      tuple.put(new Datum[] { DatumFactory.createInt(i),
          DatumFactory.createInt(10 + i),
          DatumFactory.createString("name_" + i),
          DatumFactory.createInt(30 + i) });
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    TableDesc people = TCatUtil.newTableDesc("people", peopleMeta,
        sm.getTablePath("people"));
    catalog.addTable(people);
    analyzer = new QueryAnalyzer(catalog);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }

  String[] QUERIES = {
      "select managerId, e.empId, deptName, e.memId from employee as e, people",
      "select managerId, e.empId, deptName, e.memId from employee as e inner join people as p on e.empId = p.empId and e.memId = p.fk_memId" };

  @Test
  public final void testCrossJoin() throws IOException {
    Fragment[] empFrags = sm.split("employee");
    Fragment[] peopleFrags = sm.split("people");

    Fragment[] merged = TUtil.concat(empFrags, peopleFrags);

    factory = new SubqueryContext.Factory();
    File workDir = NtaTestingUtility.getTestDir("CrossJoin");
    SubqueryContext ctx = factory.create(TUtil.newQueryUnitAttemptId(),
        merged, workDir);
    QueryBlock query = (QueryBlock) analyzer.parse(ctx, QUERIES[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    //LogicalOptimizer.optimize(ctx, plan);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    /*ProjectionExec proj = (ProjectionExec) exec;
    NLJoinExec nlJoin = (NLJoinExec) proj.getChild();
    SeqScanExec scanOuter = (SeqScanExec) nlJoin.getOuter();
    SeqScanExec scanInner = (SeqScanExec) nlJoin.getInner();

    BNLJoinExec bnl = new BNLJoinExec(ctx, nlJoin.getJoinNode(), scanOuter,
        scanInner);
    proj.setsubOp(bnl);*/

    int i = 0;
    while (exec.next() != null) {
      i++;
    }
    assertEquals(OUTER_TUPLE_NUM * INNER_TUPLE_NUM / 2, i); // expected 10 * 5
  }

  @Test
  public final void testInnerJoin() throws IOException {
    Fragment[] empFrags = sm.split("employee");
    Fragment[] peopleFrags = sm.split("people");

    Fragment[] merged = TUtil.concat(empFrags, peopleFrags);

    factory = new SubqueryContext.Factory();
    File workDir = NtaTestingUtility.getTestDir("InnerJoin");
    SubqueryContext ctx = factory.create(TUtil.newQueryUnitAttemptId(),
        merged, workDir);
    QueryBlock query = (QueryBlock) analyzer.parse(ctx, QUERIES[1]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    System.out.println(plan);
    // LogicalOptimizer.optimize(ctx, plan);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    SeqScanExec scanOuter = null;
    SeqScanExec scanInner = null;

    ProjectionExec proj = (ProjectionExec) exec;
    JoinNode joinNode = null;
    if (proj.getChild() instanceof MergeJoinExec) {
      MergeJoinExec join = (MergeJoinExec) proj.getChild();
      ExternalSortExec sortOut = (ExternalSortExec) join.getOuter();
      ExternalSortExec sortIn = (ExternalSortExec) join.getInner();
      scanOuter = (SeqScanExec) sortOut.getSubOp();
      scanInner = (SeqScanExec) sortIn.getSubOp();
      joinNode = join.getJoinNode();
    } else if (proj.getChild() instanceof HashJoinExec) {
      HashJoinExec join = (HashJoinExec) proj.getChild();
      scanOuter = (SeqScanExec) join.getOuter();
      scanInner = (SeqScanExec) join.getInner();
      joinNode = join.getPlan();
    }

    BNLJoinExec bnl = new BNLJoinExec(ctx, joinNode, scanOuter,
        scanInner);
    proj.setChild(bnl);

    Tuple tuple;
    int i = 1;
    int count = 0;
    while ((tuple = exec.next()) != null) {
      count++;
      assertTrue(i == tuple.getInt(0).asInt());
      assertTrue(i == tuple.getInt(1).asInt());
      assertTrue(("dept_" + i).equals(tuple.getString(2).asChars()));
      assertTrue(10 + i == tuple.getInt(3).asInt());
      i += 2;
    }
    assertEquals(INNER_TUPLE_NUM / 2, count);
  }
}
