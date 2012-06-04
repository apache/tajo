package nta.engine.planner.physical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import nta.catalog.CatalogService;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryIdFactory;
import nta.engine.SubqueryContext;
import nta.engine.WorkerTestingUtil;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.PhysicalPlanner;
import nta.engine.planner.logical.LogicalNode;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Byungnam Lim
 */
public class TestExternalSortExec {
  private NtaConf conf;
  private final String TEST_PATH = "target/test-data/TestExternalSortExec";
  private CatalogService catalog;
  private QueryAnalyzer analyzer;
  private SubqueryContext.Factory factory;
  private StorageManager sm;

  private final int numTuple = 100000;
  private Random rnd = new Random(System.currentTimeMillis());

  @Before
  public void setUp() throws Exception {
    this.conf = new NtaConf();
    WorkerTestingUtil.buildTestDir(TEST_PATH);
    sm = StorageManager.get(conf, TEST_PATH);

    Schema schema = new Schema();
    schema.addColumn("managerId", DataType.INT);
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("deptName", DataType.STRING);

    TableMeta employeeMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    sm.initTableBase(employeeMeta, "employee");
    Appender appender = sm.getAppender(employeeMeta, "employee", "employee");
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());
    for (int i = 0; i < numTuple; i++) {
      tuple.put(new Datum[] { DatumFactory.createInt(rnd.nextInt(50)),
          DatumFactory.createInt(rnd.nextInt(100)),
          DatumFactory.createString("dept_" + rnd.nextInt(20)) });
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    analyzer = new QueryAnalyzer(catalog);
  }

  @After
  public void tearDown() throws Exception {

  }

  String[] QUERIES = { "select managerId, empId, deptName from employee order by managerId, empId desc" };

  @Test
  public final void testNext() throws IOException {
    Fragment[] frags = sm.split("employee");
    factory = new SubqueryContext.Factory(catalog);
    File workDir = NtaTestingUtility.getTestDir("TestExteranlSortExec");
    SubqueryContext ctx = factory.create(QueryIdFactory
        .newQueryUnitId(QueryIdFactory.newScheduleUnitId(QueryIdFactory
            .newSubQueryId(QueryIdFactory.newQueryId()))),
        new Fragment[] { frags[0] }, workDir);
    QueryBlock query = (QueryBlock) analyzer.parse(ctx, QUERIES[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);
    
    ProjectionExec proj = (ProjectionExec) exec;
    SortExec inMemSort = (SortExec) proj.getSubOp();
    SeqScanExec scan = (SeqScanExec)inMemSort.getSubOp();
  
    ExternalSortExec extSort = new ExternalSortExec(ctx, sm, inMemSort.getSortNode(), scan);
    proj.setsubOp(extSort);

    Tuple tuple;
    Datum preVal = null;
    Datum curVal;
    int cnt = 0;
    while ((tuple = proj.next()) != null) {
      curVal = tuple.get(0);
      if (preVal != null) {
        assertTrue(preVal.lessThanEqual(curVal).asBool());
      }
      preVal = curVal;
      cnt++;
    }
    assertEquals(numTuple, cnt);

    // for rescan test
    preVal = null;
    proj.rescan();
    cnt = 0;
    while ((tuple = proj.next()) != null) {
      curVal = tuple.get(0);
      if (preVal != null) {
        assertTrue(preVal.lessThanEqual(curVal).asBool());
      }
      preVal = curVal;
      cnt++;
    }
    assertEquals(numTuple, cnt);
  }
}
