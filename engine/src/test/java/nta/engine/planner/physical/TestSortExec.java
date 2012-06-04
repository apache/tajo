package nta.engine.planner.physical;

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
import nta.engine.WorkerTestingUtil;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryIdFactory;
import nta.engine.SubqueryContext;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalOptimizer;
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
 * @author Hyunsik Choi
 */
public class TestSortExec {
  private NtaConf conf;
  private final String TEST_PATH = "target/test-data/TestPhysicalPlanner";
  private CatalogService catalog;
  private QueryAnalyzer analyzer;
  private SubqueryContext.Factory factory;
  private StorageManager sm;

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
    for (int i = 0; i < 100; i++) {
      tuple.put(new Datum[] {
          DatumFactory.createInt(rnd.nextInt(5)),
          DatumFactory.createInt(rnd.nextInt(10)),
          DatumFactory.createString("dept_" + rnd.nextInt(10))});
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    analyzer = new QueryAnalyzer(catalog);
  }

  @After
  public void tearDown() throws Exception {

  }

  public static String[] QUERIES = {
      "select managerId, empId, deptName from employee order by managerId, empId desc" };

  @Test
  public final void testNext() throws IOException {
    Fragment[] frags = sm.split("employee");
    factory = new SubqueryContext.Factory(catalog);
    File workDir = NtaTestingUtility.getTestDir("TestSortExec");
    SubqueryContext ctx = factory.create(QueryIdFactory.newQueryUnitId(
        QueryIdFactory.newScheduleUnitId(
            QueryIdFactory.newSubQueryId(
                QueryIdFactory.newQueryId()))),
        new Fragment[] { frags[0] }, workDir);
    QueryBlock query = (QueryBlock) analyzer.parse(ctx, QUERIES[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);

    LogicalOptimizer.optimize(ctx, plan);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    Tuple tuple;
    Datum preVal = null;
    Datum curVal;
    while ((tuple = exec.next()) != null) {
      curVal = tuple.get(0);
      if (preVal != null) {
        assertTrue(preVal.lessThanEqual(curVal).asBool());
      }

      preVal = curVal;
    }
  }
}
