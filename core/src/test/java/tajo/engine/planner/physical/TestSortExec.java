package tajo.engine.planner.physical;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.CatalogService;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.NtaConf;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.NtaTestingUtility;
import tajo.engine.SubqueryContext;
import tajo.engine.WorkerTestingUtil;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.parser.QueryBlock;
import tajo.engine.planner.LogicalOptimizer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.PhysicalPlanner;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.utils.TUtil;
import tajo.storage.Appender;
import tajo.storage.StorageManager;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertTrue;

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
    factory = new SubqueryContext.Factory();
    File workDir = NtaTestingUtility.getTestDir("TestSortExec");
    SubqueryContext ctx = factory.create(
        TUtil.newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    QueryBlock query = (QueryBlock) analyzer.parse(ctx, QUERIES[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);

    LogicalOptimizer.optimize(ctx, plan);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(conf, sm);
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
