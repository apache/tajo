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
import tajo.engine.TajoTestingUtility;
import tajo.engine.SubqueryContext;
import tajo.engine.WorkerTestingUtil;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.parser.QueryBlock;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    factory = new SubqueryContext.Factory();
    File workDir = TajoTestingUtility.getTestDir("TestExteranlSortExec");
    SubqueryContext ctx = factory.create(TUtil.newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    QueryBlock query = (QueryBlock) analyzer.parse(ctx, QUERIES[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);
    
/*    ProjectionExec proj = (ProjectionExec) exec;
    SortExec inMemSort = (SortExec) proj.getChild();
    SeqScanExec scan = (SeqScanExec)inMemSort.getChild();
  
    ExternalSortExec extSort = new ExternalSortExec(ctx, sm, inMemSort.getSortNode(), scan);
    proj.setChild(extSort);*/

    Tuple tuple;
    Datum preVal = null;
    Datum curVal;
    int cnt = 0;
    while ((tuple = exec.next()) != null) {
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
    exec.rescan();
    cnt = 0;
    while ((tuple = exec.next()) != null) {
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
