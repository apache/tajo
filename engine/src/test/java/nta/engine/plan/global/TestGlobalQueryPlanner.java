package nta.engine.plan.global;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import nta.catalog.CatalogService;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.DatumFactory;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.QueryIdFactory;
import nta.engine.SubQueryId;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.LogicalQueryUnit;
import nta.engine.planner.global.LogicalQueryUnit.Phase;
import nta.engine.planner.global.LogicalQueryUnitGraph;
import nta.engine.planner.logical.CreateTableNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.query.GlobalQueryPlanner;
import nta.storage.Appender;
import nta.storage.CSVFile2;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author jihoon
 * 
 */

public class TestGlobalQueryPlanner {

  private static NtaTestingUtility util;
  private static NtaConf conf;
  private static CatalogService catalog;
  private static GlobalQueryPlanner planner;
  private static Schema schema;
  private static QueryContext.Factory factory;
  private static QueryAnalyzer analyzer;
  private static SubQueryId subQueryId;

  @BeforeClass
  public static void setup() throws Exception {
    util = new NtaTestingUtility();

    int i, j;

    util.startMiniZKCluster();
    util.startCatalogCluster();

    schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.INT);
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("salary", DataType.INT);

    TableMeta meta;

    conf = new NtaConf(util.getConfiguration());
    catalog = util.getMiniCatalogCluster().getCatalog();
    StorageManager sm = new StorageManager(util.getConfiguration());
    FunctionDesc funcDesc = new FunctionDesc("sumtest", TestSum.class,
        FunctionType.GENERAL, DataType.INT, new DataType[] { DataType.INT });
    catalog.registerFunction(funcDesc);
    FileSystem fs = sm.getFileSystem();

    planner = new GlobalQueryPlanner(new StorageManager(conf));
    analyzer = new QueryAnalyzer(catalog);
    factory = new QueryContext.Factory(catalog);

    int tbNum = 2;
    int tupleNum;
    Appender appender;
    Tuple t = new VTuple(4);
    t.put(DatumFactory.createInt(1), DatumFactory.createInt(32),
        DatumFactory.createString("h"), DatumFactory.createInt(10));

    for (i = 0; i < tbNum; i++) {
      meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
      meta.putOption(CSVFile2.DELIMITER, ",");

      if (fs.exists(sm.getTablePath("table"+i))) {
        fs.delete(sm.getTablePath("table"+i), true);
      }
      appender = sm.getTableAppender(meta, "table" + i);
      tupleNum = 10000000;
      for (j = 0; j < tupleNum; j++) {
        appender.addTuple(t);
      }
      appender.close();

      TableDesc desc = TCatUtil.newTableDesc("table" + i, meta, sm.getTablePath("table"+i));
      catalog.addTable(desc);
    }

    QueryIdFactory.reset();
    subQueryId = QueryIdFactory.newSubQueryId();
  }

  @AfterClass
  public static void terminate() throws IOException {
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }
  
  @Test
  public void testBuildScanPlan() throws IOException {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx,
        "select age, sumtest(salary) from table0");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, block);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    LogicalQueryUnitGraph globalPlan = planner.build(subQueryId, logicalPlan);
    
    LogicalQueryUnit unit = globalPlan.getRoot();
    assertFalse(unit.hasPrevQuery());
    assertEquals(Phase.LOCAL, unit.getPhase());
    LogicalNode plan = unit.getLogicalPlan();
    assertEquals(ExprType.STORE, plan.getType());
    assertEquals(ExprType.SCAN, ((CreateTableNode)plan).getSubNode().getType());
  }

  @Test
  public void testBuildGroupbyPlan() throws IOException, KeeperException,
      InterruptedException {
    QueryContext ctx = factory.create();
    ParseTree tree = (ParseTree) analyzer.parse(ctx,
        "store1 := select age, sumtest(salary) from table0 group by age");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    LogicalQueryUnitGraph globalPlan = planner.build(subQueryId, logicalPlan);

    LogicalQueryUnit next, prev;
    
    next = globalPlan.getRoot();
    assertTrue(next.hasPrevQuery());
    assertEquals(Phase.MERGE, next.getPhase());
    Iterator<LogicalQueryUnit> it= next.getPrevIterator();
    
    prev = it.next();
    assertFalse(prev.hasPrevQuery());
    assertEquals(Phase.MAP, prev.getPhase());
    assertFalse(it.hasNext());
    
    ScanNode []scans = prev.getScanNodes();
    assertEquals(1, scans.length);
    assertEquals("table0", scans[0].getTableId());
    
    scans = next.getScanNodes();
    assertEquals(1, scans.length);
    CreateTableNode store = prev.getStoreTableNode();
    assertEquals(store.getTableName(), scans[0].getTableId());
    assertEquals(store.getOutputSchema(), scans[0].getInputSchema());
  }
  
  @Test
  public void testJoin() throws IOException {
    QueryContext ctx = factory.create();
    ParseTree tree = (ParseTree) analyzer.parse(ctx,
        "select table0.age from table0,table1 where table0.salary = table1.salary");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    LogicalQueryUnitGraph globalPlan = planner.build(subQueryId, logicalPlan);
    
  }
}
