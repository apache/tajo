/**
 * 
 */
package tajo.engine.planner.global;

import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.QueryContext;
import tajo.engine.QueryIdFactory;
import tajo.engine.SubQueryId;
import tajo.engine.TajoTestingUtility;
import tajo.engine.cluster.QueryManager;
import tajo.engine.exec.eval.TestEvalTree.TestSum;
import tajo.engine.parser.ParseTree;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalOptimizer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.logical.*;
import tajo.engine.query.GlobalPlanner;
import tajo.storage.*;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author jihoon
 * 
 */
public class TestGlobalQueryOptimizer {
  private static TajoTestingUtility util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static GlobalPlanner planner;
  private static Schema schema;
  private static QueryContext.Factory factory;
  private static QueryAnalyzer analyzer;
  private static SubQueryId subQueryId;
  private static QueryManager qm;
  private static GlobalOptimizer optimizer;

  @BeforeClass
  public static void setup() throws Exception {
    util = new TajoTestingUtility();

    int i, j;

    util.startMiniZKCluster();
    util.startCatalogCluster();

    schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.INT);
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("salary", DataType.INT);

    TableMeta meta;

    conf = new TajoConf(util.getConfiguration());
    catalog = util.getMiniCatalogCluster().getCatalog();
    StorageManager sm = new StorageManager(util.getConfiguration());
    FunctionDesc funcDesc = new FunctionDesc("sumtest", TestSum.class, FunctionType.GENERAL,
        new DataType [] {DataType.INT},
        new DataType [] {DataType.INT});
    catalog.registerFunction(funcDesc);
    FileSystem fs = sm.getFileSystem();

    qm = new QueryManager();
    planner = new GlobalPlanner(conf, new StorageManager(conf), qm, catalog);
    analyzer = new QueryAnalyzer(catalog);
    factory = new QueryContext.Factory(catalog);

    int tbNum = 2;
    int tupleNum;
    Appender appender;
    Tuple t = new VTuple(4);
    t.put(new Datum[] {
        DatumFactory.createInt(1), DatumFactory.createInt(32),
        DatumFactory.createString("h"), DatumFactory.createInt(10)});

    for (i = 0; i < tbNum; i++) {
      meta = TCatUtil.newTableMeta((Schema)schema.clone(), StoreType.CSV);
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

      TableDesc desc = TCatUtil.newTableDesc("table" + i, (TableMeta)meta.clone(), sm.getTablePath("table"+i));
      catalog.addTable(desc);
    }

    QueryIdFactory.reset();
    subQueryId = QueryIdFactory.newSubQueryId(QueryIdFactory.newQueryId());
    optimizer = new GlobalOptimizer();
  }
  
  @AfterClass
  public static void terminate() throws IOException {
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }

  @Test
  public void testReduceLogicalQueryUnitSteps() throws IOException {
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx,
        "select table0.age,table0.salary,table1.salary from table0,table1 where table0.salary = table1.salary order by table0.age");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    MasterPlan globalPlan = planner.build(subQueryId, logicalPlan);
    globalPlan = optimizer.optimize(globalPlan.getRoot());
    
    ScheduleUnit unit = globalPlan.getRoot();
    StoreTableNode store = unit.getStoreTableNode();
    assertEquals(ExprType.PROJECTION, store.getSubNode().getType());
    ProjectionNode proj = (ProjectionNode) store.getSubNode();
    assertEquals(ExprType.SORT, proj.getSubNode().getType());
    SortNode sort = (SortNode) proj.getSubNode();
    assertEquals(ExprType.SCAN, sort.getSubNode().getType());
    ScanNode scan = (ScanNode) sort.getSubNode();
    
    assertTrue(unit.hasChildQuery());
    unit = unit.getChildQuery(scan);
    store = unit.getStoreTableNode();
    assertEquals(ExprType.SORT, store.getSubNode().getType());
    sort = (SortNode) store.getSubNode();
    assertEquals(ExprType.JOIN, sort.getSubNode().getType());
    
    assertTrue(unit.hasChildQuery());
    for (ScanNode prevscan : unit.getScanNodes()) {
      ScheduleUnit prev = unit.getChildQuery(prevscan);
      store = prev.getStoreTableNode();
      assertEquals(ExprType.SCAN, store.getSubNode().getType());
    }
  }
}
