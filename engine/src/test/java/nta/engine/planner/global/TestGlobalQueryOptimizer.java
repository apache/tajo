/**
 * 
 */
package nta.engine.planner.global;

import java.io.IOException;

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
import nta.engine.cluster.QueryManager;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.query.GlobalQueryPlanner;
import nta.storage.Appender;
import nta.storage.CSVFile2;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author jihoon
 * 
 */
public class TestGlobalQueryOptimizer {
  private static NtaTestingUtility util;
  private static NtaConf conf;
  private static CatalogService catalog;
  private static GlobalQueryPlanner planner;
  private static Schema schema;
  private static QueryContext.Factory factory;
  private static QueryAnalyzer analyzer;
  private static SubQueryId subQueryId;
  private static QueryManager qm;
  private static GlobalQueryOptimizer optimizer;

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

    qm = new QueryManager();
    planner = new GlobalQueryPlanner(new StorageManager(conf), qm, catalog);
    analyzer = new QueryAnalyzer(catalog);
    factory = new QueryContext.Factory(catalog);

    int tbNum = 2;
    int tupleNum;
    Appender appender;
    Tuple t = new VTuple(4);
    t.put(DatumFactory.createInt(1), DatumFactory.createInt(32),
        DatumFactory.createString("h"), DatumFactory.createInt(10));

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
    subQueryId = QueryIdFactory.newSubQueryId();
    optimizer = new GlobalQueryOptimizer();
  }
  
  @AfterClass
  public static void terminate() throws IOException {
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }

  @Test
  public void testReduceLogicalQueryUnitSteps() throws IOException {
    QueryContext ctx = factory.create();
    ParseTree tree = (ParseTree) analyzer.parse(ctx,
        "select table0.age,table0.salary,table1.salary from table0,table1 where table0.salary = table1.salary order by table0.age");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    LogicalQueryUnitGraph globalPlan = planner.build(subQueryId, logicalPlan);
    globalPlan = optimizer.optimize(globalPlan.getRoot());
    
    ScheduleUnit unit = globalPlan.getRoot();
    StoreTableNode store = unit.getStoreTableNode();
    assertEquals(ExprType.SORT, store.getSubNode().getType());
    SortNode sort = (SortNode) store.getSubNode();
    assertEquals(ExprType.SCAN, sort.getSubNode().getType());
    ScanNode scan = (ScanNode) sort.getSubNode();
    
    assertTrue(unit.hasPrevQuery());
    unit = unit.getPrevQuery(scan);
    store = unit.getStoreTableNode();
    assertEquals(ExprType.SORT, store.getSubNode().getType());
    sort = (SortNode) store.getSubNode();
    assertEquals(ExprType.JOIN, sort.getSubNode().getType());
    
    assertTrue(unit.hasPrevQuery());
    for (ScanNode prevscan : unit.getScanNodes()) {
      ScheduleUnit prev = unit.getPrevQuery(prevscan);
      store = prev.getStoreTableNode();
      assertEquals(ExprType.SCAN, store.getSubNode().getType());
    }
  }
}
