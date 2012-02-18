package nta.engine.plan.global;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import nta.catalog.CatalogService;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.DatumFactory;
import nta.engine.NtaEngineMaster;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.QueryIdFactory;
import nta.engine.SubQueryId;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.GlobalQueryPlan;
import nta.engine.planner.global.QueryStep;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.QueryStep.Phase;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.UnaryNode;
import nta.engine.query.GlobalQueryPlanner;
import nta.storage.Appender;
import nta.storage.CSVFile2;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;
import nta.util.FileUtil;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.log.Log;

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
  private static NtaEngineMaster master;
  private static QueryContext.Factory factory;
  private static QueryAnalyzer analyzer;
  private static SubQueryId subQueryId;

  @BeforeClass
  public static void setup() throws Exception {
    util = new NtaTestingUtility();

    int i, j;
    FSDataOutputStream fos;
    Path tbPath;

    util.startMiniCluster(3);
    master = util.getMiniNtaEngineCluster().getMaster();

    schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.INT);
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("salary", DataType.INT);

    TableMeta meta;

    String[] tuples = { "1,32,hyunsik,10", "2,29,jihoon,20", "3,28,jimin,30",
        "4,24,haemi,40" };

    FileSystem fs = util.getMiniDFSCluster().getFileSystem();

    conf = new NtaConf(util.getConfiguration());
    catalog = master.getCatalog();
    StorageManager sm = new StorageManager(util.getConfiguration());
    FunctionDesc funcDesc = new FunctionDesc("sumtest", TestSum.class,
        FunctionType.GENERAL, DataType.INT, new DataType[] { DataType.INT });
    catalog.registerFunction(funcDesc);

    planner = new GlobalQueryPlanner(catalog, new StorageManager(conf));
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
    util.shutdownMiniCluster();
  }

  @Test
  public void testBuildGlobalPlan() throws IOException, KeeperException,
      InterruptedException {
    catalog.updateAllTabletServingInfo(master.getOnlineServer());

    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx,
        "store1 := select age, sumtest(salary) from table0 group by age");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, block);

    GlobalQueryPlan globalPlan = planner.build(subQueryId, logicalPlan);
    assertTrue(globalPlan.size() == 2);

    QueryStep step;
    QueryUnit unit;
    LogicalNode plan;
    assertEquals(globalPlan.getQueryStep(0).getQuery(0).getOutputName(),
        globalPlan.getQueryStep(1).getQuery(0).getInputName());
    for (int i = 0; i < globalPlan.size(); i++) {
      step = globalPlan.getQueryStep(i);
      for (int j = 0; j < step.size(); j++) {
        unit = step.getQuery(j);
        assertNotNull(unit.getInputSchema());
        assertNotNull(unit.getOutputSchema());
        assertNotNull(unit.getStoreTableNode());
        assertNotNull(unit.getScanNode());
        assertNotNull(unit.getUnaryNode());
        plan = unit.buildLogicalPlan();
        assertEquals(plan.getType(), ExprType.STORE);
        plan = ((UnaryNode) plan).getSubNode();
        assertEquals(plan.getType(), ExprType.GROUP_BY);
        plan = ((UnaryNode) plan).getSubNode();
        assertEquals(plan.getType(), ExprType.SCAN);
      }
    }
  }

//  @Test
//  public void testLocalize() throws IOException, KeeperException,
//      InterruptedException {
//    catalog.updateAllTabletServingInfo(master.getOnlineServer());
//
//    QueryContext ctx = factory.create();
//    QueryBlock block = analyzer.parse(ctx,
//        "store1 := select age, sumtest(salary) from table0 group by age");
//    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, block);
//
//    GlobalQueryPlan globalPlan = planner.build(subQueryId, logicalPlan);
//    assertTrue(globalPlan.size() == 2);
//
//    QueryStep step, localized;
//    QueryUnit unit;
//    List<Fragment> frags;
//    for (int i = 0; i < globalPlan.size(); i++) {
//      step = globalPlan.getQueryStep(i);
//      localized = planner.localize(step, 3);
//      // fragment
//      for (int j = 0; j < localized.size(); j++) {
//        unit = localized.getQuery(j);
//        assertNotNull(unit.getFragments());
//        assertTrue(unit.getFragments().size() > 0);
//        if (step.getPhase() == Phase.MAP) {
//          // hash
//          frags = unit.getFragments();
//          for (int k = 0; k < frags.size() - 1; k++) {
//            assertEquals(frags.get(k).getPath().getName(), frags.get(k + 1)
//                .getPath().getName());
//          }
//        } else {
//
//        }
//      }
//    }
//  }

  // @Test
  // public void testGroupbyAndStore() throws IOException, KeeperException,
  // InterruptedException {
  // catalog.updateAllTabletServingInfo(master.getOnlineServer());
  //
  // QueryContext ctx = factory.create();
  // QueryBlock block = analyzer.parse(ctx,
  // "store1 := select age, sumtest(salary) from table0 group by age");
  // LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, block);
  // LogicalOptimizer.optimize(ctx, logicalPlan);
  //
  // GlobalQueryPlan globalPlan = planner.build(logicalPlan);
  // assertTrue(globalPlan.size() > 0);
  //
  // QueryStep step = globalPlan.getQueryStep(0);
  // assertTrue(step.size() > 0);
  // Log.info("QueryStep size: " + step.size());
  // QueryUnit q = step.getQuery(0);
  // assertEquals(q.getOp().getType(), ExprType.STORE);
  // StoreTableNode store1 = (StoreTableNode)q.getOp();
  // assertEquals(store1.getSubNode().getType(), ExprType.GROUP_BY);
  // GroupbyNode groupby = (GroupbyNode)store1.getSubNode();
  // assertEquals(groupby.getSubNode().getType(), ExprType.SCAN);
  //
  // step = globalPlan.getQueryStep(1);
  // assertTrue(step.size() > 0);
  // Log.info("QueryStep size: " + step.size());
  // q = step.getQuery(0);
  // assertEquals(q.getOp().getType(), ExprType.STORE);
  // StoreTableNode store2 = (StoreTableNode)q.getOp();
  // assertEquals(store2.getSubNode().getType(), ExprType.GROUP_BY);
  // groupby = (GroupbyNode)store2.getSubNode();
  // assertEquals(groupby.getSubNode().getType(), ExprType.SCAN);
  // ScanNode scan = (ScanNode)groupby.getSubNode();
  //
  // assertNotNull(scan.getInputSchema());
  // assertNotNull(store1.getOutputSchema());
  // assertEquals(scan.getInputSchema(), store1.getOutputSchema());
  // assertNotNull(scan.getTableId());
  // assertNotNull(store1.getTableName());
  // assertEquals(scan.getTableId(), store1.getTableName());
  // }
  //
  // @Test
  // public void testScan() throws IOException, KeeperException,
  // InterruptedException {
  // catalog.updateAllTabletServingInfo(master.getOnlineServer());
  //
  // QueryContext ctx = factory.create();
  // QueryBlock block = analyzer.parse(ctx, "select age from table0");
  // LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, block);
  // LogicalOptimizer.optimize(ctx, logicalPlan);
  //
  // GlobalQueryPlan globalPlan = planner.build(logicalPlan);
  // assertTrue(globalPlan.size() > 0);
  //
  // QueryStep step = globalPlan.getQueryStep(0);
  // assertTrue(step.size() > 0);
  // Log.info("QueryStep size: " + step.size());
  //
  // QueryUnit q = step.getQuery(0);
  // assertEquals(q.getOp().getType(), ExprType.STORE);
  // StoreTableNode store = (StoreTableNode)q.getOp();
  // assertEquals(store.getSubNode().getType(), ExprType.SCAN);
  // }
  //
  // @Test
  // public void testSort() throws IOException, KeeperException,
  // InterruptedException {
  // catalog.updateAllTabletServingInfo(master.getOnlineServer());
  //
  // QueryContext ctx = factory.create();
  // QueryBlock block = analyzer.parse(ctx,
  // "select age from table0 order by age");
  // LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, block);
  // LogicalOptimizer.optimize(ctx, logicalPlan);
  //
  // GlobalQueryPlan globalPlan = planner.build(logicalPlan);
  // assertTrue(globalPlan.size() > 0);
  //
  // QueryStep step = globalPlan.getQueryStep(0);
  // assertTrue(step.size() > 0);
  // Log.info("QueryStep size: " + step.size());
  //
  // QueryUnit q = step.getQuery(0);
  // assertEquals(q.getOp().getType(), ExprType.STORE);
  // StoreTableNode store1 = (StoreTableNode)q.getOp();
  // assertEquals(store1.getSubNode().getType(), ExprType.SORT);
  // SortNode sort = (SortNode)store1.getSubNode();
  // assertEquals(sort.getSubNode().getType(), ExprType.SCAN);
  //
  // step = globalPlan.getQueryStep(1);
  // assertTrue(step.size() > 0);
  // Log.info("QueryStep size: " + step.size());
  // q = step.getQuery(0);
  // assertEquals(q.getOp().getType(), ExprType.STORE);
  // StoreTableNode store = (StoreTableNode)q.getOp();
  // assertEquals(store.getSubNode().getType(), ExprType.SORT);
  // sort = (SortNode)store.getSubNode();
  // assertEquals(sort.getSubNode().getType(), ExprType.SCAN);
  // ScanNode scan = (ScanNode)sort.getSubNode();
  //
  // assertNotNull(scan.getInputSchema());
  // assertNotNull(store1.getOutputSchema());
  // assertEquals(scan.getInputSchema(), store1.getOutputSchema());
  // assertNotNull(scan.getTableId());
  // assertNotNull(store1.getTableName());
  // assertEquals(scan.getTableId(), store1.getTableName());
  // }
}
