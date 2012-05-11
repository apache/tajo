package nta.engine.plan.global;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
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
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.MasterInterfaceProtos.InProgressStatusProto;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.QueryIdFactory;
import nta.engine.SubQueryId;
import nta.engine.cluster.QueryManager;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.global.ScheduleUnit.PARTITION_TYPE;
import nta.engine.planner.global.MasterPlan;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.UnionNode;
import nta.engine.query.GlobalPlanner;
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
  private static GlobalPlanner planner;
  private static Schema schema;
  private static QueryContext.Factory factory;
  private static QueryAnalyzer analyzer;
  private static SubQueryId subQueryId;
  private static QueryManager qm;

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
    planner = new GlobalPlanner(new StorageManager(conf), qm, catalog);
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
  }

  @AfterClass
  public static void terminate() throws IOException {
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }
  
  @Test
  public void testScan() throws IOException {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx,
        "select age, sumtest(salary) from table0");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, block);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    MasterPlan globalPlan = planner.build(subQueryId, logicalPlan);
    
    ScheduleUnit unit = globalPlan.getRoot();
    assertFalse(unit.hasChildQuery());
    assertEquals(PARTITION_TYPE.LIST, unit.getOutputType());
    LogicalNode plan = unit.getLogicalPlan();
    assertEquals(ExprType.STORE, plan.getType());
    assertEquals(ExprType.SCAN, ((StoreTableNode)plan).getSubNode().getType());
  }

  @Test
  public void testGroupby() throws IOException, KeeperException,
      InterruptedException {
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx,
        "store1 := select age, sumtest(salary) from table0 group by age");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    MasterPlan globalPlan = planner.build(subQueryId, logicalPlan);

    ScheduleUnit next, prev;
    
    next = globalPlan.getRoot();
    assertTrue(next.hasChildQuery());
    assertEquals(PARTITION_TYPE.LIST, next.getOutputType());
    for (ScanNode scan : next.getScanNodes()) {
      assertTrue(scan.isLocal());
    }
    assertFalse(next.getStoreTableNode().isLocal());
    Iterator<ScheduleUnit> it= next.getChildIterator();
    
    prev = it.next();
    assertFalse(prev.hasChildQuery());
    assertEquals(PARTITION_TYPE.HASH, prev.getOutputType());
    assertTrue(prev.getStoreTableNode().isLocal());
    assertFalse(it.hasNext());
    
    ScanNode []scans = prev.getScanNodes();
    assertEquals(1, scans.length);
    assertEquals("table0", scans[0].getTableId());
    assertFalse(scans[0].isLocal());
    
    scans = next.getScanNodes();
    assertEquals(1, scans.length);
    StoreTableNode store = prev.getStoreTableNode();
    assertEquals(store.getTableName(), scans[0].getTableId());
    assertEquals(store.getOutputSchema(), scans[0].getInputSchema());
  }
  
  @Test
  public void testSort() throws IOException {
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx,
        "store1 := select age from table0 order by age");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    MasterPlan globalPlan = planner.build(subQueryId, logicalPlan);
    
    ScheduleUnit next, prev;
    
    next = globalPlan.getRoot();
    assertTrue(next.hasChildQuery());
    assertEquals(PARTITION_TYPE.LIST, next.getOutputType());
    Iterator<ScheduleUnit> it= next.getChildIterator();
    
    prev = it.next();
    assertFalse(prev.hasChildQuery());
    assertEquals(PARTITION_TYPE.HASH, prev.getOutputType());
    assertFalse(it.hasNext());
    
    ScanNode []scans = prev.getScanNodes();
    assertEquals(1, scans.length);
    assertEquals("table0", scans[0].getTableId());
    
    scans = next.getScanNodes();
    assertEquals(1, scans.length);
    StoreTableNode store = prev.getStoreTableNode();
    assertEquals(store.getTableName(), scans[0].getTableId());
    assertEquals(store.getOutputSchema(), scans[0].getInputSchema());
  }
  
  @Test
  public void testJoin() throws IOException {
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx,
        "select table0.age,table0.salary,table1.salary from table0,table1 where table0.salary = table1.salary order by table0.age");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    MasterPlan globalPlan = planner.build(subQueryId, logicalPlan);
    
    ScheduleUnit next, prev;
    
    // the second phase of the sort
    next = globalPlan.getRoot();
    assertTrue(next.hasChildQuery());
    assertEquals(PARTITION_TYPE.LIST, next.getOutputType());
    assertEquals(ExprType.SORT, next.getStoreTableNode().getSubNode().getType());
    ScanNode []scans = next.getScanNodes();
    assertEquals(1, scans.length);
    Iterator<ScheduleUnit> it= next.getChildIterator();
    
    // the first phase of the sort
    prev = it.next();
    assertEquals(ExprType.SORT, prev.getStoreTableNode().getSubNode().getType());
    assertEquals(scans[0].getInputSchema(), prev.getOutputSchema());
    assertTrue(prev.hasChildQuery());
    assertEquals(PARTITION_TYPE.HASH, prev.getOutputType());
    assertFalse(it.hasNext());
    scans = prev.getScanNodes();
    assertEquals(1, scans.length);
    next = prev;
    it= next.getChildIterator();
    
    // the second phase of the join
    prev = it.next();
    assertEquals(ExprType.JOIN, prev.getStoreTableNode().getSubNode().getType());
    assertEquals(scans[0].getInputSchema(), prev.getOutputSchema());
    assertTrue(prev.hasChildQuery());
    assertEquals(PARTITION_TYPE.LIST, prev.getOutputType());
    assertFalse(it.hasNext());
    scans = prev.getScanNodes();
    assertEquals(2, scans.length);
    next = prev;
    it= next.getChildIterator();
    
    // the first phase of the join
    prev = it.next();
    assertEquals(ExprType.SCAN, prev.getStoreTableNode().getSubNode().getType());
    assertFalse(prev.hasChildQuery());
    assertEquals(PARTITION_TYPE.HASH, prev.getOutputType());
    assertEquals(1, prev.getScanNodes().length);
    
    prev = it.next();
    assertEquals(ExprType.SCAN, prev.getStoreTableNode().getSubNode().getType());
    assertFalse(prev.hasChildQuery());
    assertEquals(PARTITION_TYPE.HASH, prev.getOutputType());
    assertEquals(1, prev.getScanNodes().length);
    assertFalse(it.hasNext());
  }
  
  @Test
  public void testSelectAfterJoin() throws IOException {
    String query = "select table0.name, table1.salary from table0,table1 where table0.name = table1.name and table1.salary > 10";
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, query);
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);
    
    MasterPlan globalPlan = planner.build(subQueryId, logicalPlan);
    
    ScheduleUnit unit = globalPlan.getRoot();
    StoreTableNode store = unit.getStoreTableNode();
    assertEquals(ExprType.JOIN, store.getSubNode().getType());
    assertTrue(unit.hasChildQuery());
    ScanNode [] scans = unit.getScanNodes();
    assertEquals(2, scans.length);
    ScheduleUnit prev;
    for (ScanNode scan : scans) {
      prev = unit.getChildQuery(scan);
      store = prev.getStoreTableNode();
      assertEquals(ExprType.SCAN, store.getSubNode().getType());
    }
  }
  
  @Test
  public void testCubeby() throws IOException {
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, 
        "select age, sum(salary) from table0 group by cube (age, id)");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);
    
    MasterPlan globalPlan = planner.build(subQueryId, logicalPlan);
    
    ScheduleUnit unit = globalPlan.getRoot();
    StoreTableNode store = unit.getStoreTableNode();
    assertEquals(ExprType.UNION, store.getSubNode().getType());
    UnionNode union = (UnionNode) store.getSubNode();
    assertEquals(ExprType.SCAN, union.getOuterNode().getType());
    assertEquals(ExprType.UNION, union.getInnerNode().getType());
    union = (UnionNode) union.getInnerNode();
    assertEquals(ExprType.SCAN, union.getOuterNode().getType());
    assertEquals(ExprType.UNION, union.getInnerNode().getType());
    union = (UnionNode) union.getInnerNode();
    assertEquals(ExprType.SCAN, union.getOuterNode().getType());
    assertEquals(ExprType.SCAN, union.getInnerNode().getType());
    assertTrue(unit.hasChildQuery());
    
    String tableId = "";
    for (ScanNode scan : unit.getScanNodes()) {
      ScheduleUnit prev = unit.getChildQuery(scan);
      store = prev.getStoreTableNode();
      assertEquals(ExprType.GROUP_BY, store.getSubNode().getType());
      GroupbyNode groupby = (GroupbyNode) store.getSubNode();
      assertEquals(ExprType.SCAN, groupby.getSubNode().getType());
      if (tableId.equals("")) {
        tableId = store.getTableName();
      } else {
        assertEquals(tableId, store.getTableName());
      }
      assertEquals(1, prev.getScanNodes().length);
      prev = prev.getChildQuery(prev.getScanNodes()[0]);
      store = prev.getStoreTableNode();
      assertEquals(ExprType.GROUP_BY, store.getSubNode().getType());
      groupby = (GroupbyNode) store.getSubNode();
      assertEquals(ExprType.SCAN, groupby.getSubNode().getType());
    }
  }
  
  //@Test
  public void testLocalize() throws IOException, URISyntaxException, NoSuchQueryIdException {
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx,
        "select table0.age,table0.salary,table1.salary from table0 inner join table1 on table0.salary = table1.salary");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    MasterPlan globalPlan = planner.build(subQueryId, logicalPlan);
    
    recursiveTestLocalize(globalPlan.getRoot());
  }
  
  private void recursiveTestLocalize(ScheduleUnit plan) 
      throws IOException, URISyntaxException, NoSuchQueryIdException {
    if (plan.hasChildQuery()) {
      Iterator<ScheduleUnit> it = plan.getChildIterator();
      while (it.hasNext()) {
        recursiveTestLocalize(it.next());
      }
    }
    
    QueryUnit[] units = planner.localize(plan, 3);
    assertEquals(3, units.length);
    for (QueryUnit unit : units) {
      // partition
      if (plan.getOutputType() == PARTITION_TYPE.HASH) {
        assertTrue(unit.getStoreTableNode().getNumPartitions() > 1);
        assertNotNull(unit.getStoreTableNode().getPartitionKeys());
      }
      
      // fragment
      for (ScanNode scan : unit.getScanNodes()) {
        assertNotNull(unit.getFragments(scan.getTableId()));  
      }
      InProgressStatusProto.Builder builder = InProgressStatusProto.newBuilder();
      builder.setId(unit.getId().getProto())
      .setProgress(1.0f)
      .setStatus(QueryStatus.FINISHED);
      
      qm.updateProgress(unit.getId(), builder.build());
    }
  }
}
