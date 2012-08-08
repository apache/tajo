package nta.engine.plan.global;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import nta.catalog.*;
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
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.MasterPlan;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.global.ScheduleUnit.PARTITION_TYPE;
import nta.engine.planner.logical.*;
import nta.engine.query.GlobalPlanner;
import nta.engine.query.GlobalPlannerUtils;
import nta.storage.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

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
  private static StorageManager sm;

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
    sm = new StorageManager(util.getConfiguration());
    FunctionDesc funcDesc = new FunctionDesc("sumtest", TestSum.class, FunctionType.GENERAL,
        new DataType[] {DataType.INT},
        new DataType[] {DataType.INT});
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
    assertEquals(ExprType.PROJECTION,
        next.getStoreTableNode().getSubNode().getType());
    assertTrue(next.hasChildQuery());
    assertEquals(PARTITION_TYPE.LIST, next.getOutputType());
    Iterator<ScheduleUnit> it= next.getChildIterator();

    prev = it.next();
    assertEquals(ExprType.SORT,
        prev.getStoreTableNode().getSubNode().getType());
    assertTrue(prev.hasChildQuery());
    assertEquals(PARTITION_TYPE.LIST, prev.getOutputType());
    it= prev.getChildIterator();
    next = prev;
    
    prev = it.next();
    assertFalse(prev.hasChildQuery());
    assertEquals(PARTITION_TYPE.RANGE, prev.getOutputType());
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
    assertEquals(ExprType.PROJECTION, next.getStoreTableNode().getSubNode().getType());
    ScanNode []scans = next.getScanNodes();
    assertEquals(1, scans.length);
    Iterator<ScheduleUnit> it= next.getChildIterator();

    prev = it.next();
    assertEquals(ExprType.SORT, prev.getStoreTableNode().getSubNode().getType());
    assertEquals(PARTITION_TYPE.LIST, prev.getOutputType());
    scans = prev.getScanNodes();
    assertEquals(1, scans.length);
    it= prev.getChildIterator();
    
    // the first phase of the sort
    prev = it.next();
    assertEquals(ExprType.SORT, prev.getStoreTableNode().getSubNode().getType());
    assertEquals(scans[0].getInputSchema(), prev.getOutputSchema());
    assertTrue(prev.hasChildQuery());
    assertEquals(PARTITION_TYPE.RANGE, prev.getOutputType());
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
    assertEquals(ExprType.PROJECTION, store.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) store.getSubNode();
    ScanNode[] scans = unit.getScanNodes();
    assertEquals(1, scans.length);

    unit = unit.getChildQuery(scans[0]);
    store = unit.getStoreTableNode();
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
        assertNotNull(unit.getFragment(scan.getTableId()));
      }
      InProgressStatusProto.Builder builder = InProgressStatusProto.newBuilder();
      builder.setId(unit.getId().getProto())
      .setProgress(1.0f)
      .setStatus(QueryStatus.QUERY_FINISHED);
      
      qm.updateProgress(unit.getId(), builder.build());
    }
  }

  @Test
  public void testHashFetches() {
    URI[] uris = {
        URI.create("http://192.168.0.21:35385/?qid=query_20120726371_000_001_003_000835&fn=0"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_003_001064&fn=0"),
        URI.create("http://192.168.0.21:35385/?qid=query_20120726371_000_001_003_001059&fn=0"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_003_000104&fn=0"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_003_000104&fn=1"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_003_001059&fn=1")
    };

    Map<String, List<URI>> hashed = GlobalPlanner.hashFetches(null, Lists.newArrayList(uris));
    assertEquals(2, hashed.size());
    List<URI> res = hashed.get("0");
    assertEquals(2, res.size());
    res = hashed.get("1");
    assertEquals(1, res.size());
    QueryStringDecoder decoder = new QueryStringDecoder(res.get(0));
    Map<String, List<String>> params = decoder.getParameters();
    String [] qids = params.get("qid").get(0).split(",");
    assertEquals(2, qids.length);
    assertEquals("104", qids[0]);
    assertEquals("1059", qids[1]);
  }

  @Test
  public void testHashFetchesForBinary() {
    URI[] urisOuter = {
        URI.create("http://192.168.0.21:35385/?qid=query_20120726371_000_001_003_000835&fn=0"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_003_001064&fn=0"),
        URI.create("http://192.168.0.21:35385/?qid=query_20120726371_000_001_003_001059&fn=0"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_003_000104&fn=0"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_003_000104&fn=1"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_003_001059&fn=1")
    };

    URI[] urisInner = {
        URI.create("http://192.168.0.21:35385/?qid=query_20120726371_000_001_004_000111&fn=0"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_004_000123&fn=0"),
        URI.create("http://192.168.0.17:35385/?qid=query_20120726371_000_001_004_00134&fn=0"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_004_000155&fn=0"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_004_000255&fn=1"),
        URI.create("http://192.168.0.8:55205/?qid=query_20120726371_000_001_004_001356&fn=1")
    };

    Schema schema1 = new Schema();
    schema1.addColumn("col1", DataType.INT);
    TableMeta meta1 = new TableMetaImpl(schema1, StoreType.CSV, Options.create());
    TableDesc desc1 = new TableDescImpl("table1", meta1, new Path("/"));
    TableDesc desc2 = new TableDescImpl("table2", meta1, new Path("/"));

    QueryBlock.FromTable table1 = new QueryBlock.FromTable(desc1);
    QueryBlock.FromTable table2 = new QueryBlock.FromTable(desc2);
    ScanNode scan1 = new ScanNode(table1);
    ScanNode scan2 = new ScanNode(table2);

    Map<ScanNode, List<URI>> uris = Maps.newHashMap();
    uris.put(scan1, Lists.newArrayList(urisOuter));
    uris.put(scan2, Lists.newArrayList(urisInner));

    Map<String, Map<ScanNode, List<URI>>> hashed = GlobalPlanner.hashFetches(uris);
    assertEquals(2, hashed.size());
    assertTrue(hashed.keySet().contains("0"));
    assertTrue(hashed.keySet().contains("1"));

    assertTrue(hashed.get("0").containsKey(scan1));
    assertTrue(hashed.get("0").containsKey(scan2));

    assertEquals(2, hashed.get("0").get(scan1).size());
    assertEquals(3, hashed.get("0").get(scan2).size());

    QueryStringDecoder decoder = new QueryStringDecoder(hashed.get("0").get(scan1).get(0));
    Map<String, List<String>> params = decoder.getParameters();
    String [] qids = params.get("qid").get(0).split(",");
    assertEquals(2, qids.length);
    assertEquals("1064", qids[0]);
    assertEquals("104", qids[1]);

    decoder = new QueryStringDecoder(hashed.get("0").get(scan1).get(1));
    params = decoder.getParameters();
    qids = params.get("qid").get(0).split(",");
    assertEquals(2, qids.length);
    assertEquals("835", qids[0]);
    assertEquals("1059", qids[1]);
  }

  @Test
  public void testCreateMultilevelGroupby()
      throws IOException, CloneNotSupportedException {
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx,
        "store1 := select age, sumtest(salary) from table0 group by age");
    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, tree);
    logicalPlan = LogicalOptimizer.optimize(ctx, logicalPlan);

    MasterPlan globalPlan = planner.build(subQueryId, logicalPlan);

    ScheduleUnit second, first, mid;
    ScanNode secondScan, firstScan, midScan;

    second = globalPlan.getRoot();
    assertTrue(second.getScanNodes().length == 1);

    first = second.getChildQuery(second.getScanNodes()[0]);

    GroupbyNode firstGroupby, secondGroupby, midGroupby;
    secondGroupby = (GroupbyNode) second.getStoreTableNode().getSubNode();

    Column[] originKeys = secondGroupby.getGroupingColumns();
    Column[] newKeys = new Column[2];
    newKeys[0] = new Column("age", DataType.INT);
    newKeys[1] = new Column("name", DataType.STRING);

    mid = planner.createMultilevelGroupby(first, newKeys);
    midGroupby = (GroupbyNode) mid.getStoreTableNode().getSubNode();
    firstGroupby = (GroupbyNode) first.getStoreTableNode().getSubNode();

    secondScan = second.getScanNodes()[0];
    midScan = mid.getScanNodes()[0];
    firstScan = first.getScanNodes()[0];

    assertTrue(first.getParentQuery().equals(mid));
    assertTrue(mid.getParentQuery().equals(second));
    assertTrue(second.getChildQuery(secondScan).equals(mid));
    assertTrue(mid.getChildQuery(midScan).equals(first));
    assertEquals(first.getOutputName(), midScan.getTableId());
    assertEquals(first.getOutputSchema(), midScan.getInputSchema());
    assertEquals(mid.getOutputName(), secondScan.getTableId());
    assertEquals(mid.getOutputSchema(), secondScan.getOutputSchema());
    assertArrayEquals(newKeys, firstGroupby.getGroupingColumns());
    assertArrayEquals(newKeys, midGroupby.getGroupingColumns());
    assertArrayEquals(originKeys, secondGroupby.getGroupingColumns());
    assertFalse(firstScan.isLocal());
    assertTrue(midScan.isLocal());
    assertTrue(secondScan.isLocal());
  }
}
