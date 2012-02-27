package nta.engine.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import nta.catalog.CatalogService;
import nta.catalog.Column;
import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.DatumFactory;
import nta.datum.NullDatum;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.QueryIdFactory;
import nta.engine.QueryUnitId;
import nta.engine.SubqueryContext;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.logical.CreateTableNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.physical.PhysicalExec;
import nta.storage.Appender;
import nta.storage.Scanner;
import nta.storage.StorageManager;
import nta.storage.StorageUtil;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Hyunsik Choi
 */
public class TestPhysicalPlanner {
  private static NtaTestingUtility util;
  private static Configuration conf;
  private static CatalogService catalog;
  private static QueryAnalyzer analyzer;
  private static QueryContext.Factory qfactory;
  private static SubqueryContext.Factory factory;
  private static StorageManager sm;

  private static TableDesc employee = null;
  private static TableDesc student = null;
  private static TableDesc score = null;

  @BeforeClass
  public static final void setUp() throws Exception {
    QueryIdFactory.reset();
    util = new NtaTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    conf = util.getConfiguration();
    sm = StorageManager.get(conf, util.setupClusterTestBuildDir()
        .getAbsolutePath());
    catalog = util.getMiniCatalogCluster().getCatalog();

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("deptName", DataType.STRING);

    Schema schema2 = new Schema();
    schema2.addColumn("deptName", DataType.STRING);
    schema2.addColumn("manager", DataType.STRING);

    Schema scoreSchema = new Schema();
    scoreSchema.addColumn("deptName", DataType.STRING);
    scoreSchema.addColumn("class", DataType.STRING);
    scoreSchema.addColumn("score", DataType.INT);
    scoreSchema.addColumn("nullable", DataType.STRING);

    TableMeta employeeMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    sm.initTableBase(employeeMeta, "employee");
    Appender appender = sm.getAppender(employeeMeta, "employee", "employee");
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());
    for (int i = 0; i < 100; i++) {
      tuple.put(DatumFactory.createString("name_" + i),
          DatumFactory.createInt(i), DatumFactory.createString("dept_" + i));
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    employee = new TableDescImpl("employee", employeeMeta, 
        sm.getTablePath("employee"));
    catalog.addTable(employee);

    student = new TableDescImpl("dept", schema2, StoreType.CSV, new Options(),
        new Path("file:///"));
    catalog.addTable(student);

    score = new TableDescImpl("score", scoreSchema, StoreType.CSV, 
        new Options(), sm.getTablePath("score"));
    sm.initTableBase(score.getMeta(), "score");
    appender = sm.getAppender(score.getMeta(), "score", "score");
    tuple = new VTuple(score.getMeta().getSchema().getColumnNum());
    int m = 0;
    for (int i = 1; i <= 5; i++) {
      for (int k = 3; k < 5; k++) {
        for (int j = 1; j <= 3; j++) {
          tuple.put(DatumFactory.createString("name_" + i), // name_1 ~ 5 (cad:
                                                            // 5)
              DatumFactory.createString(k + "rd"), // 3 or 4rd (cad: 2)
              DatumFactory.createInt(j), // 1 ~ 3
              m % 3 == 1 ? DatumFactory.createString("one") : NullDatum.get()); 
          appender.addTuple(tuple);
          m++;
        }
      }
    }
    appender.flush();
    appender.close();
    catalog.addTable(score);
    qfactory = new QueryContext.Factory(catalog);
    factory = new SubqueryContext.Factory(catalog);
    analyzer = new QueryAnalyzer(catalog);
  }

  @AfterClass
  public static final void tearDown() throws Exception {
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }

  private String[] QUERIES = {
      "select name, empId, deptName from employee where empId", // 0
      "select name, empId, e.deptName, manager from employee as e, dept as dp", // 1
      "select name, empId, e.deptName, manager, score from employee as e, dept, score", // 2
      "select p.deptName, sum(score) from dept as p, score group by p.deptName having sum(score) > 30", // 3
      "select p.deptName, score from dept as p, score order by score asc", // 4
      "select name from employee where empId = 100", // 5
      "select deptName, class, score from score", // 6
      "select deptName, class, sum(score), max(score), min(score) from score group by deptName, class", // 7
      "grouped := select deptName, class, sum(score), max(score), min(score) from score group by deptName, class", // 8
      "select count(*), max(score), min(score) from score", // 9
      "select count(deptName) from score", // 10
      "select managerId, empId, deptName from employee order by managerId, empId desc", // 11
      "select deptName, nullable from score group by deptName, nullable", // 12
      "select 3 < 4 as ineq, 3.5 * 2 as real", // 13
      "select 3 > 2 = 1 > 0 and 3 > 1", // 14
  };

  public final void testCreateScanPlan() throws IOException {
    Fragment[] frags = sm.split("employee");
    factory = new SubqueryContext.Factory(catalog);
    QueryUnitId id = QueryIdFactory.newQueryUnitId();
    SubqueryContext ctx = factory.create(id, new Fragment[] { frags[0] });
    ParseTree query = (ParseTree) analyzer.parse(ctx, QUERIES[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);

    LogicalOptimizer.optimize(ctx, plan);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    Tuple tuple = null;
    int i = 0;
    long start = System.currentTimeMillis();
    while ((tuple = exec.next()) != null) {
      assertTrue(tuple.contains(0));
      assertTrue(tuple.contains(1));
      assertTrue(tuple.contains(2));
      i++;
    }
    assertEquals(100, i);
    long end = System.currentTimeMillis();
    System.out.println((end - start) + " msc");
  }

  @Test
  public final void testGroupByPlan() throws IOException {
    Fragment[] frags = sm.split("score");
    SubqueryContext ctx = factory.create(QueryIdFactory.newQueryUnitId(),
        new Fragment[] { frags[0] });
    ParseTree query = (ParseTree) analyzer.parse(ctx, QUERIES[7]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    LogicalOptimizer.optimize(ctx, plan);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    int i = 0;
    Tuple tuple = null;
    while ((tuple = exec.next()) != null) {
      assertEquals(6, tuple.getInt(2).asInt()); // sum
      assertEquals(3, tuple.getInt(3).asInt()); // max
      assertEquals(1, tuple.getInt(4).asInt()); // min
      i++;
    }
    assertEquals(10, i);
  }

  @Test
  public final void testStorePlan() throws IOException {
    Fragment[] frags = sm.split("score");
    factory = new SubqueryContext.Factory(catalog);
    QueryUnitId id = QueryIdFactory.newQueryUnitId();
    SubqueryContext ctx = factory.create(id, new Fragment[] { frags[0] });
    ParseTree query = (ParseTree) analyzer.parse(ctx, QUERIES[8]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);

    LogicalOptimizer.optimize(ctx, plan);

    TableMeta outputMeta = TCatUtil.newTableMeta(plan.getOutputSchema(),
        StoreType.CSV);
    sm.initTableBase(outputMeta, "grouped");

    PhysicalPlanner phyPlanner = new PhysicalPlanner(sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);
    exec.next();

    Scanner scanner = sm.getScanner("grouped", id.toString());
    Tuple tuple = null;
    int i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(6, tuple.getInt(2).asInt()); // sum
      assertEquals(3, tuple.getInt(3).asInt()); // max
      assertEquals(1, tuple.getInt(4).asInt()); // min
      i++;
    }
    assertEquals(10, i);
    scanner.close();
  }

  @Test
  public final void testPartitionedStorePlan() throws IOException {
    Fragment[] frags = sm.split("score");
    factory = new SubqueryContext.Factory(catalog);
    QueryUnitId id = QueryIdFactory.newQueryUnitId();
    SubqueryContext ctx = factory.create(id, new Fragment[] { frags[0] });
    ParseTree query = (ParseTree) analyzer.parse(ctx, QUERIES[7]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);

    int numPartitions = 3;
    Column key1 = new Column("score.deptName", DataType.STRING);
    Column key2 = new Column("score.class", DataType.STRING);
    CreateTableNode storeNode = new CreateTableNode("partition");
    storeNode.setPartitions(new Column[] { key1, key2 }, numPartitions);
    PlannerUtil.insertNode(plan, storeNode);
    LogicalOptimizer.optimize(ctx, plan);

    TableMeta outputMeta = TCatUtil.newTableMeta(plan.getOutputSchema(),
        StoreType.CSV);
    sm.initTableBase(outputMeta, "partition");

    PhysicalPlanner phyPlanner = new PhysicalPlanner(sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);
    exec.next();

    Path path = StorageUtil.concatPath( 
        sm.getTablePath("partition"),
        id.toString());
    FileSystem fs = sm.getFileSystem();
        
    assertEquals(numPartitions,
        fs.listStatus(StorageUtil.concatPath(path, "data")).length);

    Scanner scanner = sm.getTableScanner(path);
    Tuple tuple = null;
    int i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(6, tuple.getInt(2).asInt()); // sum
      assertEquals(3, tuple.getInt(3).asInt()); // max
      assertEquals(1, tuple.getInt(4).asInt()); // min
      i++;
    }
    assertEquals(10, i);
    scanner.close();
  }

  @Test
  public final void testAggregationFunction() throws IOException {
    Fragment[] frags = sm.split("score");
    factory = new SubqueryContext.Factory(catalog);
    SubqueryContext ctx = factory.create(QueryIdFactory.newQueryUnitId(),
        new Fragment[] { frags[0] });
    ParseTree query = (ParseTree) analyzer.parse(ctx, QUERIES[9]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    LogicalOptimizer.optimize(ctx, plan);

    System.out.println(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    Tuple tuple = exec.next();
    assertEquals(30, tuple.get(0).asLong());
    assertEquals(3, tuple.get(1).asInt());
    assertEquals(1, tuple.get(2).asInt());
    assertNull(exec.next());
  }

  @Test
  public final void testCountFunction() throws IOException {
    Fragment[] frags = sm.split("score");
    factory = new SubqueryContext.Factory(catalog);
    SubqueryContext ctx = factory.create(QueryIdFactory.newQueryUnitId(),
        new Fragment[] { frags[0] });
    ParseTree query = (ParseTree) analyzer.parse(ctx, QUERIES[10]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    LogicalOptimizer.optimize(ctx, plan);

    System.out.println(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    Tuple tuple = exec.next();
    assertEquals(30, tuple.get(0).asLong());
    assertNull(exec.next());
  }
  
  @Test
  public final void testGroupByWithNullValue() throws IOException {
    Fragment[] frags = sm.split("score");
    factory = new SubqueryContext.Factory(catalog);
    SubqueryContext ctx = factory.create(QueryIdFactory.newQueryUnitId(),
        new Fragment[] { frags[0] });
    ParseTree query = (ParseTree) analyzer.parse(ctx, QUERIES[12]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    LogicalOptimizer.optimize(ctx, plan);

    System.out.println(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlanner(sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    @SuppressWarnings("unused")
    Tuple tuple = null;    
    int count = 0;
    while((tuple = exec.next()) != null) {
      count++;
    }
    assertEquals(10, count);
  }
  
  @Test
  public final void testEvalExpr() throws IOException {
    factory = new SubqueryContext.Factory(catalog);
    SubqueryContext ctx = factory.create(QueryIdFactory.newQueryUnitId(),
        new Fragment[] { });
    ParseTree query = (ParseTree) analyzer.parse(ctx, QUERIES[13]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    LogicalOptimizer.optimize(ctx, plan);
    
    PhysicalPlanner phyPlanner = new PhysicalPlanner(sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);
    Tuple tuple = null;
    tuple = exec.next();
    assertEquals(true, tuple.get(0).asBool());
    assertTrue(7.0d == tuple.get(1).asDouble());    

    query = (ParseTree) analyzer.parse(ctx, QUERIES[14]);
    plan = LogicalPlanner.createPlan(ctx, query);
    LogicalOptimizer.optimize(ctx, plan);
    
    phyPlanner = new PhysicalPlanner(sm);
    exec = phyPlanner.createPlan(ctx, plan);
    tuple = null;
    tuple = exec.next();
    assertEquals(DatumFactory.createBool(true), tuple.get(0));
  }
}
