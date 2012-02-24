package nta.engine.planner;

import static org.junit.Assert.assertEquals;

import java.util.Stack;

import nta.catalog.CatalogService;
import nta.catalog.FunctionDesc;
import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.function.SumInt;
import nta.engine.json.GsonCreator;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalNodeVisitor;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.StoreTableNode;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;

/**
 * @author Hyunsik Choi
 */
public class TestLogicalPlanner {
  private static NtaTestingUtility util;
  private static CatalogService catalog;
  private static QueryContext.Factory factory;
  private static QueryAnalyzer analyzer;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    
    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("deptName", DataType.STRING);

    Schema schema2 = new Schema();
    schema2.addColumn("deptName", DataType.STRING);
    schema2.addColumn("manager", DataType.STRING);

    Schema schema3 = new Schema();
    schema3.addColumn("deptName", DataType.STRING);
    schema3.addColumn("score", DataType.INT);

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    TableDesc people = new TableDescImpl("employee", meta, 
        new Path("file:///"));
    catalog.addTable(people);

    TableDesc student = new TableDescImpl("dept", schema2, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(student);

    TableDesc score = new TableDescImpl("score", schema3, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(score);

    FunctionDesc funcDesc = new FunctionDesc("sumtest", SumInt.class,
        FunctionType.GENERAL, DataType.INT, new DataType[] { DataType.INT });

    catalog.registerFunction(funcDesc);
    analyzer = new QueryAnalyzer(catalog);
    factory = new QueryContext.Factory(catalog);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }

  static String[] QUERIES = {
      "select name, empId, deptName from employee where empId > 500", // 0
      "select name, empId, e.deptName, manager from employee as e, dept as dp", // 1
      "select name, empId, e.deptName, manager, score from employee as e, dept, score", // 2
      "select p.deptName, sumtest(score) from dept as p, score group by p.deptName having sumtest(score) > 30", // 3
      "select p.deptName, score from dept as p, score order by score asc", // 4
      "select name from employee where empId = 100", // 5
      "select name, score from employee, score", // 6
      "select p.deptName, sumtest(score) from dept as p, score group by p.deptName", // 7
      "store1 := select p.deptName, sumtest(score) from dept as p, score group by p.deptName", // 8
      "select deptName, sumtest(score) from score group by deptName having sumtest(score) > 30", // 9
      "select 7 + 8", // 10
  };

  @Test
  public final void testSingleRelation() throws CloneNotSupportedException {
    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    assertEquals(ExprType.ROOT, plan.getType());
    TestLogicalNode.testCloneLogicalNode(plan);
    LogicalRootNode root = (LogicalRootNode) plan;

    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();

    assertEquals(ExprType.SELECTION, projNode.getSubNode().getType());
    SelectionNode selNode = (SelectionNode) projNode.getSubNode();

    assertEquals(ExprType.SCAN, selNode.getSubNode().getType());
    ScanNode scanNode = (ScanNode) selNode.getSubNode();
    assertEquals("employee", scanNode.getTableId());
  }

  @Test
  public final void testMultiRelations() throws CloneNotSupportedException {
    // two relations
    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[1]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);

    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();

    assertEquals(ExprType.JOIN, projNode.getSubNode().getType());
    JoinNode joinNode = (JoinNode) projNode.getSubNode();

    assertEquals(ExprType.SCAN, joinNode.getLeftSubNode().getType());
    ScanNode leftNode = (ScanNode) joinNode.getLeftSubNode();
    assertEquals("employee", leftNode.getTableId());
    assertEquals(ExprType.SCAN, joinNode.getRightSubNode().getType());
    ScanNode rightNode = (ScanNode) joinNode.getRightSubNode();
    assertEquals("dept", rightNode.getTableId());

    // three relations
    ctx = factory.create();
    block = analyzer.parse(ctx, QUERIES[2]);
    plan = LogicalPlanner.createPlan(ctx, block);
    TestLogicalNode.testCloneLogicalNode(plan);

    assertEquals(ExprType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;

    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    projNode = (ProjectionNode) root.getSubNode();

    assertEquals(ExprType.JOIN, projNode.getSubNode().getType());
    joinNode = (JoinNode) projNode.getSubNode();

    assertEquals(ExprType.JOIN, joinNode.getLeftSubNode().getType());
    JoinNode leftNode2 = (JoinNode) joinNode.getLeftSubNode();

    assertEquals(ExprType.SCAN, leftNode2.getLeftSubNode().getType());
    ScanNode scan1 = (ScanNode) leftNode2.getLeftSubNode();
    assertEquals("employee", scan1.getTableId());
    assertEquals(ExprType.SCAN, leftNode2.getRightSubNode().getType());
    ScanNode scan2 = (ScanNode) leftNode2.getRightSubNode();
    assertEquals("dept", scan2.getTableId());

    assertEquals(ExprType.SCAN, joinNode.getRightSubNode().getType());
    rightNode = (ScanNode) joinNode.getRightSubNode();
    assertEquals("score", rightNode.getTableId());
  }

  @Test
  public final void testGroupby() throws CloneNotSupportedException {
    // without 'having clause'
    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[7]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);

    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    testQuery7(root.getSubNode());
    
    // with having clause
    ctx = factory.create();
    block = analyzer.parse(ctx, QUERIES[3]);
    plan = LogicalPlanner.createPlan(ctx, block);
    TestLogicalNode.testCloneLogicalNode(plan);

    assertEquals(ExprType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;

    assertEquals(ExprType.GROUP_BY, root.getSubNode().getType());
    GroupbyNode groupByNode = (GroupbyNode) root.getSubNode();

    assertEquals(ExprType.JOIN, groupByNode.getSubNode().getType());
    JoinNode joinNode = (JoinNode) groupByNode.getSubNode();

    assertEquals(ExprType.SCAN, joinNode.getLeftSubNode().getType());
    ScanNode leftNode = (ScanNode) joinNode.getLeftSubNode();
    assertEquals("dept", leftNode.getTableId());
    assertEquals(ExprType.SCAN, joinNode.getRightSubNode().getType());
    ScanNode rightNode = (ScanNode) joinNode.getRightSubNode();
    assertEquals("score", rightNode.getTableId());
    
    System.out.println(plan);
    System.out.println("-------------------");
    LogicalOptimizer.optimize(ctx, plan);
    System.out.println(plan);
  }
  
  static void testQuery7(LogicalNode plan) {
    assertEquals(ExprType.GROUP_BY, plan.getType());
    GroupbyNode groupByNode = (GroupbyNode) plan;

    assertEquals(ExprType.JOIN, groupByNode.getSubNode().getType());
    JoinNode joinNode = (JoinNode) groupByNode.getSubNode();

    assertEquals(ExprType.SCAN, joinNode.getLeftSubNode().getType());
    ScanNode leftNode = (ScanNode) joinNode.getLeftSubNode();
    assertEquals("dept", leftNode.getTableId());
    assertEquals(ExprType.SCAN, joinNode.getRightSubNode().getType());
    ScanNode rightNode = (ScanNode) joinNode.getRightSubNode();
    assertEquals("score", rightNode.getTableId());
  }
  
  @Test
  public final void testStoreTable() throws CloneNotSupportedException {
    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[8]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    TestLogicalNode.testCloneLogicalNode(plan);
    
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    
    assertEquals(ExprType.STORE, root.getSubNode().getType());
    StoreTableNode storeNode = (StoreTableNode) root.getSubNode();
    testQuery7(storeNode.getSubNode());
    LogicalOptimizer.optimize(ctx, plan);
  }

  @Test
  public final void testOrderBy() throws CloneNotSupportedException {
    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[4]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    TestLogicalNode.testCloneLogicalNode(plan);

    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();

    assertEquals(ExprType.SORT, projNode.getSubNode().getType());
    SortNode sortNode = (SortNode) projNode.getSubNode();

    assertEquals(ExprType.JOIN, sortNode.getSubNode().getType());
    JoinNode joinNode = (JoinNode) sortNode.getSubNode();

    assertEquals(ExprType.SCAN, joinNode.getLeftSubNode().getType());
    ScanNode leftNode = (ScanNode) joinNode.getLeftSubNode();
    assertEquals("dept", leftNode.getTableId());
    assertEquals(ExprType.SCAN, joinNode.getRightSubNode().getType());
    ScanNode rightNode = (ScanNode) joinNode.getRightSubNode();
    assertEquals("score", rightNode.getTableId());
  }

  @Test
  public final void testSPJPush() throws CloneNotSupportedException {
    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[5]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    TestLogicalNode.testCloneLogicalNode(plan);
    
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();
    assertEquals(ExprType.SELECTION, projNode.getSubNode().getType());
    SelectionNode selNode = (SelectionNode) projNode.getSubNode();    
    assertEquals(ExprType.SCAN, selNode.getSubNode().getType());
    ScanNode scanNode = (ScanNode) selNode.getSubNode();
    
    LogicalOptimizer.optimize(ctx, plan);
    assertEquals(ExprType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;
    assertEquals(ExprType.SCAN, root.getSubNode().getType());
    scanNode = (ScanNode) root.getSubNode();
    assertEquals("employee", scanNode.getTableId());
  }
  
  @Test
  public final void testSPJ() throws CloneNotSupportedException {
    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[6]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    TestLogicalNode.testCloneLogicalNode(plan);
  }
  
  @Test
  public final void testJson() {
    QueryContext ctx = factory.create();
	  QueryBlock block = analyzer.parse(ctx, QUERIES[9]);
	  LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
	  LogicalOptimizer.optimize(ctx, plan);
	    
	  String json = plan.toJSON();
	  System.out.println(json);
	  Gson gson = GsonCreator.getInstance();
	  LogicalNode fromJson = gson.fromJson(json, LogicalNode.class);
	  System.out.println(fromJson.toJSON());
	  assertEquals(ExprType.ROOT, fromJson.getType());
	  LogicalNode groupby = ((LogicalRootNode)fromJson).getSubNode();
	  assertEquals(ExprType.GROUP_BY, groupby.getType());
	  LogicalNode scan = ((GroupbyNode)groupby).getSubNode();
	  assertEquals(ExprType.SCAN, scan.getType());
  }
  
  @Test
  public final void testVisitor() {
    // two relations
    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[1]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    
    TestVisitor vis = new TestVisitor();
    plan.accept(vis);
    
    assertEquals(ExprType.ROOT, vis.stack.pop().getType());
    assertEquals(ExprType.PROJECTION, vis.stack.pop().getType());
    assertEquals(ExprType.JOIN, vis.stack.pop().getType());
    assertEquals(ExprType.SCAN, vis.stack.pop().getType());
    assertEquals(ExprType.SCAN, vis.stack.pop().getType());
  }
  
  private static class TestVisitor implements LogicalNodeVisitor {
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    @Override
    public void visit(LogicalNode node) {
      stack.push(node);
    }
  }
  
  @Test
  public final void testExprNode() {
    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[10]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    LogicalOptimizer.optimize(ctx, plan);    
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(ExprType.EXPRS, root.getSubNode().getType());
  }
}
