package nta.engine.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import nta.catalog.CatalogService;
import nta.catalog.Column;
import nta.catalog.FunctionDesc;
import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.IndexMethod;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.function.SumInt;
import nta.engine.json.GsonCreator;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.logical.IndexWriteNode;
import nta.engine.planner.logical.CreateTableNode;
import nta.engine.planner.logical.ExceptNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.IntersectNode;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalNodeVisitor;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.UnionNode;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import tajo.benchmark.TPCH;

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
      "select 7 + 8, 8 * 9, 10 * 10 as mul", // 10
      "create index idx_employee on employee using bitmap (name null first, empId desc) with ('fillfactor' = 70)" // 11
  };

  @Test
  public final void testSingleRelation() throws CloneNotSupportedException {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, QUERIES[0]);
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
  public final void testImplicityJoinPlan() throws CloneNotSupportedException {
    // two relations
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, QUERIES[1]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);

    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();

    assertEquals(ExprType.JOIN, projNode.getSubNode().getType());
    JoinNode joinNode = (JoinNode) projNode.getSubNode();

    assertEquals(ExprType.SCAN, joinNode.getOuterNode().getType());
    ScanNode leftNode = (ScanNode) joinNode.getOuterNode();
    assertEquals("employee", leftNode.getTableId());
    assertEquals(ExprType.SCAN, joinNode.getInnerNode().getType());
    ScanNode rightNode = (ScanNode) joinNode.getInnerNode();
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

    assertEquals(ExprType.SCAN, joinNode.getOuterNode().getType());
    ScanNode scan1 = (ScanNode) joinNode.getOuterNode();
    assertEquals("employee", scan1.getTableId());
    
    JoinNode rightNode2 = (JoinNode) joinNode.getInnerNode();
    assertEquals(ExprType.JOIN, rightNode2.getType());
        
    assertEquals(ExprType.SCAN, rightNode2.getOuterNode().getType());
    ScanNode leftScan = (ScanNode) rightNode2.getOuterNode();
    assertEquals("dept", leftScan.getTableId());

    assertEquals(ExprType.SCAN, rightNode2.getInnerNode().getType());
    ScanNode rightScan = (ScanNode) rightNode2.getInnerNode();
    assertEquals("score", rightScan.getTableId());
  }
  
  String [] JOINS = { 
      "select name, dept.deptName, score from employee natural join dept natural join score", // 0
      "select name, dept.deptName, score from employee inner join dept inner join score on dept.deptName = score.deptName", // 1
      "select name, dept.deptName, score from employee left outer join dept right outer join score on dept.deptName = score.deptName" // 2
  };
  
  @Test
  public final void testNaturalJoinPlan() {
    // two relations
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, JOINS[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;    
    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode proj = (ProjectionNode) root.getSubNode();
    assertEquals(ExprType.JOIN, proj.getSubNode().getType());
    JoinNode join = (JoinNode) proj.getSubNode();
    assertEquals(JoinType.NATURAL, join.getJoinType());
    assertEquals(ExprType.SCAN, join.getOuterNode().getType());
    assertTrue(join.hasJoinQual());
    ScanNode scan = (ScanNode) join.getOuterNode();
    assertEquals("employee", scan.getTableId());
    
    assertEquals(ExprType.JOIN, join.getInnerNode().getType());
    join = (JoinNode) join.getInnerNode();
    assertEquals(JoinType.NATURAL, join.getJoinType());
    assertEquals(ExprType.SCAN, join.getOuterNode().getType());
    ScanNode outer = (ScanNode) join.getOuterNode();
    assertEquals("dept", outer.getTableId());
    assertEquals(ExprType.SCAN, join.getInnerNode().getType());
    ScanNode inner = (ScanNode) join.getInnerNode();
    assertEquals("score", inner.getTableId());
  }
  
  @Test
  public final void testInnerJoinPlan() {
    // two relations
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, JOINS[1]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;    
    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode proj = (ProjectionNode) root.getSubNode();
    assertEquals(ExprType.JOIN, proj.getSubNode().getType());
    JoinNode join = (JoinNode) proj.getSubNode();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals(ExprType.SCAN, join.getOuterNode().getType());
    ScanNode scan = (ScanNode) join.getOuterNode();
    assertEquals("employee", scan.getTableId());    
    
    assertEquals(ExprType.JOIN, join.getInnerNode().getType());
    join = (JoinNode) join.getInnerNode();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals(ExprType.SCAN, join.getOuterNode().getType());
    ScanNode outer = (ScanNode) join.getOuterNode();
    assertEquals("dept", outer.getTableId());
    assertEquals(ExprType.SCAN, join.getInnerNode().getType());
    ScanNode inner = (ScanNode) join.getInnerNode();
    assertEquals("score", inner.getTableId());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalNode.Type.EQUAL, join.getJoinQual().getType());
  }
  
  @Test
  public final void testOuterJoinPlan() {
    // two relations
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, JOINS[2]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;    
    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode proj = (ProjectionNode) root.getSubNode();
    assertEquals(ExprType.JOIN, proj.getSubNode().getType());
    JoinNode join = (JoinNode) proj.getSubNode();
    assertEquals(JoinType.LEFT_OUTER, join.getJoinType());
    assertEquals(ExprType.SCAN, join.getOuterNode().getType());
    ScanNode scan = (ScanNode) join.getOuterNode();
    assertEquals("employee", scan.getTableId());
    
    assertEquals(ExprType.JOIN, join.getInnerNode().getType());
    join = (JoinNode) join.getInnerNode();
    assertEquals(JoinType.RIGHT_OUTER, join.getJoinType());
    assertEquals(ExprType.SCAN, join.getOuterNode().getType());
    ScanNode outer = (ScanNode) join.getOuterNode();
    assertEquals("dept", outer.getTableId());
    assertEquals(ExprType.SCAN, join.getInnerNode().getType());
    ScanNode inner = (ScanNode) join.getInnerNode();
    assertEquals("score", inner.getTableId());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalNode.Type.EQUAL, join.getJoinQual().getType());
  }

  @Test
  public final void testGroupby() throws CloneNotSupportedException {
    // without 'having clause'
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, QUERIES[7]);
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

    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();
    assertEquals(ExprType.GROUP_BY, projNode.getSubNode().getType());
    GroupbyNode groupByNode = (GroupbyNode) projNode.getSubNode();

    assertEquals(ExprType.JOIN, groupByNode.getSubNode().getType());
    JoinNode joinNode = (JoinNode) groupByNode.getSubNode();

    assertEquals(ExprType.SCAN, joinNode.getOuterNode().getType());
    ScanNode leftNode = (ScanNode) joinNode.getOuterNode();
    assertEquals("dept", leftNode.getTableId());
    assertEquals(ExprType.SCAN, joinNode.getInnerNode().getType());
    ScanNode rightNode = (ScanNode) joinNode.getInnerNode();
    assertEquals("score", rightNode.getTableId());
    
    LogicalOptimizer.optimize(ctx, plan);    
  }

  @Test
  public final void testMultipleJoin() {
    TPCH tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadQueries();

    for (String name : tpch.getTableNames()) {
      TableMeta meta = TCatUtil.newTableMeta(tpch.getSchema(name), StoreType.CSV);
      TableDesc desc = TCatUtil.newTableDesc(name, meta, new Path("/"));
      catalog.addTable(desc);
    }

    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, tpch.getQuery("q2"));
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    System.out.println(plan);
  }
  
  static void testQuery7(LogicalNode plan) {
    assertEquals(ExprType.PROJECTION, plan.getType());
    ProjectionNode projNode = (ProjectionNode) plan;
    assertEquals(ExprType.GROUP_BY, projNode.getSubNode().getType());
    GroupbyNode groupByNode = (GroupbyNode) projNode.getSubNode();

    assertEquals(ExprType.JOIN, groupByNode.getSubNode().getType());
    JoinNode joinNode = (JoinNode) groupByNode.getSubNode();

    assertEquals(ExprType.SCAN, joinNode.getOuterNode().getType());
    ScanNode leftNode = (ScanNode) joinNode.getOuterNode();
    assertEquals("dept", leftNode.getTableId());
    assertEquals(ExprType.SCAN, joinNode.getInnerNode().getType());
    ScanNode rightNode = (ScanNode) joinNode.getInnerNode();
    assertEquals("score", rightNode.getTableId());
  }
  
  @Test
  public final void testStoreTable() throws CloneNotSupportedException {
    QueryContext ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, QUERIES[8]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, tree);
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
    ParseTree block = analyzer.parse(ctx, QUERIES[4]);
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

    assertEquals(ExprType.SCAN, joinNode.getOuterNode().getType());
    ScanNode leftNode = (ScanNode) joinNode.getOuterNode();
    assertEquals("dept", leftNode.getTableId());
    assertEquals(ExprType.SCAN, joinNode.getInnerNode().getType());
    ScanNode rightNode = (ScanNode) joinNode.getInnerNode();
    assertEquals("score", rightNode.getTableId());
    LogicalNode opt = LogicalOptimizer.optimize(ctx, plan);
    System.out.println(opt);
  }

  @Test
  public final void testSPJPush() throws CloneNotSupportedException {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, QUERIES[5]);
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
    
    LogicalNode optimized = LogicalOptimizer.optimize(ctx, plan);
    assertEquals(ExprType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    
    assertEquals(ExprType.SCAN, root.getSubNode().getType());
    scanNode = (ScanNode) root.getSubNode();
    assertEquals("employee", scanNode.getTableId());
  }
  
  @Test
  public final void testSPJ() throws CloneNotSupportedException {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, QUERIES[6]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    TestLogicalNode.testCloneLogicalNode(plan);
  }
  
  @Test
  public final void testJson() {
    QueryContext ctx = factory.create();
	  ParseTree block = analyzer.parse(ctx, QUERIES[9]);
	  LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
	  LogicalOptimizer.optimize(ctx, plan);
	    
	  String json = plan.toJSON();
	  System.out.println(json);
	  Gson gson = GsonCreator.getInstance();
	  LogicalNode fromJson = gson.fromJson(json, LogicalNode.class);
	  System.out.println(fromJson.toJSON());
	  assertEquals(ExprType.ROOT, fromJson.getType());
	  LogicalNode groupby = ((LogicalRootNode)fromJson).getSubNode();
	  assertEquals(ExprType.PROJECTION, groupby.getType());
	  LogicalNode projNode = ((ProjectionNode)groupby).getSubNode();
	  assertEquals(ExprType.GROUP_BY, projNode.getType());
	  LogicalNode scan = ((GroupbyNode)projNode).getSubNode();
	  assertEquals(ExprType.SCAN, scan.getType());
  }
  
  @Test
  public final void testVisitor() {
    // two relations
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, QUERIES[1]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    
    TestVisitor vis = new TestVisitor();
    plan.postOrder(vis);
    
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
    ParseTree block = analyzer.parse(ctx, QUERIES[10]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    LogicalOptimizer.optimize(ctx, plan);
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(ExprType.EXPRS, root.getSubNode().getType());
    Schema out = root.getOutputSchema();
    
    Iterator<Column> it = out.getColumns().iterator();
    Column col = it.next();
    assertEquals("column_0", col.getColumnName());
    col = it.next();
    assertEquals("column_1", col.getColumnName());
    col = it.next();
    assertEquals("mul", col.getColumnName());
  }
  
  @Test
  public final void testCreateIndex() {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, QUERIES[11]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    LogicalOptimizer.optimize(ctx, plan);
    LogicalRootNode root = (LogicalRootNode) plan;
    
    assertEquals(ExprType.CREATE_INDEX, root.getSubNode().getType());
    IndexWriteNode indexNode = (IndexWriteNode) root.getSubNode();
    assertEquals("idx_employee", indexNode.getIndexName());
    assertEquals("employee", indexNode.getTableName());
    assertEquals(false, indexNode.isUnique());
    assertEquals(2, indexNode.getSortSpecs().length);
    assertEquals("name", indexNode.getSortSpecs()[0].getSortKey().getColumnName());
    assertEquals(DataType.STRING, indexNode.getSortSpecs()[0].getSortKey().getDataType());
    assertEquals(true, indexNode.getSortSpecs()[0].isNullFirst());
    assertEquals("empid", indexNode.getSortSpecs()[1].getSortKey().getColumnName());
    assertEquals(DataType.INT, indexNode.getSortSpecs()[1].getSortKey().getDataType());
    assertEquals(false, indexNode.getSortSpecs()[1].isAscending());
    assertEquals(false, indexNode.getSortSpecs()[1].isNullFirst());
    assertEquals(IndexMethod.BITMAP, indexNode.getMethod());
  }
  
  static final String ALIAS [] = {
    "select deptName, sum(score) as total from score group by deptName",
    "select em.empId as id, sum(score) as total from employee as em inner join score using (em.deptName)"
  };
  
  @Test
  public final void testAlias() {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, ALIAS[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    plan = LogicalOptimizer.optimize(ctx, plan);
    LogicalRootNode root = (LogicalRootNode) plan;
        
    Schema finalSchema = root.getOutputSchema();
    Iterator<Column> it = finalSchema.getColumns().iterator();
    Column col = it.next();
    assertEquals("deptname", col.getColumnName());
    col = it.next();
    assertEquals("total", col.getColumnName());
    
    ctx = factory.create();
    block = analyzer.parse(ctx, ALIAS[1]);
    plan = LogicalPlanner.createPlan(ctx, block);
    plan = LogicalOptimizer.optimize(ctx, plan);
    root = (LogicalRootNode) plan;
    
    finalSchema = root.getOutputSchema();
    it = finalSchema.getColumns().iterator();
    col = it.next();
    assertEquals("id", col.getColumnName());
    col = it.next();
    assertEquals("total", col.getColumnName());
  }
  
  static final String CREATE_TABLE [] = {
    "create table table1 (name string, age int, earn long, score float) using csv location '/tmp/data' with ('csv.delimiter'='|')"
  };
  
  @Test
  public final void testCreateTableDef() {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, CREATE_TABLE[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    plan = LogicalOptimizer.optimize(ctx, plan);
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(ExprType.CREATE_TABLE, root.getSubNode().getType());
    CreateTableNode createTable = (CreateTableNode) root.getSubNode();
    
    Schema def = createTable.getSchema();
    assertEquals("name", def.getColumn(0).getColumnName());
    assertEquals(DataType.STRING, def.getColumn(0).getDataType());
    assertEquals("age", def.getColumn(1).getColumnName());
    assertEquals(DataType.INT, def.getColumn(1).getDataType());
    assertEquals("earn", def.getColumn(2).getColumnName());
    assertEquals(DataType.LONG, def.getColumn(2).getDataType());
    assertEquals("score", def.getColumn(3).getColumnName());
    assertEquals(DataType.FLOAT, def.getColumn(3).getDataType());    
    assertEquals(StoreType.CSV, createTable.getStoreType());    
    assertEquals("/tmp/data", createTable.getPath().toString());
    assertTrue(createTable.hasOptions());
    assertEquals("|", createTable.getOptions().get("csv.delimiter"));
  }
  
  private static final List<Set<Column>> testGenerateCuboidsResult 
    = Lists.newArrayList();
  private static final int numCubeColumns = 3;
  private static final Column [] testGenerateCuboids = new Column[numCubeColumns];
  
  private static final List<Set<Column>> testCubeByResult 
    = Lists.newArrayList();
  private static final Column [] testCubeByCuboids = new Column[2];
  static {
    testGenerateCuboids[0] = new Column("col1", DataType.INT);
    testGenerateCuboids[1] = new Column("col2", DataType.LONG);
    testGenerateCuboids[2] = new Column("col3", DataType.FLOAT);
    
    testGenerateCuboidsResult.add(new HashSet<Column>());
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[1]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[2]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0], 
        testGenerateCuboids[1]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0], 
        testGenerateCuboids[2]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[1], 
        testGenerateCuboids[2]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0], 
        testGenerateCuboids[1], testGenerateCuboids[2]));
    
    testCubeByCuboids[0] = new Column("employee.name", DataType.STRING);
    testCubeByCuboids[1] = new Column("employee.empid", DataType.INT);
    testCubeByResult.add(new HashSet<Column>());
    testCubeByResult.add(Sets.newHashSet(testCubeByCuboids[0]));
    testCubeByResult.add(Sets.newHashSet(testCubeByCuboids[1]));
    testCubeByResult.add(Sets.newHashSet(testCubeByCuboids[0], 
        testCubeByCuboids[1]));
  }
  
  @Test
  public final void testGenerateCuboids() {
    Column [] columns = new Column[3];
    
    columns[0] = new Column("col1", DataType.INT);
    columns[1] = new Column("col2", DataType.LONG);
    columns[2] = new Column("col3", DataType.FLOAT);
    
    List<Column[]> cube = LogicalPlanner.generateCuboids(columns);
    assertEquals(((int)Math.pow(2, numCubeColumns)), cube.size());    
    
    Set<Set<Column>> cuboids = Sets.newHashSet();
    for (Column [] cols : cube) {
      cuboids.add(Sets.newHashSet(cols));
    }
    
    for (Set<Column> result : testGenerateCuboidsResult) {
      assertTrue(cuboids.contains(result));
    }
  }
  
  static final String CUBE_ROLLUP [] = {
    "select name, empid, sum(score) from employee natural join score group by cube(name, empid)"
  };
  
  @Test
  public final void testCubeBy() {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, CUBE_ROLLUP[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);    
    plan = LogicalOptimizer.optimize(ctx, plan);
    
    Set<Set<Column>> cuboids = Sets.newHashSet();
    
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(ExprType.UNION, root.getSubNode().getType());
    UnionNode u0 = (UnionNode) root.getSubNode();
    assertEquals(ExprType.GROUP_BY, u0.getOuterNode().getType());
    assertEquals(ExprType.UNION, u0.getInnerNode().getType());
    GroupbyNode grp = (GroupbyNode) u0.getOuterNode();
    cuboids.add(Sets.newHashSet(grp.getGroupingColumns()));
    
    UnionNode u1 = (UnionNode) u0.getInnerNode();
    assertEquals(ExprType.GROUP_BY, u1.getOuterNode().getType());
    assertEquals(ExprType.UNION, u1.getInnerNode().getType());
    grp = (GroupbyNode) u1.getOuterNode();
    cuboids.add(Sets.newHashSet(grp.getGroupingColumns()));
    
    UnionNode u2 = (UnionNode) u1.getInnerNode();
    assertEquals(ExprType.GROUP_BY, u2.getOuterNode().getType());
    grp = (GroupbyNode) u2.getInnerNode();
    cuboids.add(Sets.newHashSet(grp.getGroupingColumns()));
    assertEquals(ExprType.GROUP_BY, u2.getInnerNode().getType());    
    grp = (GroupbyNode) u2.getOuterNode();
    cuboids.add(Sets.newHashSet(grp.getGroupingColumns()));
    
    assertEquals((int)Math.pow(2, 2), cuboids.size());
    for (Set<Column> result : testCubeByResult) {
      assertTrue(cuboids.contains(result));
    }
  }
  
  static final String setStatements [] = {
    "select deptName from employee where deptName like 'data%' union select deptName from score where deptName like 'data%'",
    "select deptName from employee union select deptName from score intersect select deptName from score",
    "select deptName from employee union select deptName from score except select deptName from score intersect select deptName from score"
  };
  
  @Test
  public final void testSetPlan() {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, setStatements[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);    
    plan = LogicalOptimizer.optimize(ctx, plan);
    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(ExprType.UNION, root.getSubNode().getType());
    UnionNode union = (UnionNode) root.getSubNode();
    assertEquals(ExprType.PROJECTION, union.getOuterNode().getType());
    ProjectionNode projL = (ProjectionNode) union.getOuterNode();
    assertEquals(ExprType.SELECTION, projL.getSubNode().getType());
    assertEquals(ExprType.PROJECTION, union.getInnerNode().getType());
    ProjectionNode projR = (ProjectionNode) union.getInnerNode();
    assertEquals(ExprType.SELECTION, projR.getSubNode().getType());
    
    // for testing multiple set statements
    ctx = factory.create();
    block = analyzer.parse(ctx, setStatements[1]);
    plan = LogicalPlanner.createPlan(ctx, block);    
    plan = LogicalOptimizer.optimize(ctx, plan);
    assertEquals(ExprType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;
    assertEquals(ExprType.UNION, root.getSubNode().getType());
    union = (UnionNode) root.getSubNode();
    assertEquals(ExprType.PROJECTION, union.getOuterNode().getType());
    assertEquals(ExprType.INTERSECT, union.getInnerNode().getType());
    IntersectNode intersect = (IntersectNode) union.getInnerNode();
    assertEquals(ExprType.PROJECTION, intersect.getOuterNode().getType());
    assertEquals(ExprType.PROJECTION, intersect.getInnerNode().getType());
    
    // for testing multiple set statements
    ctx = factory.create();
    block = analyzer.parse(ctx, setStatements[2]);
    plan = LogicalPlanner.createPlan(ctx, block);    
    plan = LogicalOptimizer.optimize(ctx, plan);
    assertEquals(ExprType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;
    assertEquals(ExprType.EXCEPT, root.getSubNode().getType());
    ExceptNode except = (ExceptNode) root.getSubNode();
    assertEquals(ExprType.UNION, except.getOuterNode().getType());
    assertEquals(ExprType.INTERSECT, except.getInnerNode().getType());
    union = (UnionNode) except.getOuterNode();
    assertEquals(ExprType.PROJECTION, union.getOuterNode().getType());
    assertEquals(ExprType.PROJECTION, union.getInnerNode().getType());
    intersect = (IntersectNode) except.getInnerNode();
    assertEquals(ExprType.PROJECTION, intersect.getOuterNode().getType());
    assertEquals(ExprType.PROJECTION, intersect.getInnerNode().getType());
  }
}