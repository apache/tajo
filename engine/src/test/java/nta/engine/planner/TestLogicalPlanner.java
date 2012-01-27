package nta.engine.planner;

import static org.junit.Assert.assertEquals;
import nta.catalog.CatalogServer;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.engine.QueryContext;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.SortNode;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Hyunsik Choi
 */
public class TestLogicalPlanner {
  private CatalogServer catalog;
  private QueryContext.Factory factory;

  @Before
  public void setUp() throws Exception {
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

    TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
    TableDesc people = new TableDescImpl("employee", meta);
    people.setPath(new Path("file:///"));
    catalog = new CatalogServer(new NtaConf());
    catalog.addTable(people);

    TableDesc student = new TableDescImpl("dept", schema2, StoreType.CSV);
    student.setPath(new Path("file:///"));
    catalog.addTable(student);

    TableDesc score = new TableDescImpl("score", schema3, StoreType.CSV);
    score.setPath(new Path("file:///"));
    catalog.addTable(score);

    FunctionDesc funcDesc = new FunctionDesc("sum", TestSum.class,
        FunctionType.GENERAL, DataType.INT, new DataType[] { DataType.INT });

    catalog.registerFunction(funcDesc);

    factory = new QueryContext.Factory(catalog);
  }

  @After
  public void tearDown() throws Exception {
  }

  private String[] QUERIES = {
      "select name, empId, deptName from employee where empId > 500", // 0
      "select name, empId, e.deptName, manager from employee as e, dept as dp", // 1
      "select name, empId, e.deptName, manager, score from employee as e, dept, score", // 2
      "select p.deptName, sum(score) from dept as p, score group by p.deptName having sum(score) > 30", // 3
      "select p.deptName, score from dept as p, score order by score asc", // 4
      "select name from employee where empId = 100", // 5
      "select name, score from employee, score" // 6
  };

  @Test
  public final void testSingleRelation() {
    QueryBlock block = QueryAnalyzer.parse(QUERIES[0], catalog);
    QueryContext ctx = factory.create(block);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    assertEquals(ExprType.ROOT, plan.getType());
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
  public final void testMultiRelations() {
    // two relations
    QueryBlock block = QueryAnalyzer.parse(QUERIES[1], catalog);
    QueryContext ctx = factory.create(block);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);

    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

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
    block = QueryAnalyzer.parse(QUERIES[2], catalog);
    ctx = factory.create(block);
    plan = LogicalPlanner.createPlan(ctx, block);

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
  public final void testGroupby() {
    QueryBlock block = QueryAnalyzer.parse(QUERIES[3], catalog);
    QueryContext ctx = factory.create(block);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);

    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    assertEquals(ExprType.PROJECTION, root.getSubNode().getType());
    ProjectionNode projNode = (ProjectionNode) root.getSubNode();

    assertEquals(ExprType.GROUP_BY, projNode.getSubNode().getType());
    GroupbyNode groupByNode = (GroupbyNode) projNode.getSubNode();

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

  @Test
  public final void testOrderBy() {
    QueryBlock block = QueryAnalyzer.parse(QUERIES[4], catalog);
    QueryContext ctx = factory.create(block);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);

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
  public final void testSPJPush() {
    QueryBlock block = QueryAnalyzer.parse(QUERIES[5], catalog);
    QueryContext ctx = factory.create(block);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
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
  public final void testSPJ() {
    QueryBlock block = QueryAnalyzer.parse(QUERIES[6], catalog);
    QueryContext ctx = factory.create(block);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
/*    System.out.println(plan);
    System.out.println("-------------------");
    LogicalOptimizer.optimize(ctx, plan);
    System.out.println(plan);*/
  }
}
