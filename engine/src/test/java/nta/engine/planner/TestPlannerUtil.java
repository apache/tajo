package nta.engine.planner;

import static org.junit.Assert.assertEquals;
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
import nta.engine.parser.ParseTree;
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
import nta.engine.planner.logical.UnaryNode;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Hyunsik Choi
 */
public class TestPlannerUtil {
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

  @Test
  public final void testTransformTwoPhase() {
    // without 'having clause'
    QueryContext ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, 
        TestLogicalPlanner.QUERIES[7]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);

    assertEquals(ExprType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalPlanner.testQuery7(root.getSubNode());
    
    root.accept(new TwoPhaseBuilder());
    
    System.out.println(root);
  }
  
  @Test
  public final void testTrasformTwoPhaseWithStore() {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, TestLogicalPlanner.QUERIES[9]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    
    assertEquals(ExprType.ROOT, plan.getType());
    UnaryNode unary = (UnaryNode) plan;
    
    assertEquals(ExprType.GROUP_BY, unary.getSubNode().getType());
    GroupbyNode groupby = (GroupbyNode) unary.getSubNode();
    unary = (UnaryNode) PlannerUtil.transformGroupbyTo2PWithStore(
        groupby, "test");
    assertEquals(ExprType.STORE, unary.getSubNode().getType());
    unary = (UnaryNode) unary.getSubNode();
    
    assertEquals(groupby.getInputSchema(), unary.getOutputSchema());
    
    assertEquals(ExprType.GROUP_BY, unary.getSubNode().getType());
  }
  
  private final class TwoPhaseBuilder implements LogicalNodeVisitor {
    @Override
    public void visit(LogicalNode node) {
      if (node.getType() == ExprType.GROUP_BY) {
        PlannerUtil.transformGroupbyTo2P((GroupbyNode) node);
      }
    }    
  }
  
  @Test
  public final void testFindTopNode() throws CloneNotSupportedException {
    // two relations
    QueryContext ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, 
        TestLogicalPlanner.QUERIES[1]);
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
    
    LogicalNode node = PlannerUtil.findTopNode(root, ExprType.ROOT);
    assertEquals(ExprType.ROOT, node.getType());
    
    node = PlannerUtil.findTopNode(root, ExprType.PROJECTION);
    assertEquals(ExprType.PROJECTION, node.getType());
    
    node = PlannerUtil.findTopNode(root, ExprType.JOIN);
    assertEquals(ExprType.JOIN, node.getType());
    
    node = PlannerUtil.findTopNode(root, ExprType.SCAN);
    assertEquals(ExprType.SCAN, node.getType());
  }
}
