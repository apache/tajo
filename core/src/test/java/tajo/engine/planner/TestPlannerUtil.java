package tajo.engine.planner;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.datum.DatumFactory;
import tajo.engine.NtaTestingUtility;
import tajo.engine.QueryContext;
import tajo.engine.exec.eval.BinaryEval;
import tajo.engine.exec.eval.ConstEval;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.exec.eval.FieldEval;
import tajo.engine.function.builtin.NewSumInt;
import tajo.engine.parser.ParseTree;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.parser.QueryBlock;
import tajo.engine.planner.logical.*;
import tajo.engine.planner.physical.TupleComparator;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.util.List;

import static org.junit.Assert.*;

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

    FunctionDesc funcDesc = new FunctionDesc("sumtest", NewSumInt.class, FunctionType.AGGREGATION,
        new DataType [] {DataType.INT},
        new DataType [] {DataType.INT});

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
    
    root.postOrder(new TwoPhaseBuilder());
    
    System.out.println(root);
  }
  
  @Test
  public final void testTrasformTwoPhaseWithStore() {
    QueryContext ctx = factory.create();
    ParseTree block = analyzer.parse(ctx, TestLogicalPlanner.QUERIES[9]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    
    assertEquals(ExprType.ROOT, plan.getType());
    UnaryNode unary = (UnaryNode) plan;
    assertEquals(ExprType.PROJECTION, unary.getSubNode().getType());
    ProjectionNode proj = (ProjectionNode) unary.getSubNode();
    assertEquals(ExprType.GROUP_BY, proj.getSubNode().getType());
    GroupbyNode groupby = (GroupbyNode) proj.getSubNode();
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

  @Test
  public final void testIsJoinQual() {
    FieldEval f1 = new FieldEval("part.p_partkey", DataType.INT);
    FieldEval f2 = new FieldEval("partsupp.ps_partkey", DataType.INT);


    BinaryEval [] joinQuals = new BinaryEval[5];
    int idx = 0;
    joinQuals[idx++] = new BinaryEval(EvalNode.Type.EQUAL, f1, f2);
    joinQuals[idx++] = new BinaryEval(EvalNode.Type.LEQ, f1, f2);
    joinQuals[idx++] = new BinaryEval(EvalNode.Type.LTH, f1, f2);
    joinQuals[idx++] = new BinaryEval(EvalNode.Type.GEQ, f1, f2);
    joinQuals[idx] = new BinaryEval(EvalNode.Type.GTH, f1, f2);
    for (int i = 0; i < idx; i++) {
      assertTrue(PlannerUtil.isJoinQual(joinQuals[idx]));
    }

    BinaryEval [] wrongJoinQuals = new BinaryEval[5];
    idx = 0;
    wrongJoinQuals[idx++] = new BinaryEval(EvalNode.Type.OR, f1, f2);
    wrongJoinQuals[idx++] = new BinaryEval(EvalNode.Type.PLUS, f1, f2);
    wrongJoinQuals[idx++] = new BinaryEval(EvalNode.Type.LIKE, f1, f2);

    ConstEval f3 = new ConstEval(DatumFactory.createInt(1));
    wrongJoinQuals[idx] = new BinaryEval(EvalNode.Type.EQUAL, f1, f3);

    for (int i = 0; i < idx; i++) {
      assertFalse(PlannerUtil.isJoinQual(wrongJoinQuals[idx]));
    }
  }

  @Test
  public final void testGetJoinKeyPairs() {
    Schema outerSchema = new Schema();
    outerSchema.addColumn("employee.id1", DataType.INT);
    outerSchema.addColumn("employee.id2", DataType.INT);
    Schema innerSchema = new Schema();
    innerSchema.addColumn("people.fid1", DataType.INT);
    innerSchema.addColumn("people.fid2", DataType.INT);

    FieldEval f1 = new FieldEval("employee.id1", DataType.INT);
    FieldEval f2 = new FieldEval("people.fid1", DataType.INT);
    FieldEval f3 = new FieldEval("employee.id2", DataType.INT);
    FieldEval f4 = new FieldEval("people.fid2", DataType.INT);

    EvalNode joinQual = new BinaryEval(EvalNode.Type.EQUAL, f1, f2);

    // the case where part is the outer and partsupp is the inner.
    List<Column[]> pairs = PlannerUtil.getJoinKeyPairs(joinQual, outerSchema,  innerSchema);
    assertEquals(1, pairs.size());
    assertEquals("employee.id1", pairs.get(0)[0].getQualifiedName());
    assertEquals("people.fid1", pairs.get(0)[1].getQualifiedName());

    // after exchange of outer and inner
    pairs = PlannerUtil.getJoinKeyPairs(joinQual, innerSchema, outerSchema);
    assertEquals("people.fid1", pairs.get(0)[0].getQualifiedName());
    assertEquals("employee.id1", pairs.get(0)[1].getQualifiedName());

    // composited join key test
    EvalNode joinQual2 = new BinaryEval(EvalNode.Type.EQUAL, f3, f4);
    EvalNode compositedJoinQual = new BinaryEval(EvalNode.Type.AND, joinQual, joinQual2);
    pairs = PlannerUtil.getJoinKeyPairs(compositedJoinQual, outerSchema,  innerSchema);
    assertEquals(2, pairs.size());
    assertEquals("employee.id1", pairs.get(0)[0].getQualifiedName());
    assertEquals("people.fid1", pairs.get(0)[1].getQualifiedName());
    assertEquals("employee.id2", pairs.get(1)[0].getQualifiedName());
    assertEquals("people.fid2", pairs.get(1)[1].getQualifiedName());

    // after exchange of outer and inner
    pairs = PlannerUtil.getJoinKeyPairs(compositedJoinQual, innerSchema,  outerSchema);
    assertEquals(2, pairs.size());
    assertEquals("people.fid1", pairs.get(0)[0].getQualifiedName());
    assertEquals("employee.id1", pairs.get(0)[1].getQualifiedName());
    assertEquals("people.fid2", pairs.get(1)[0].getQualifiedName());
    assertEquals("employee.id2", pairs.get(1)[1].getQualifiedName());
  }

  @Test
  public final void testGetSortKeysFromJoinQual() {
    Schema outerSchema = new Schema();
    outerSchema.addColumn("employee.id1", DataType.INT);
    outerSchema.addColumn("employee.id2", DataType.INT);
    Schema innerSchema = new Schema();
    innerSchema.addColumn("people.fid1", DataType.INT);
    innerSchema.addColumn("people.fid2", DataType.INT);

    FieldEval f1 = new FieldEval("employee.id1", DataType.INT);
    FieldEval f2 = new FieldEval("people.fid1", DataType.INT);
    FieldEval f3 = new FieldEval("employee.id2", DataType.INT);
    FieldEval f4 = new FieldEval("people.fid2", DataType.INT);

    EvalNode joinQual = new BinaryEval(EvalNode.Type.EQUAL, f1, f2);
    QueryBlock.SortSpec [][] sortSpecs = PlannerUtil.getSortKeysFromJoinQual(joinQual, outerSchema, innerSchema);
    assertEquals(2, sortSpecs.length);
    assertEquals(1, sortSpecs[0].length);
    assertEquals(1, sortSpecs[1].length);
    assertEquals(outerSchema.getColumnByName("id1"), sortSpecs[0][0].getSortKey());
    assertEquals(innerSchema.getColumnByName("fid1"), sortSpecs[1][0].getSortKey());

    // tests for composited join key
    EvalNode joinQual2 = new BinaryEval(EvalNode.Type.EQUAL, f3, f4);
    EvalNode compositedJoinQual = new BinaryEval(EvalNode.Type.AND, joinQual, joinQual2);

    sortSpecs = PlannerUtil.getSortKeysFromJoinQual(compositedJoinQual, outerSchema, innerSchema);
    assertEquals(2, sortSpecs.length);
    assertEquals(2, sortSpecs[0].length);
    assertEquals(2, sortSpecs[1].length);
    assertEquals(outerSchema.getColumnByName("id1"), sortSpecs[0][0].getSortKey());
    assertEquals(outerSchema.getColumnByName("id2"), sortSpecs[0][1].getSortKey());
    assertEquals(innerSchema.getColumnByName("fid1"), sortSpecs[1][0].getSortKey());
    assertEquals(innerSchema.getColumnByName("fid2"), sortSpecs[1][1].getSortKey());
  }

  @Test
  public final void testComparatorsFromJoinQual() {
    Schema outerSchema = new Schema();
    outerSchema.addColumn("employee.id1", DataType.INT);
    outerSchema.addColumn("employee.id2", DataType.INT);
    Schema innerSchema = new Schema();
    innerSchema.addColumn("people.fid1", DataType.INT);
    innerSchema.addColumn("people.fid2", DataType.INT);

    FieldEval f1 = new FieldEval("employee.id1", DataType.INT);
    FieldEval f2 = new FieldEval("people.fid1", DataType.INT);
    FieldEval f3 = new FieldEval("employee.id2", DataType.INT);
    FieldEval f4 = new FieldEval("people.fid2", DataType.INT);

    EvalNode joinQual = new BinaryEval(EvalNode.Type.EQUAL, f1, f2);
    TupleComparator [] comparators = PlannerUtil.getComparatorsFromJoinQual(joinQual, outerSchema, innerSchema);

    Tuple t1 = new VTuple(2);
    t1.put(0, DatumFactory.createInt(1));
    t1.put(1, DatumFactory.createInt(2));

    Tuple t2 = new VTuple(2);
    t2.put(0, DatumFactory.createInt(2));
    t2.put(1, DatumFactory.createInt(3));

    TupleComparator outerComparator = comparators[0];
    assertTrue(outerComparator.compare(t1, t2) < 0);
    assertTrue(outerComparator.compare(t2, t1) > 0);

    TupleComparator innerComparator = comparators[1];
    assertTrue(innerComparator.compare(t1, t2) < 0);
    assertTrue(innerComparator.compare(t2, t1) > 0);

    // tests for composited join key
    EvalNode joinQual2 = new BinaryEval(EvalNode.Type.EQUAL, f3, f4);
    EvalNode compositedJoinQual = new BinaryEval(EvalNode.Type.AND, joinQual, joinQual2);
    comparators = PlannerUtil.getComparatorsFromJoinQual(compositedJoinQual, outerSchema, innerSchema);

    outerComparator = comparators[0];
    assertTrue(outerComparator.compare(t1, t2) < 0);
    assertTrue(outerComparator.compare(t2, t1) > 0);

    innerComparator = comparators[1];
    assertTrue(innerComparator.compare(t1, t2) < 0);
    assertTrue(innerComparator.compare(t2, t1) > 0);
  }
}
