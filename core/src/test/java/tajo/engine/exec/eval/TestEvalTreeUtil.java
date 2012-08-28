package tajo.engine.exec.eval;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.datum.DatumFactory;
import tajo.engine.QueryContext;
import tajo.engine.TajoTestingUtility;
import tajo.engine.exception.InternalException;
import tajo.engine.exec.eval.EvalNode.Type;
import tajo.engine.exec.eval.TestEvalTree.TestSum;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.parser.QueryBlock;
import tajo.engine.parser.QueryBlock.Target;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Hyunsik Choi
 */
public class TestEvalTreeUtil {
  static TajoTestingUtility util;
  static CatalogService catalog = null;
  static EvalNode expr1;
  static EvalNode expr2;
  static EvalNode expr3;
  static QueryAnalyzer analyzer;
  static QueryContext.Factory factory = new QueryContext.Factory(catalog);

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("age", DataType.INT);

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta, new Path("file:///"));
    catalog.addTable(desc);

    FunctionDesc funcMeta = new FunctionDesc("sum", TestSum.class, FunctionType.GENERAL,
        new DataType [] {DataType.INT},
        new DataType [] {DataType.INT,DataType.INT});
    catalog.registerFunction(funcMeta);

    factory = new QueryContext.Factory(catalog);
    analyzer = new QueryAnalyzer(catalog);
    
    QueryBlock block;

    QueryContext ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, TestEvalTree.QUERIES[0]);
    expr1 = block.getWhereCondition();

    block = (QueryBlock) analyzer.parse(ctx, TestEvalTree.QUERIES[1]);
    expr2 = block.getWhereCondition();
    
    block = (QueryBlock) analyzer.parse(ctx, TestEvalTree.QUERIES[2]);
    expr3 = block.getWhereCondition();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();    
  }

  @Test
  public final void testChangeColumnRef() throws CloneNotSupportedException {
    EvalNode copy = (EvalNode)expr1.clone();
    EvalTreeUtil.changeColumnRef(copy, "people.score", "newscore");
    Set<Column> set = EvalTreeUtil.findDistinctRefColumns(copy);
    assertEquals(1, set.size());
    assertTrue(set.contains(new Column("newscore", DataType.INT)));
    
    copy = (EvalNode)expr2.clone();
    EvalTreeUtil.changeColumnRef(copy, "people.age", "sum_age");
    set = EvalTreeUtil.findDistinctRefColumns(copy);
    assertEquals(2, set.size());
    assertTrue(set.contains(new Column("people.score", DataType.INT)));
    assertTrue(set.contains(new Column("sum_age", DataType.INT)));
    
    copy = (EvalNode)expr3.clone();
    EvalTreeUtil.changeColumnRef(copy, "people.age", "sum_age");
    set = EvalTreeUtil.findDistinctRefColumns(copy);
    assertEquals(2, set.size());
    assertTrue(set.contains(new Column("people.score", DataType.INT)));
    assertTrue(set.contains(new Column("sum_age", DataType.INT)));
  }

  @Test
  public final void testFindAllRefColumns() {    
    Set<Column> set = EvalTreeUtil.findDistinctRefColumns(expr1);
    assertEquals(1, set.size());
    assertTrue(set.contains(new Column("people.score", DataType.INT)));
    
    set = EvalTreeUtil.findDistinctRefColumns(expr2);
    assertEquals(2, set.size());
    assertTrue(set.contains(new Column("people.score", DataType.INT)));
    assertTrue(set.contains(new Column("people.age", DataType.INT)));
    
    set = EvalTreeUtil.findDistinctRefColumns(expr3);
    assertEquals(2, set.size());
    assertTrue(set.contains(new Column("people.score", DataType.INT)));
    assertTrue(set.contains(new Column("people.age", DataType.INT)));
  }
  
  public static final String [] QUERIES = {
    "select 3 + 4 as plus, (3.5 * 2) as mul", // 0
    "select (score + 3) < 4, age > 5 from people", // 1
    "select score from people where score > 7", // 2
    "select score from people where (10 * 2) * (score + 2) > 20 + 30 + 10", // 3
    "select score from people where 10 * 2 > score * 10", // 4
    "select score from people where score < 10 and 4 < score", // 5
    "select score from people where score < 10 and 4 < score and age > 5", // 6
  };
  
  @Test
  public final void testGetSchemaFromTargets() throws InternalException {
    QueryContext ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[0]);
    Schema schema = 
        EvalTreeUtil.getSchemaByTargets(null, block.getTargetList());
    Column col1 = schema.getColumn(0);
    Column col2 = schema.getColumn(1);
    assertEquals("plus", col1.getColumnName());
    assertEquals(DataType.INT, col1.getDataType());
    assertEquals("mul", col2.getColumnName());
    assertEquals(DataType.DOUBLE, col2.getDataType());
  }
  
  @Test
  public final void testGetContainExprs() throws CloneNotSupportedException {
    QueryContext ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[1]);
    Target [] targets = block.getTargetList();
    
    Column col1 = new Column("people.score", DataType.INT);
    Collection<EvalNode> exprs = EvalTreeUtil.getContainExpr(targets[0].getEvalTree(), col1);
    EvalNode node = exprs.iterator().next();
    assertEquals(Type.LTH, node.getType());
    assertEquals(Type.PLUS, node.getLeftExpr().getType());
    assertEquals(new ConstEval(DatumFactory.createInt(4)), node.getRightExpr());
    
    Column col2 = new Column("people.age", DataType.INT);
    exprs = EvalTreeUtil.getContainExpr(targets[1].getEvalTree(), col2);
    node = exprs.iterator().next();
    assertEquals(Type.GTH, node.getType());
    assertEquals("people.age", node.getLeftExpr().getName());
    assertEquals(new ConstEval(DatumFactory.createInt(5)), node.getRightExpr());
  }
  
  @Test
  public final void testGetCNF() {
    // "select score from people where score < 10 and 4 < score "
    QueryContext ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[5]);
    EvalNode node = block.getWhereCondition();
    EvalNode [] cnf = EvalTreeUtil.getConjNormalForm(node);
    
    Column col1 = new Column("people.score", DataType.INT);
    
    assertEquals(2, cnf.length);
    EvalNode first = cnf[0];
    EvalNode second = cnf[1];
    
    FieldEval field = (FieldEval) first.getLeftExpr();
    assertEquals(col1, field.getColumnRef());
    assertEquals(Type.LTH, first.getType());
    EvalContext firstRCtx = first.getRightExpr().newContext();
    first.getRightExpr().eval(firstRCtx, null,  null);
    assertEquals(10, first.getRightExpr().terminate(firstRCtx).asInt());
    
    field = (FieldEval) second.getRightExpr();
    assertEquals(col1, field.getColumnRef());
    assertEquals(Type.LTH, second.getType());
    EvalContext secondLCtx = second.getLeftExpr().newContext();
    second.getLeftExpr().eval(secondLCtx, null,  null);
    assertEquals(4, second.getLeftExpr().terminate(secondLCtx).asInt());
  }
  
  @Test
  public final void testTransformCNF2Singleton() {
    // "select score from people where score < 10 and 4 < score "
    QueryContext ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[6]);
    EvalNode node = block.getWhereCondition();
    EvalNode [] cnf1 = EvalTreeUtil.getConjNormalForm(node);
    assertEquals(3, cnf1.length);
    
    EvalNode conj = EvalTreeUtil.transformCNF2Singleton(cnf1);
    EvalNode [] cnf2 = EvalTreeUtil.getConjNormalForm(conj);
    
    Set<EvalNode> set1 = Sets.newHashSet(cnf1);
    Set<EvalNode> set2 = Sets.newHashSet(cnf2);
    assertEquals(set1, set2);
  }
  
  @Test
  public final void testSimplify() {
    QueryContext ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[0]);
    Target [] targets = block.getTargetList();
    EvalNode node = AlgebraicUtil.simplify(targets[0].getEvalTree());
    EvalContext nodeCtx = node.newContext();
    assertEquals(Type.CONST, node.getType());
    node.eval(nodeCtx, null, null);
    assertEquals(7, node.terminate(nodeCtx).asInt());
    node = AlgebraicUtil.simplify(targets[1].getEvalTree());
    assertEquals(Type.CONST, node.getType());
    nodeCtx = node.newContext();
    node.eval(nodeCtx, null, null);
    assertTrue(7.0d == node.terminate(nodeCtx).asDouble());
    
    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, QUERIES[1]);
    targets = block.getTargetList();
    Column col1 = new Column("people.score", DataType.INT);
    Collection<EvalNode> exprs = EvalTreeUtil.getContainExpr(targets[0].getEvalTree(), col1);
    node = exprs.iterator().next();
    System.out.println(AlgebraicUtil.simplify(node));
  }
  
  @Test
  public final void testConatainSingleVar() {
    QueryContext ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[2]);
    EvalNode node = block.getWhereCondition();
    assertEquals(true, AlgebraicUtil.containSingleVar(node));
    
    block = (QueryBlock) analyzer.parse(ctx, QUERIES[3]);
    node = block.getWhereCondition();
    assertEquals(true, AlgebraicUtil.containSingleVar(node));
  }
  
  @Test
  public final void testTranspose() {
    QueryContext ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[2]);
    EvalNode node = block.getWhereCondition();
    assertEquals(true, AlgebraicUtil.containSingleVar(node));
    
    Column col1 = new Column("people.score", DataType.INT);
    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, QUERIES[3]);
    node = block.getWhereCondition();    
    // we expect that score < 3
    EvalNode transposed = AlgebraicUtil.transpose(node, col1);
    assertEquals(Type.GTH, transposed.getType());
    FieldEval field = (FieldEval) transposed.getLeftExpr(); 
    assertEquals(col1, field.getColumnRef());
    EvalContext evalCtx = transposed.getRightExpr().newContext();
    transposed.getRightExpr().eval(evalCtx, null, null);
    assertEquals(1, transposed.getRightExpr().terminate(evalCtx).asInt());
            
    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, QUERIES[4]);
    node = block.getWhereCondition();    
    // we expect that score < 3
    transposed = AlgebraicUtil.transpose(node, col1);
    assertEquals(Type.LTH, transposed.getType());
    field = (FieldEval) transposed.getLeftExpr(); 
    assertEquals(col1, field.getColumnRef());
    evalCtx = transposed.getRightExpr().newContext();
    transposed.getRightExpr().eval(evalCtx, null, null);
    assertEquals(2, transposed.getRightExpr().terminate(evalCtx).asInt());
  }

  @Test
  public final void testFindDistinctAggFunctions() {
    QueryContext ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, "select sum(score) + max(age) from people");
    List<AggFuncCallEval> list = EvalTreeUtil.findDistinctAggFunction(block.getTargetList()[0].getEvalTree());
    assertEquals(2, list.size());
    Set<String> result = Sets.newHashSet(new String [] {"max", "sum"});
    for (AggFuncCallEval eval : list) {
      assertTrue(result.contains(eval.getName()));
    }
  }
}