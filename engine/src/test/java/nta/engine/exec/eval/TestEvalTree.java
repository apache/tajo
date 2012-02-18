package nta.engine.exec.eval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import nta.catalog.CatalogService;
import nta.catalog.Column;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.exec.eval.EvalNode.Type;
import nta.engine.function.Function;
import nta.engine.json.GsonCreator;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

/**
 * @author Hyunsik Choi
 */
public class TestEvalTree {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  public static class TestSum extends Function {

    public TestSum() {
      super(new Column[] { new Column("arg1", DataType.INT),
          new Column("arg2", DataType.INT) });
    }

    @Override
    public Datum invoke(Datum... data) {
      if(data[1] == null)
        return data[0];
      return data[0].plus(data[1]);
    }

    @Override
    public DataType getResType() {
      return DataType.INT;
    }
    
    public String toJSON() {
    	return GsonCreator.getInstance().toJson(this, Function.class);
    }
  }
  
  public static class TestAggSum extends Function {

    public TestAggSum() {
      super(new Column[] { new Column("arg1", DataType.INT)});
    }

    @Override
    public Datum invoke(Datum... data) {
      return data[0].plus(data[1]);
    }

    @Override
    public DataType getResType() {
      return DataType.INT;
    }
    
    public String toJSON() {
    	Gson gson = GsonCreator.getInstance();
    	return gson.toJson(this, Function.class);
    }
  }

  static String[] QUERIES = {
      "select name, score, age from people where score > 30", // 0
      "select name, score, age from people where score * age", // 1
      "select name, score, age from people where sum(score * age, 50)", // 2
      "select 2+3", // 3
      "select aggsum(score) from people" // 4
  };

  @Test
  public final void testFunctionEval() throws Exception {
    NtaTestingUtility util = new NtaTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    CatalogService cat = util.getMiniCatalogCluster().getCatalog();

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("age", DataType.INT);

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta, new Path("file:///"));
    cat.addTable(desc);

    FunctionDesc funcMeta = new FunctionDesc("sum", TestSum.class,
        FunctionType.GENERAL, DataType.INT, 
        new DataType [] { DataType.INT, DataType.INT});
    cat.registerFunction(funcMeta);
    
    funcMeta = new FunctionDesc("aggsum", TestAggSum.class,
        FunctionType.AGGREGATION, DataType.INT, 
        new DataType [] { DataType.INT});
    cat.registerFunction(funcMeta);

    Tuple tuple = new VTuple(3);
    tuple.put(DatumFactory.createString("hyunsik"), 
        DatumFactory.createInt(500), 
        DatumFactory.createInt(30));

    QueryContext.Factory factory = new QueryContext.Factory(cat);
    QueryAnalyzer analyzer = new QueryAnalyzer(cat);
    
    QueryBlock block = null;
    EvalNode expr = null;

    Schema peopleSchema = cat.getTableDesc("people").getMeta().getSchema();
    QueryContext ctx = factory.create();
    block = analyzer.parse(ctx, QUERIES[0]);
    expr = block.getWhereCondition();
    assertEquals(true, expr.eval(peopleSchema, tuple)
        .asBool());

    block = analyzer.parse(ctx, QUERIES[1]);
    expr = block.getWhereCondition();
    assertEquals(15000, expr.eval(peopleSchema, tuple).asInt());
    assertCloneEqual(expr);

    block = analyzer.parse(ctx, QUERIES[2]);
    expr = block.getWhereCondition();
    assertEquals(15050, expr.eval(peopleSchema, tuple).asInt());
    assertCloneEqual(expr);
    
    block = analyzer.parse(ctx, QUERIES[2]);
    expr = block.getWhereCondition();
    assertEquals(15050, expr.eval(peopleSchema, tuple).asInt());
    assertCloneEqual(expr);
    
    // Aggregation function test
    block = analyzer.parse(ctx, QUERIES[4]);
    expr = block.getTargetList()[0].getEvalTree();
    Datum accumulated = DatumFactory.createInt(0);
    
    final int tuplenum = 10;
    Tuple [] tuples = new Tuple[tuplenum];
    for (int i=0; i < tuplenum; i++) {
      tuples[i] = new VTuple(3);
      tuples[i].put(DatumFactory.createString("hyunsik")); 
      tuples[i].put(1,DatumFactory.createInt(i+1)); 
      tuples[i].put(2, DatumFactory.createInt(30));
    }
    
    int sum = 0;
    for (int i=0; i < tuplenum; i++) {
      accumulated = expr.eval(peopleSchema, tuples[i], accumulated);
      sum = sum + (i+1);
      assertEquals(sum, accumulated.asInt());
    }
    
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }
  
  
  @Test
  public void testTupleEval() throws CloneNotSupportedException {
    ConstEval e1 = new ConstEval(DatumFactory.createInt(1));
    assertCloneEqual(e1);
    FieldEval e2 = new FieldEval("table1.score", DataType.INT); // it indicates
    assertCloneEqual(e2);

    Schema schema1 = new Schema();
    schema1.addColumn("table1.id", DataType.INT);
    schema1.addColumn("table1.score", DataType.INT);
    
    BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);
    assertCloneEqual(expr);
    VTuple tuple = new VTuple(2);
    tuple.put(0, DatumFactory.createInt(1)); // put 0th field
    tuple.put(1, DatumFactory.createInt(99)); // put 0th field

    // the result of evaluation must be 100.
    assertEquals(expr.eval(schema1, tuple).asInt(), 100);
  }

  public static class MockTrueEval extends EvalNode {

    public MockTrueEval() {
      super(Type.CONST);
    }

    @Override
    public Datum eval(Schema schema, Tuple tuple, Datum... args) {
      return DatumFactory.createBool(true);
    }

    @Override
    public String getName() {
      return this.getClass().getName();
    }

    @Override
    public DataType getValueType() {
      return DataType.BOOLEAN;
    }

  }

  public static class MockFalseExpr extends EvalNode {

    public MockFalseExpr() {
      super(Type.CONST);
    }

    @Override
    public Datum eval(Schema schema, Tuple tuple, Datum... args) {
      return DatumFactory.createBool(false);
    }

    @Override
    public String getName() {
      return this.getClass().getName();
    }

    @Override
    public DataType getValueType() {
      return DataType.BOOLEAN;
    }
  }

  @Test
  public void testAndTest() {
    MockTrueEval trueExpr = new MockTrueEval();
    MockFalseExpr falseExpr = new MockFalseExpr();

    BinaryEval andExpr = new BinaryEval(Type.AND, trueExpr, trueExpr);
    assertTrue(andExpr.eval(null, null).asBool());

    andExpr = new BinaryEval(Type.AND, falseExpr, trueExpr);
    assertFalse(andExpr.eval(null, null).asBool());

    andExpr = new BinaryEval(Type.AND, trueExpr, falseExpr);
    assertFalse(andExpr.eval(null, null).asBool());

    andExpr = new BinaryEval(Type.AND, falseExpr, falseExpr);
    assertFalse(andExpr.eval(null, null).asBool());
  }

  @Test
  public void testOrTest() {
    MockTrueEval trueExpr = new MockTrueEval();
    MockFalseExpr falseExpr = new MockFalseExpr();

    BinaryEval orExpr = new BinaryEval(Type.OR, trueExpr, trueExpr);
    assertTrue(orExpr.eval(null, null).asBool());

    orExpr = new BinaryEval(Type.OR, falseExpr, trueExpr);
    assertTrue(orExpr.eval(null, null).asBool());

    orExpr = new BinaryEval(Type.OR, trueExpr, falseExpr);
    assertTrue(orExpr.eval(null, null).asBool());

    orExpr = new BinaryEval(Type.OR, falseExpr, falseExpr);
    assertFalse(orExpr.eval(null, null).asBool());
  }

  @Test
  public final void testCompOperator() {
    ConstEval e1 = null;
    ConstEval e2 = null;
    BinaryEval expr = null;

    // Constant
    e1 = new ConstEval(DatumFactory.createInt(9));
    e2 = new ConstEval(DatumFactory.createInt(34));
    expr = new BinaryEval(Type.LTH, e1, e2);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.LEQ, e1, e2);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.LTH, e2, e1);
    assertFalse(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.LEQ, e2, e1);
    assertFalse(expr.eval(null, null).asBool());

    expr = new BinaryEval(Type.GTH, e2, e1);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.GEQ, e2, e1);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.GTH, e1, e2);
    assertFalse(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.GEQ, e1, e2);
    assertFalse(expr.eval(null, null).asBool());

    BinaryEval plus = new BinaryEval(Type.PLUS, e1, e2);
    expr = new BinaryEval(Type.LTH, e1, plus);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.LEQ, e1, plus);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.LTH, plus, e1);
    assertFalse(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.LEQ, plus, e1);
    assertFalse(expr.eval(null, null).asBool());

    expr = new BinaryEval(Type.GTH, plus, e1);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.GEQ, plus, e1);
    assertTrue(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.GTH, e1, plus);
    assertFalse(expr.eval(null, null).asBool());
    expr = new BinaryEval(Type.GEQ, e1, plus);
    assertFalse(expr.eval(null, null).asBool());
  }

  @Test
  public final void testArithmaticsOperator() throws CloneNotSupportedException {
    ConstEval e1 = null;
    ConstEval e2 = null;

    // PLUS
    e1 = new ConstEval(DatumFactory.createInt(9));
    e2 = new ConstEval(DatumFactory.createInt(34));
    BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);
    assertEquals(expr.eval(null, null).asInt(), 43);
    assertCloneEqual(expr);
    
    // MINUS
    e1 = new ConstEval(DatumFactory.createInt(5));
    e2 = new ConstEval(DatumFactory.createInt(2));
    expr = new BinaryEval(Type.MINUS, e1, e2);
    assertEquals(expr.eval(null, null).asInt(), 3);
    assertCloneEqual(expr);
    
    // MULTIPLY
    e1 = new ConstEval(DatumFactory.createInt(5));
    e2 = new ConstEval(DatumFactory.createInt(2));
    expr = new BinaryEval(Type.MULTIPLY, e1, e2);
    assertEquals(expr.eval(null, null).asInt(), 10);
    assertCloneEqual(expr);
    
    // DIVIDE
    e1 = new ConstEval(DatumFactory.createInt(10));
    e2 = new ConstEval(DatumFactory.createInt(5));
    expr = new BinaryEval(Type.DIVIDE, e1, e2);
    assertEquals(expr.eval(null, null).asInt(), 2);
    assertCloneEqual(expr);
  }

  @Test
  public final void testGetReturnType() {
    ConstEval e1 = null;
    ConstEval e2 = null;

    // PLUS
    e1 = new ConstEval(DatumFactory.createInt(9));
    e2 = new ConstEval(DatumFactory.createInt(34));
    BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);
    assertEquals(DataType.INT, expr.getValueType());

    expr = new BinaryEval(Type.LTH, e1, e2);
    assertTrue(expr.eval(null, null).asBool());
    assertEquals(DataType.BOOLEAN, expr.getValueType());

    e1 = new ConstEval(DatumFactory.createDouble(9.3));
    e2 = new ConstEval(DatumFactory.createDouble(34.2));
    expr = new BinaryEval(Type.PLUS, e1, e2);
    assertEquals(DataType.DOUBLE, expr.getValueType());
  }
  
  @Test
  public final void testEquals() throws CloneNotSupportedException {
    ConstEval e1 = null;
    ConstEval e2 = null;

    // PLUS
    e1 = new ConstEval(DatumFactory.createInt(34));
    e2 = new ConstEval(DatumFactory.createInt(34));
    assertEquals(e1, e2);
    
    BinaryEval plus1 = new BinaryEval(Type.PLUS, e1, e2);
    BinaryEval plus2 = new BinaryEval(Type.PLUS, e2, e1);
    assertEquals(plus1, plus2);
    
    ConstEval e3 = new ConstEval(DatumFactory.createInt(29));
    BinaryEval plus3 = new BinaryEval(Type.PLUS, e1, e3);
    assertFalse(plus1.equals(plus3));
    
    // LTH
    ConstEval e4 = new ConstEval(DatumFactory.createInt(9));
    ConstEval e5 = new ConstEval(DatumFactory.createInt(34));
    BinaryEval compExpr1 = new BinaryEval(Type.LTH, e4, e5);
    assertCloneEqual(compExpr1);
    
    ConstEval e6 = new ConstEval(DatumFactory.createInt(9));
    ConstEval e7 = new ConstEval(DatumFactory.createInt(34));
    BinaryEval compExpr2 = new BinaryEval(Type.LTH, e6, e7);
    assertCloneEqual(compExpr2);
    
    assertTrue(compExpr1.equals(compExpr2));
  }
  
  @Test
  public final void testJson() throws CloneNotSupportedException {
    ConstEval e1 = null;
    ConstEval e2 = null;

    // 29 > (34 + 5) + (5 + 34)
    e1 = new ConstEval(DatumFactory.createInt(34));
    e2 = new ConstEval(DatumFactory.createInt(5));
    assertCloneEqual(e1); 
    
    BinaryEval plus1 = new BinaryEval(Type.PLUS, e1, e2);
    assertCloneEqual(plus1);
    BinaryEval plus2 = new BinaryEval(Type.PLUS, e2, e1);
    assertCloneEqual(plus2);
    BinaryEval plus3 = new BinaryEval(Type.PLUS, plus2, plus1);
    assertCloneEqual(plus3);
    
    ConstEval e3 = new ConstEval(DatumFactory.createInt(29));
    BinaryEval gth = new BinaryEval(Type.GTH, e3, plus3);
    assertCloneEqual(gth);
    
    String json = gth.toJSON();
    EvalNode eval = GsonCreator.getInstance().fromJson(json, EvalNode.class);
    assertCloneEqual(eval);
    
    assertEquals(gth.getType(), eval.getType());
    assertEquals(e3.getType(), eval.getLeftExpr().getType());
    assertEquals(plus3.getType(), eval.getRightExpr().getType());
    assertEquals(plus3.getLeftExpr(), eval.getRightExpr().getLeftExpr());
    assertEquals(plus3.getRightExpr(), eval.getRightExpr().getRightExpr());
    assertEquals(plus2.getLeftExpr(), eval.getRightExpr().getLeftExpr().getLeftExpr());
    assertEquals(plus2.getRightExpr(), eval.getRightExpr().getLeftExpr().getRightExpr());
    assertEquals(plus1.getLeftExpr(), eval.getRightExpr().getRightExpr().getLeftExpr());
    assertEquals(plus1.getRightExpr(), eval.getRightExpr().getRightExpr().getRightExpr());
  }
  
  private void assertCloneEqual(EvalNode eval) throws CloneNotSupportedException {
    EvalNode copy = (EvalNode) eval.clone();
    assertEquals(eval, copy);
  }
}
