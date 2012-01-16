package nta.engine.exec.expr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import nta.catalog.Catalog;
import nta.catalog.ColumnBase;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.exception.InternalException;
import nta.engine.exception.NQLSyntaxException;
import nta.engine.exec.eval.BinaryEval;
import nta.engine.exec.eval.ConstEval;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.EvalNode.Type;
import nta.engine.exec.eval.FieldEval;
import nta.engine.function.Function;
import nta.engine.parser.NQLCompiler;
import nta.engine.parser.QueryBlock;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEvalTree {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  

  public static class Sum extends Function {

    public Sum() {
      super(new ColumnBase[] { new ColumnBase("arg1", DataType.INT),
          new ColumnBase("arg2", DataType.INT) });
    }

    @Override
    public Datum invoke(Datum... data) {
      return data[0].plus(data[1]);
    }

    @Override
    public DataType getResType() {
      return DataType.ANY;
    }
  }

  private String[] QUERIES = {
      "select name, score, age from people where score > 30", // 0
      "select name, score, age from people where score * age", // 1
      "select name, score, age from people where sum(score * age, 50)", // 2
      "select 2+3", // 3
  };

  @Test
  public final void testEvalExprTreeBin() throws NQLSyntaxException,
      InternalException {

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("age", DataType.INT);

    TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta);
    desc.setURI(URI.create("file:///"));
    Catalog cat = new Catalog(new NtaConf());
    cat.addTable(desc);

    FunctionDesc funcMeta = new FunctionDesc("sum", Sum.class,
        Function.Type.GENERAL, DataType.INT, new Class[] { EvalNode.class,
            EvalNode.class });

    cat.registerFunction(funcMeta);

    Tuple tuple = new VTuple(3);
    tuple.put("hyunsik", 500, 30);

    QueryBlock block = null;
    EvalNode expr = null;

    block = NQLCompiler.parse(QUERIES[0]);
    expr = NQLCompiler.evalExprTreeBin(block.getWhereCond(), cat);
    assertEquals(true, expr.eval(tuple).asBool());

    block = NQLCompiler.parse(QUERIES[1]);
    expr = NQLCompiler.evalExprTreeBin(block.getWhereCond(), cat);
    assertEquals(15000, expr.eval(tuple).asInt());

    block = NQLCompiler.parse(QUERIES[2]);
    expr = NQLCompiler.evalExprTreeBin(block.getWhereCond(), cat);
    assertEquals(15050, expr.eval(tuple).asInt());
  }

  @Test
  public void testTupleEval() {
    ConstEval e1 = new ConstEval(DatumFactory.create(1));
    FieldEval e2 = new FieldEval(DataType.INT, 1, 4, "field1"); // it indicates
                                                                // 4th field.

    BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);
    VTuple tuple = new VTuple(6);
    tuple.put(4, 99); // put 0th field

    // the result of evaluation must be 100.
    assertEquals(expr.eval(tuple).asInt(), 100);
  }

  public static class MockTrueEval extends EvalNode {

    public MockTrueEval() {
      super(Type.CONST);
    }

    @Override
    public Datum eval(Tuple tuple, Datum... args) {
      return DatumFactory.create(true);
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
    public Datum eval(Tuple tuple, Datum... args) {
      return DatumFactory.create(false);
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
    assertTrue(andExpr.eval(null).asBool());

    andExpr = new BinaryEval(Type.AND, falseExpr, trueExpr);
    assertFalse(andExpr.eval(null).asBool());

    andExpr = new BinaryEval(Type.AND, trueExpr, falseExpr);
    assertFalse(andExpr.eval(null).asBool());

    andExpr = new BinaryEval(Type.AND, falseExpr, falseExpr);
    assertFalse(andExpr.eval(null).asBool());
  }

  @Test
  public void testOrTest() {
    MockTrueEval trueExpr = new MockTrueEval();
    MockFalseExpr falseExpr = new MockFalseExpr();

    BinaryEval orExpr = new BinaryEval(Type.OR, trueExpr, trueExpr);
    assertTrue(orExpr.eval(null).asBool());

    orExpr = new BinaryEval(Type.OR, falseExpr, trueExpr);
    assertTrue(orExpr.eval(null).asBool());

    orExpr = new BinaryEval(Type.OR, trueExpr, falseExpr);
    assertTrue(orExpr.eval(null).asBool());

    orExpr = new BinaryEval(Type.OR, falseExpr, falseExpr);
    assertFalse(orExpr.eval(null).asBool());
  }

  @Test
  public final void testCompOperator() {
    ConstEval e1 = null;
    ConstEval e2 = null;
    BinaryEval expr = null;

    // Constant
    e1 = new ConstEval(DatumFactory.create(9));
    e2 = new ConstEval(DatumFactory.create(34));
    expr = new BinaryEval(Type.LTH, e1, e2);
    assertTrue(expr.eval(null).asBool());
    expr = new BinaryEval(Type.LEQ, e1, e2);
    assertTrue(expr.eval(null).asBool());
    expr = new BinaryEval(Type.LTH, e2, e1);
    assertFalse(expr.eval(null).asBool());
    expr = new BinaryEval(Type.LEQ, e2, e1);
    assertFalse(expr.eval(null).asBool());

    expr = new BinaryEval(Type.GTH, e2, e1);
    assertTrue(expr.eval(null).asBool());
    expr = new BinaryEval(Type.GEQ, e2, e1);
    assertTrue(expr.eval(null).asBool());
    expr = new BinaryEval(Type.GTH, e1, e2);
    assertFalse(expr.eval(null).asBool());
    expr = new BinaryEval(Type.GEQ, e1, e2);
    assertFalse(expr.eval(null).asBool());

    BinaryEval plus = new BinaryEval(Type.PLUS, e1, e2);
    expr = new BinaryEval(Type.LTH, e1, plus);
    assertTrue(expr.eval(null).asBool());
    expr = new BinaryEval(Type.LEQ, e1, plus);
    assertTrue(expr.eval(null).asBool());
    expr = new BinaryEval(Type.LTH, plus, e1);
    assertFalse(expr.eval(null).asBool());
    expr = new BinaryEval(Type.LEQ, plus, e1);
    assertFalse(expr.eval(null).asBool());

    expr = new BinaryEval(Type.GTH, plus, e1);
    assertTrue(expr.eval(null).asBool());
    expr = new BinaryEval(Type.GEQ, plus, e1);
    assertTrue(expr.eval(null).asBool());
    expr = new BinaryEval(Type.GTH, e1, plus);
    assertFalse(expr.eval(null).asBool());
    expr = new BinaryEval(Type.GEQ, e1, plus);
    assertFalse(expr.eval(null).asBool());
  }

  @Test
  public final void testArithmaticsOperator() {
    ConstEval e1 = null;
    ConstEval e2 = null;

    // PLUS
    e1 = new ConstEval(DatumFactory.create(9));
    e2 = new ConstEval(DatumFactory.create(34));
    BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);
    assertEquals(expr.eval(null).asInt(), 43);

    // MINUS
    e1 = new ConstEval(DatumFactory.create(5));
    e2 = new ConstEval(DatumFactory.create(2));
    expr = new BinaryEval(Type.MINUS, e1, e2);
    assertEquals(expr.eval(null).asInt(), 3);

    // MULTIPLY
    e1 = new ConstEval(DatumFactory.create(5));
    e2 = new ConstEval(DatumFactory.create(2));
    expr = new BinaryEval(Type.MULTIPLY, e1, e2);
    assertEquals(expr.eval(null).asInt(), 10);

    // DIVIDE
    e1 = new ConstEval(DatumFactory.create(10));
    e2 = new ConstEval(DatumFactory.create(5));
    expr = new BinaryEval(Type.DIVIDE, e1, e2);
    assertEquals(expr.eval(null).asInt(), 2);
  }

  @Test
  public final void testGetReturnType() {
    ConstEval e1 = null;
    ConstEval e2 = null;

    // PLUS
    e1 = new ConstEval(DatumFactory.create(9));
    e2 = new ConstEval(DatumFactory.create(34));
    BinaryEval expr = new BinaryEval(Type.PLUS, e1, e2);
    assertEquals(DataType.INT, expr.getValueType());

    expr = new BinaryEval(Type.LTH, e1, e2);
    assertTrue(expr.eval(null).asBool());
    assertEquals(DataType.BOOLEAN, expr.getValueType());

    e1 = new ConstEval(DatumFactory.create(9.3));
    e2 = new ConstEval(DatumFactory.create(34.2));
    expr = new BinaryEval(Type.PLUS, e1, e2);
    assertEquals(DataType.DOUBLE, expr.getValueType());
  }
}
