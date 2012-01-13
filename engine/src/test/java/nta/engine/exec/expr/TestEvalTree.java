package nta.engine.exec.expr;

import static org.junit.Assert.*;

import java.net.URI;

import nta.catalog.Catalog;
import nta.catalog.Column;
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
import nta.datum.DatumType;
import nta.engine.exception.InternalException;
import nta.engine.exception.NQLSyntaxException;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.EvalNode.Type;
import nta.engine.function.Function;
import nta.engine.parser.NQLCompiler;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.NQL.Query;
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
      super(
          new ColumnBase [] {
              new ColumnBase("arg1", DataType.INT),
              new ColumnBase("arg2", DataType.INT)
          });
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
  public final void testEvalExprTreeBin() throws NQLSyntaxException, InternalException {

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("age", DataType.INT);
    
    TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta);
    desc.setURI(URI.create("file:///"));
    Catalog cat = new Catalog(new NtaConf());
    cat.addTable(desc);
    
    FunctionDesc funcMeta = new FunctionDesc("sum", Sum.class, Function.Type.GENERAL,
        DataType.INT, new Class [] { EvalNode.class, EvalNode.class}); 
    
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
  public void testbuildExpr() throws NTAQueryException {
//    QueryBlock block = null;
//    block = NQLCompiler.parse(QUERIES[4]);
//    
//    assertEquals(stmt.getTargetList().length,1);
//    EvalNode expr = stmt.getTargetList()[0].expr;
//    assertEquals(expr.getType(), Type.PLUS);
//    assertEquals(expr.getLeftExpr().getType(), Type.CONST);
//    assertEquals(expr.getLeftExpr().eval(null).type(), DatumType.INT);
//    assertEquals(expr.getLeftExpr().eval(null).asInt(), 2);
//    assertEquals(expr.getRightExpr().getType(), Type.CONST);
//    assertEquals(expr.getRightExpr().eval(null).type(), DatumType.INT);
//    assertEquals(expr.getRightExpr().eval(null).asInt(), 3);    
  }
}
