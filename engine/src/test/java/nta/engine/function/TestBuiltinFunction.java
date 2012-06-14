package nta.engine.function;

import static org.junit.Assert.assertEquals;
import nta.catalog.CatalogService;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.exec.eval.EvalContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class tests the builtin functions (avg, count, min, max, and sum).
 * 
 * @author Hyunsik Choi
 *
 */
public class TestBuiltinFunction {
  private static NtaTestingUtility util;
  private static CatalogService cat;
  private static Schema schema;

  private static QueryContext.Factory factory;
  private static QueryAnalyzer analyzer;

  private static final int tuplenum = 10;
  private static Tuple [] tuples;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    cat = util.getMiniCatalogCluster().getCatalog();

    schema = new Schema();
    schema.addColumn("people.name", DataType.STRING);
    schema.addColumn("people.score", DataType.INT);
    schema.addColumn("people.age", DataType.INT);

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta, new Path("file:///"));
    cat.addTable(desc);

    tuples = new Tuple[tuplenum];
    for (int i = 0; i < tuplenum - 3; i++) {
      tuples[i] = new VTuple(3);
      tuples[i].put(0, DatumFactory.createString("hyunsik"));
      tuples[i].put(1, DatumFactory.createInt(i + 1));
      tuples[i].put(2, DatumFactory.createInt(30));
    }

    for (int i = 7; i < 9; i++) {
      tuples[i] = new VTuple(3);
      tuples[i].put(0, DatumFactory.createString("nullval"));
      tuples[i].put(1, DatumFactory.createNullDatum());
      tuples[i].put(2, DatumFactory.createNullDatum());
    }

    tuples[9] = new VTuple(3);
    tuples[9].put(0, DatumFactory.createString("specificval"));
    tuples[9].put(1, DatumFactory.createInt(99));
    tuples[9].put(2, DatumFactory.createNullDatum());

    factory = new QueryContext.Factory(cat);
    analyzer = new QueryAnalyzer(cat);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }

  private String[] QUERIES = { "select count(*) from people", // 0
      "select count(name), count(score), count(age) from people", // 1
  };

  @Test
  public final void testCountRows() {
    QueryBlock block;
    EvalNode expr;
    QueryContext ctx = factory.create();

    block = (QueryBlock) analyzer.parse(ctx, QUERIES[0]);
    expr = block.getTargetList()[0].getEvalTree();
    EvalContext exprCtx = expr.newContext();
    Datum accumulated = DatumFactory.createInt(0);

    int sum = 0;
    for (int i = 0; i < tuplenum; i++) {
      System.out.println(tuples[i]);
      expr.eval(exprCtx, schema, tuples[i], accumulated);
      accumulated = expr.terminate(exprCtx);
      sum += 1;
      assertEquals(sum, accumulated.asInt());
    }
  }

  @Test
  public final void testCountVals() {
    QueryBlock block;
    EvalNode expr1;
    EvalNode expr2;
    EvalNode expr3;
    QueryContext ctx = factory.create();

    block = (QueryBlock) analyzer.parse(ctx, QUERIES[1]);
    expr1 = block.getTargetList()[0].getEvalTree();
    expr2 = block.getTargetList()[1].getEvalTree();
    expr3 = block.getTargetList()[2].getEvalTree();
    Datum accumulated1 = DatumFactory.createInt(0);
    Datum accumulated2 = DatumFactory.createInt(0);
    Datum accumulated3 = DatumFactory.createInt(0);
    EvalContext evalCtx1 = expr1.newContext();
    EvalContext evalCtx2 = expr2.newContext();
    EvalContext evalCtx3 = expr3.newContext();

    for (int i = 0; i < tuplenum; i++) {
      expr1.eval(evalCtx1, schema, tuples[i], accumulated1);
      expr2.eval(evalCtx2, schema, tuples[i], accumulated2);
      expr3.eval(evalCtx3, schema, tuples[i], accumulated3);
      accumulated1 = expr1.terminate(evalCtx1);
      accumulated2 = expr2.terminate(evalCtx2);
      accumulated3 = expr3.terminate(evalCtx3);
    }

    assertEquals(10, accumulated1.asLong());
    assertEquals(8, accumulated2.asLong());
    assertEquals(7, accumulated3.asLong());
  }
}
