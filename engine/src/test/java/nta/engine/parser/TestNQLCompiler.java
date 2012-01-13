package nta.engine.parser;

import static org.junit.Assert.assertEquals;

import java.net.URI;

import nta.catalog.Catalog;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.conf.NtaConf;
import nta.engine.exception.InternalException;
import nta.engine.exception.NQLSyntaxException;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.eval.EvalNode;
import nta.engine.parser.NQL.Query;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 이 테스트는 NQLCompiler라 정확한 QueryBlock을 생성해 내는지 테스트한다.
 * 
 * @author Hyunsik Choi
 * 
 * @see QueryBlock
 */
public class TestNQLCompiler {

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {
  }

  private String[] QUERIES = { 
      "select id, name, age, gender from people",
      "select name, score, age from people where score > 30",
      "select name, score, age from people where 3 + 5 * 3", 
      "select age, sum(score) as total from test group by age having sum(score) > 30" // 3
  };

  private String[] EXPRS = { "3 + 5 * 3" };

  // It's for benchmark of the evaluation tree methods.
  public final void testLegacyEvalTree() throws RecognitionException, 
      NTAQueryException {
    NQLParser p = parseExpr(EXPRS[0]);
    CommonTree node = (CommonTree) p.search_condition().getTree();

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("age", DataType.INT);

    TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta);
    desc.setURI(URI.create("file:///"));
    Catalog cat = new Catalog(new NtaConf());
    cat.addTable(desc);

    NQL nql = new NQL(cat);
    Query q = nql.parse(QUERIES[2]);
    EvalNode expr = nql.buildExpr(q, node);

    Tuple tuples[] = new Tuple[1000000];
    for (int i = 0; i < 1000000; i++) {
      tuples[i] = new VTuple(3);
      tuples[i].put("hyunsik_" + i, i + 500, i);
    }

    long start = System.currentTimeMillis();
    for (int i = 0; i < tuples.length; i++) {
      expr.eval(tuples[i]);
    }
    long end = System.currentTimeMillis();

    System.out.println("legacy elapsed time: " + (end - start));
  }

  public static NQLParser parseExpr(final String expr) {
    ANTLRStringStream input = new ANTLRStringStream(expr);
    NQLLexer lexer = new NQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    NQLParser parser = new NQLParser(tokens);
    return parser;
  }

  public final void testNewEvalTree() throws NQLSyntaxException, InternalException {
    QueryBlock block = NQLCompiler.parse(QUERIES[0]);

    assertEquals(1, block.getNumFromTables());

    block = NQLCompiler.parse(QUERIES[2]);

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("age", DataType.INT);

    Tuple tuples[] = new Tuple[1000000];
    for (int i = 0; i < 1000000; i++) {
      tuples[i] = new VTuple(3);
      tuples[i].put("hyunsik_" + i, i + 500, i);
    }

    TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta);
    desc.setURI(URI.create("file:///"));
    Catalog cat = new Catalog(new NtaConf());
    cat.addTable(desc);

    EvalNode expr = NQLCompiler.evalExprTreeBin(block.getWhereCond(), cat);

    long start = System.currentTimeMillis();
    for (int i = 0; i < tuples.length; i++) {
      expr.eval(tuples[i]);
    }
    long end = System.currentTimeMillis();

    System.out.println("elapsed time: " + (end - start));
  }

  @Test
  public final void testEvalExprTreeBin() throws NQLSyntaxException, InternalException {
    QueryBlock block = NQLCompiler.parse(QUERIES[0]);

    assertEquals(1, block.getNumFromTables());

    block = NQLCompiler.parse(QUERIES[2]);

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("age", DataType.INT);

    Tuple tuple = new VTuple(3);
    tuple.put("hyunsik", 500, 30);

    TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta);
    desc.setURI(URI.create("file:///"));
    Catalog cat = new Catalog(new NtaConf());
    cat.addTable(desc);

    EvalNode expr = NQLCompiler.evalExprTreeBin(block.getWhereCond(), cat);

    assertEquals(18, expr.eval(tuple).asInt());
  }
  
  @Test
  public final void testGroupByClause() throws NQLSyntaxException {
    QueryBlock block = NQLCompiler.parse(QUERIES[3]);
    assertEquals("age", block.getGroupFields()[0]);    
    assertEquals("total", block.getTargetList()[1].getAlias());
  }
}
