package nta.engine.parser;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

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
import nta.datum.DatumFactory;
import nta.engine.Context;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.parser.NQL.Query;
import nta.engine.query.exception.InvalidQueryException;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 이 테스트는 QueryAnalyzer라 정확한 QueryBlock을 생성해 내는지 테스트한다.
 * 
 * @author Hyunsik Choi
 * 
 * @see QueryBlock
 */
public class TestQueryAnalyzer {
  private static NtaTestingUtility util;
  private static CatalogService cat = null;
  private static Schema schema1 = null;
  private static QueryAnalyzer analyzer = null;
  private static QueryContext.Factory factory = null;
  
  @BeforeClass
  public static void setUp() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    cat = util.getMiniCatalogCluster().getCatalog();
    
    schema1 = new Schema();
    schema1.addColumn("id", DataType.INT);
    schema1.addColumn("name", DataType.STRING);
    schema1.addColumn("score", DataType.INT);
    schema1.addColumn("age", DataType.INT);
    
    Schema schema2 = new Schema();
    schema2.addColumn("id", DataType.INT);
    schema2.addColumn("people_id", DataType.INT);
    schema2.addColumn("dept", DataType.STRING);
    schema2.addColumn("year", DataType.INT);

    TableMeta meta = TCatUtil.newTableMeta(schema1, StoreType.CSV);
    TableDesc people = new TableDescImpl("people", meta, new Path("file:///"));
    cat.addTable(people);
    
    TableDesc student = TCatUtil.newTableDesc("student", schema2, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    cat.addTable(student);
    
    FunctionDesc funcMeta = new FunctionDesc("sumtest", TestSum.class,
        FunctionType.GENERAL, DataType.INT, 
        new DataType [] {DataType.INT});

    cat.registerFunction(funcMeta);
    
    analyzer = new QueryAnalyzer(cat);
    factory = new QueryContext.Factory(cat);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }

  private String[] QUERIES = { 
      "select id, name, score, age from people",
      "select name, score, age from people where score > 30",
      "select name, score, age from people where 3 + 5 * 3", 
      "select age, sumtest(score) as total from people group by age having sumtest(score) > 30", // 3
      "select p.id, s.id, score, dept from people as p, student as s where p.id = s.id", // 4
      "select name, score from people order by score asc, age desc", // 5
      "store1 := select name, score from people order by score asc, age desc",// 6
  };

  private String[] EXPRS = { "3 + 5 * 3" };

  // It's for benchmark of the evaluation tree methods.
  public final void testLegacyEvalTree() throws RecognitionException, 
      IOException {
    NQLParser p = parseExpr(EXPRS[0]);
    CommonTree node = (CommonTree) p.search_condition().getTree();

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("age", DataType.INT);

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta, new Path("file:///"));
    cat.addTable(desc);

    NQL nql = new NQL(cat);
    Query q = nql.parse(QUERIES[2]);
    EvalNode expr = nql.buildExpr(q, node);

    Tuple tuples[] = new Tuple[1000000];
    for (int i = 0; i < 1000000; i++) {
      tuples[i] = new VTuple(3);
      tuples[i].put(
          DatumFactory.createString("hyunsik_" + i), 
          DatumFactory.createInt(i + 500),
          DatumFactory.createInt(i));
    }

    long start = System.currentTimeMillis();
    for (int i = 0; i < tuples.length; i++) {
      expr.eval(schema, tuples[i]);
    }
    long end = System.currentTimeMillis();
  }

  public static NQLParser parseExpr(final String expr) {
    ANTLRStringStream input = new ANTLRStringStream(expr);
    NQLLexer lexer = new NQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    NQLParser parser = new NQLParser(tokens);
    return parser;
  }

  public final void testNewEvalTree() {    
    Tuple tuples[] = new Tuple[1000000];
    for (int i = 0; i < 1000000; i++) {
      tuples[i] = new VTuple(4);
      tuples[i].put(
          DatumFactory.createInt(i),
          DatumFactory.createString("hyunsik_" + i),
          DatumFactory.createInt(i + 500),
          DatumFactory.createInt(i));
    }
    
    Context ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[0]);

    assertEquals(1, block.getNumFromTables());

    ctx = factory.create();
    block = analyzer.parse(ctx, QUERIES[2]);
    EvalNode expr = block.getWhereCondition();

    long start = System.currentTimeMillis();
    for (int i = 0; i < tuples.length; i++) {
      expr.eval(schema1, tuples[i]);
    }
    long end = System.currentTimeMillis();

    System.out.println("elapsed time: " + (end - start));
  }
 
  @Test
  public final void testSelectStatement() {
    Context ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[0]);
    
    assertEquals(1, block.getFromTables().length);
    assertEquals("people", block.getFromTables()[0].getTableId());
    ctx = factory.create();
    block = analyzer.parse(ctx, QUERIES[3]);
    
    // TODO - to be more regressive
  }
  
  @Test
  public final void testSelectStatementWithAlias() {
    Context ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[4]);
    assertEquals(2, block.getFromTables().length);
    assertEquals("people", block.getFromTables()[0].getTableId());
    assertEquals("student", block.getFromTables()[1].getTableId());
  }
  
  @Test
  public final void testOrderByClause() {
    Context ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[5]);
    testOrderByCluse(block);
  }
  
  private static final void testOrderByCluse(QueryBlock block) {
    assertEquals(2, block.getSortKeys().length);
    assertEquals("people.score", block.getSortKeys()[0].getSortKey().getQualifiedName());
    assertEquals(true, block.getSortKeys()[0].isAscending());    
    assertEquals("people.age", block.getSortKeys()[1].getSortKey().getQualifiedName());
    assertEquals(false, block.getSortKeys()[1].isAscending());
  }
  
  @Test
  public final void testStoreTable() {
    Context ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[6]);
    assertEquals("store1", block.getStoreTable());
    testOrderByCluse(block);
  }
  
  private String [] INVALID_QUERIES = {
      "select * from invalid", // 0 - when a given table does not exist
      "select time, age from people", // 1 - when a given column does not exist
      "select age from people group by age2" // 2 - when a grouping field does not eixst
  };
  @Test(expected = InvalidQueryException.class)
  public final void testNoSuchTables()  {
    Context ctx = factory.create();
    analyzer.parse(ctx, INVALID_QUERIES[0]);
  }
  
  @Test(expected = InvalidQueryException.class)
  public final void testNoSuchFields()  {
    Context ctx = factory.create();
    analyzer.parse(ctx, INVALID_QUERIES[1]);
  }
  
  @Test
  public final void testGroupByClause() {
    Context ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, QUERIES[3]);
    assertEquals("people.age", block.getGroupFields()[0].getQualifiedName());
  }
  
  @Test(expected = InvalidQueryException.class)
  public final void testInvalidGroupFields() {
    Context ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx, INVALID_QUERIES[2]);
    assertEquals("age", block.getGroupFields()[0].getQualifiedName());
  }
}