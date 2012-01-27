package nta.engine.parser;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

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
import nta.datum.DatumFactory;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 이 테스트는 QueryAnalyzer라 정확한 QueryBlock을 생성해 내는지 테스트한다.
 * 
 * @author Hyunsik Choi
 * 
 * @see QueryBlock
 */
public class TestQueryAnalyzer {
  private CatalogServer cat = null;
  private Schema schema1 = null;
  
  @Before
  public void setUp() throws Exception {
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

    TableMeta meta = new TableMetaImpl(schema1, StoreType.CSV);
    TableDesc people = new TableDescImpl("people", meta);
    people.setPath(new Path("file:///"));
    cat = new CatalogServer(new NtaConf());
    cat.addTable(people);
    
    TableDesc student = new TableDescImpl("student", schema2, StoreType.CSV);
    student.setPath(new Path("file:///"));
    cat.addTable(student);
    
    FunctionDesc funcMeta = new FunctionDesc("sum", TestSum.class,
        FunctionType.GENERAL, DataType.INT, 
        new DataType [] {DataType.INT});

    cat.registerFunction(funcMeta);
  }

  @After
  public void tearDown() throws Exception {
  }

  private String[] QUERIES = { 
      "select id, name, score, age from people",
      "select name, score, age from people where score > 30",
      "select name, score, age from people where 3 + 5 * 3", 
      "select age, sum(score) as total from people group by age having sum(score) > 30", // 3
      "select p.id, s.id, score, dept from people as p, student as s where p.id = s.id", // 4
      "select name, score from people order by score asc, age desc" // 5
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

    TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta);
    desc.setPath(new Path("file:///"));
    CatalogServer cat = new CatalogServer(new NtaConf());
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

    System.out.println("legacy elapsed time: " + (end - start));
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
    
    QueryBlock block = QueryAnalyzer.parse(QUERIES[0], cat);

    assertEquals(1, block.getNumFromTables());

    block = QueryAnalyzer.parse(QUERIES[2], cat);
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
    QueryBlock block = QueryAnalyzer.parse(QUERIES[0], cat);
    
    assertEquals(1, block.getFromTables().length);
    assertEquals("people", block.getFromTables()[0].getTableId());
    
    block = QueryAnalyzer.parse(QUERIES[3], cat);
    
    // TODO - to be more regressive
  }
  
  @Test
  public final void testSelectStatementWithAlias() {
    QueryBlock block = QueryAnalyzer.parse(QUERIES[4], cat);
    assertEquals(2, block.getFromTables().length);
    assertEquals("people", block.getFromTables()[0].getTableId());
    assertEquals("student", block.getFromTables()[1].getTableId());
  }
  
  @Test
  public final void testOrderByClause() {
    QueryBlock block = QueryAnalyzer.parse(QUERIES[5], cat);
    assertEquals(2, block.getSortKeys().length);
    assertEquals("people.score", block.getSortKeys()[0].getSortKey().getName());
    assertEquals(true, block.getSortKeys()[0].isAscending());    
    assertEquals("people.age", block.getSortKeys()[1].getSortKey().getName());
    assertEquals(false, block.getSortKeys()[1].isAscending());
  }
  
  private String [] INVALID_QUERIES = {
      "select * from invalid", // 0 - when a given table does not exist
      "select time, age from people", // 1 - when a given column does not exist
      "select age from people group by age2" // 2 - when a grouping field does not eixst
  };
  @Test(expected = InvalidQueryException.class)
  public final void testNoSuchTables()  {
    QueryAnalyzer.parse(INVALID_QUERIES[0], cat);
  }
  
  @Test(expected = InvalidQueryException.class)
  public final void testNoSuchFields()  {
    QueryAnalyzer.parse(INVALID_QUERIES[1], cat);
  }
  
  @Test
  public final void testGroupByClause() {
    QueryBlock block = QueryAnalyzer.parse(QUERIES[3], cat);
    assertEquals("people.age", block.getGroupFields()[0].getName());
  }
  
  @Test(expected = InvalidQueryException.class)
  public final void testInvalidGroupFields() {
    QueryBlock block = QueryAnalyzer.parse(INVALID_QUERIES[2], cat);
    assertEquals("age", block.getGroupFields()[0].getName());
  }
}