package nta.engine.exec.eval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import nta.catalog.CatalogService;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.exec.eval.TestEvalTree.TestAggSum;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Hyunsik Choi
 */
public class TestEvalTreeUtil {
  static CatalogService catalog = null;
  static EvalNode expr1;
  static EvalNode expr2;
  static EvalNode expr3;

  @BeforeClass
  public static void setUp() throws Exception {
    NtaTestingUtility util = new NtaTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();

    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("age", DataType.INT);

    TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);
    TableDesc desc = new TableDescImpl("people", meta);
    desc.setPath(new Path("file:///"));
    catalog.addTable(desc);

    FunctionDesc funcMeta = new FunctionDesc("sum", TestSum.class,
        FunctionType.GENERAL, DataType.INT, 
        new DataType [] { DataType.INT, DataType.INT});
    catalog.registerFunction(funcMeta);
    
    funcMeta = new FunctionDesc("aggsum", TestAggSum.class,
        FunctionType.AGGREGATION, DataType.INT, 
        new DataType [] { DataType.INT});
    catalog.registerFunction(funcMeta);
    
    QueryContext.Factory factory = new QueryContext.Factory(catalog);
    QueryAnalyzer analyzer = new QueryAnalyzer(catalog);
    
    QueryBlock block = null;    

    QueryContext ctx = factory.create();
    block = analyzer.parse(ctx, TestEvalTree.QUERIES[0]);
    expr1 = block.getWhereCondition();

    block = analyzer.parse(ctx, TestEvalTree.QUERIES[1]);
    expr2 = block.getWhereCondition();
    
    block = analyzer.parse(ctx, TestEvalTree.QUERIES[2]);
    expr3 = block.getWhereCondition();
  }

  @AfterClass
  public static void tearDown() throws Exception {

  }

  @Test
  public final void testChangeColumnRef() throws CloneNotSupportedException {
    EvalNode copy = (EvalNode)expr1.clone();
    EvalTreeUtil.changeColumnRef(copy, "people.score", "newscore");
    Set<String> set = EvalTreeUtil.findAllRefColumns(copy);
    assertEquals(1, set.size());
    assertTrue(set.contains("newscore"));
    
    copy = (EvalNode)expr2.clone();
    EvalTreeUtil.changeColumnRef(copy, "people.age", "sum_age");
    set = EvalTreeUtil.findAllRefColumns(copy);
    assertEquals(2, set.size());
    assertTrue(set.contains("people.score"));
    assertTrue(set.contains("sum_age"));
    
    copy = (EvalNode)expr3.clone();
    EvalTreeUtil.changeColumnRef(copy, "people.age", "sum_age");
    set = EvalTreeUtil.findAllRefColumns(copy);
    assertEquals(2, set.size());
    assertTrue(set.contains("people.score"));
    assertTrue(set.contains("sum_age"));
  }

  @Test
  public final void testFindAllRefColumns() {    
    Set<String> set = EvalTreeUtil.findAllRefColumns(expr1);
    assertEquals(1, set.size());
    assertTrue(set.contains("people.score"));
    
    set = EvalTreeUtil.findAllRefColumns(expr2);
    assertEquals(2, set.size());
    assertTrue(set.contains("people.score"));
    assertTrue(set.contains("people.age"));
    
    set = EvalTreeUtil.findAllRefColumns(expr3);
    assertEquals(2, set.size());
    assertTrue(set.contains("people.score"));
    assertTrue(set.contains("people.age"));
  }
}