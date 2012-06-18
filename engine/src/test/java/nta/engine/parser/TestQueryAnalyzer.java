package nta.engine.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

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
import nta.catalog.proto.CatalogProtos.IndexMethod;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.Context;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.EvalNode.Type;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.parser.QueryBlock.GroupElement;
import nta.engine.parser.QueryBlock.GroupType;
import nta.engine.parser.QueryBlock.JoinClause;
import nta.engine.parser.QueryBlock.SortSpec;
import nta.engine.planner.JoinType;
import nta.engine.query.exception.InvalidQueryException;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.benchmark.TPCH;

/**
 * This unit test examines the correctness of QueryAnalyzer that analyzes 
 * an abstract syntax tree built from Antlr and generates a QueryBlock instance.
 * 
 * @author Hyunsik Choi
 * 
 * @see QueryAnalyzer
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
    
    Schema schema3 = new Schema();
    schema3.addColumn("id", DataType.INT);
    schema3.addColumn("people_id", DataType.INT);
    schema3.addColumn("class", DataType.STRING);
    schema3.addColumn("branch_name", DataType.STRING);

    Schema schema4 = new Schema();
    schema4.addColumn("char_col", DataType.CHAR);
    schema4.addColumn("short_col", DataType.SHORT);
    schema4.addColumn("int_col", DataType.INT);
    schema4.addColumn("long_col", DataType.LONG);
    schema4.addColumn("float_col", DataType.FLOAT);
    schema4.addColumn("double_col", DataType.DOUBLE);
    schema4.addColumn("string_col", DataType.STRING);

    TableMeta meta = TCatUtil.newTableMeta(schema1, StoreType.CSV);
    TableDesc people = new TableDescImpl("people", meta, new Path("file:///"));
    cat.addTable(people);
    
    TableDesc student = TCatUtil.newTableDesc("student", schema2, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    cat.addTable(student);
    
    TableDesc branch = TCatUtil.newTableDesc("branch", schema3, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    cat.addTable(branch);

    TableDesc allType = TCatUtil.newTableDesc("alltype", schema4, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    cat.addTable(allType);

    TPCH tpch = new TPCH();
    tpch.loadSchemas();
    Schema lineitemSchema = tpch.getSchema("lineitem");
    Schema partSchema = tpch.getSchema("part");
    TableDesc lineitem = TCatUtil.newTableDesc("lineitem", lineitemSchema, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    TableDesc part = TCatUtil.newTableDesc("part", partSchema, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    cat.addTable(lineitem);
    cat.addTable(part);
    
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
      "select id, name, score, age from people", // 0
      "select name, score, age from people where score > 30", // 1
      "select name, score, age from people where 3 + 5 * 3", // 2
      "select age, sumtest(score) as total from people group by age having sumtest(score) > 30", // 3
      "select p.id, s.id, score, dept from people as p, student as s where p.id = s.id", // 4
      "select name, score from people order by score asc, age desc null first", // 5
      // only expr
      "select 7 + 8", // 6
      // create table test
      "store1 := select name, score from people order by score asc, age desc null first",// 7
      // create table test
      "create table store2 as select name, score from people order by score asc, age desc null first", // 8
      // create index
      "create unique index score_idx on people using hash (score, age desc null first) with ('fillfactor' = 70)", // 9
      // create table def
      "create table table1 (name string, age int, earn long, score float) using csv location '/tmp/data' with ('csv.delimiter'='|')" // 10     
  };
 
  @Test
  public final void testSelectStatement() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[0]);    
    assertEquals(1, block.getFromTables().length);
    assertEquals("people", block.getFromTables()[0].getTableName());
  }
  
  private String[] GROUP_BY = { 
      "select age, sumtest(score) as total from people group by age having sumtest(score) > 30", // 0
      "select name, age, sumtest(score) total from people group by cube (name,age)", // 1
      "select name, age, sumtest(score) total from people group by rollup (name,age)", // 2
      "select id, name, age, sumtest(score) total from people group by id, cube (name), rollup (age)", // 3
      "select id, name, age, sumtest(score) total from people group by ()", // 4
  };
  
  @Test
  public final void testGroupByStatement() {
    Context ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, GROUP_BY[0]);
    assertEquals(StatementType.SELECT, tree.getType());
    QueryBlock block = (QueryBlock) tree;
    assertTrue(block.hasGroupbyClause());
    assertEquals(1, block.getGroupByClause().getGroupSet().size());
    assertEquals("age", block.getGroupByClause().getGroupSet().get(0).getColumns()[0].getColumnName());
    assertTrue(block.hasHavingCond());
    assertEquals(Type.GTH, block.getHavingCond().getType());
  }
  
  @Test
  public final void testCubeByStatement() {
    Context ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, GROUP_BY[1]);
    assertEquals(StatementType.SELECT, tree.getType());
    QueryBlock block = (QueryBlock) tree;
    assertTrue(block.hasGroupbyClause());
    assertEquals(1, block.getGroupByClause().getGroupSet().size());
    assertEquals(GroupType.CUBE, block.getGroupByClause().
        getGroupSet().get(0).getType());
    List<GroupElement> groups = block.getGroupByClause().getGroupSet();
    assertEquals("people.name", groups.get(0).getColumns()[0].getQualifiedName());
    assertEquals("people.age", groups.get(0).getColumns()[1].getQualifiedName());
  }
  
  @Test
  public final void testRollUpStatement() {
    Context ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, GROUP_BY[2]);
    assertEquals(StatementType.SELECT, tree.getType());
    QueryBlock block = (QueryBlock) tree;
    assertTrue(block.hasGroupbyClause());
    assertEquals(1, block.getGroupByClause().getGroupSet().size());
    assertEquals(GroupType.ROLLUP, block.getGroupByClause().
        getGroupSet().get(0).getType());
    List<GroupElement> groups = block.getGroupByClause().getGroupSet();
    assertEquals("people.name", groups.get(0).getColumns()[0].getQualifiedName());
    assertEquals("people.age", groups.get(0).getColumns()[1].getQualifiedName());
  }
  
  @Test
  public final void testMixedGroupByStatement() {
    Context ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, GROUP_BY[3]);
    assertEquals(StatementType.SELECT, tree.getType());
    QueryBlock block = (QueryBlock) tree;
    assertTrue(block.hasGroupbyClause());
    assertEquals(3, block.getGroupByClause().getGroupSet().size());
    Iterator<GroupElement> it = block.getGroupByClause().getGroupSet().iterator();
    GroupElement group = it.next();
    assertEquals(GroupType.CUBE, group.getType());    
    assertEquals("people.name", group.getColumns()[0].getQualifiedName());
    group = it.next();
    assertEquals(GroupType.ROLLUP, group.getType());    
    assertEquals("people.age", group.getColumns()[0].getQualifiedName());
    group = it.next();
    assertEquals(GroupType.GROUPBY, group.getType());
    assertEquals("people.id", group.getColumns()[0].getQualifiedName());
  }
  
  @Test
  public final void testEmptyGroupSetStatement() {
    Context ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, GROUP_BY[4]);
    assertEquals(StatementType.SELECT, tree.getType());
    QueryBlock block = (QueryBlock) tree;
    assertTrue(block.hasGroupbyClause());
    assertTrue(block.getGroupByClause().isEmptyGroupSet());
  }
  
  @Test
  public final void testSelectStatementWithAlias() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[4]);
    assertEquals(2, block.getFromTables().length);
    assertEquals("people", block.getFromTables()[0].getTableName());
    assertEquals("student", block.getFromTables()[1].getTableName());
  }
  
  @Test
  public final void testOrderByClause() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[5]);
    testOrderByCluse(block);
  }
  
  private static void testOrderByCluse(QueryBlock block) {
    assertEquals(2, block.getSortKeys().length);
    assertEquals("people.score", block.getSortKeys()[0].getSortKey().getQualifiedName());
    assertEquals(true, block.getSortKeys()[0].isAscending());
    assertEquals(false, block.getSortKeys()[0].isNullFirst());
    assertEquals("people.age", block.getSortKeys()[1].getSortKey().getQualifiedName());
    assertEquals(false, block.getSortKeys()[1].isAscending());
    assertEquals(true, block.getSortKeys()[1].isNullFirst());
  }
  
  @Test
  public final void testCreateTableAsSelect() {
    Context ctx = factory.create();
    CreateTableStmt stmt = (CreateTableStmt) analyzer.parse(ctx, QUERIES[7]);
    assertEquals("store1", stmt.getTableName());
    testOrderByCluse(stmt.getSelectStmt());
    
    ctx = factory.create();
    stmt = (CreateTableStmt) analyzer.parse(ctx, QUERIES[8]);
    assertEquals("store2", stmt.getTableName());
    testOrderByCluse(stmt.getSelectStmt());
  }
  
  @Test
  public final void testCreateTableDef() {
    Context ctx = factory.create();
    CreateTableStmt stmt = (CreateTableStmt) analyzer.parse(ctx, QUERIES[10]);
    assertEquals("table1", stmt.getTableName());
    Schema def = stmt.getSchema();
    assertEquals("name", def.getColumn(0).getColumnName());
    assertEquals(DataType.STRING, def.getColumn(0).getDataType());
    assertEquals("age", def.getColumn(1).getColumnName());
    assertEquals(DataType.INT, def.getColumn(1).getDataType());
    assertEquals("earn", def.getColumn(2).getColumnName());
    assertEquals(DataType.LONG, def.getColumn(2).getDataType());
    assertEquals("score", def.getColumn(3).getColumnName());
    assertEquals(DataType.FLOAT, def.getColumn(3).getDataType());    
    assertEquals(StoreType.CSV, stmt.getStoreType());    
    assertEquals("/tmp/data", stmt.getPath().toString());
    assertTrue(stmt.hasOptions());
    assertEquals("|", stmt.getOptions().get("csv.delimiter"));
  }
  
  @Test 
  public final void testCreateIndex() {
    Context ctx = factory.create();
    CreateIndexStmt stmt = (CreateIndexStmt) analyzer.parse(ctx, QUERIES[9]);
    assertEquals("score_idx", stmt.getIndexName());
    assertTrue(stmt.isUnique());
    assertEquals("people", stmt.getTableName());
    assertEquals(IndexMethod.HASH, stmt.getMethod());
    
    SortSpec [] sortKeys = stmt.getSortSpecs();
    assertEquals(2, sortKeys.length);
    assertEquals("score", sortKeys[0].getSortKey().getColumnName());
    assertEquals(DataType.INT, sortKeys[0].getSortKey().getDataType());
    assertEquals("age", sortKeys[1].getSortKey().getColumnName());
    assertEquals(DataType.INT, sortKeys[1].getSortKey().getDataType());
    assertEquals(false, sortKeys[1].isAscending());
    assertEquals(true, sortKeys[1].isNullFirst());
    
    assertTrue(stmt.hasParams());
    assertEquals("70", stmt.getParams().get("fillfactor"));
  }
  
  @Test
  public final void testOnlyExpr() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, QUERIES[6]);
    EvalNode node = block.getTargetList()[0].getEvalTree();
    assertEquals(Type.PLUS, node.getType());
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
  
  @Test(expected = InvalidQueryException.class)
  public final void testInvalidGroupFields() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, INVALID_QUERIES[2]);
    assertEquals("age", block.getGroupByClause().getGroupSet().get(0).getColumns()[0].getQualifiedName());
  }
  
  static String [] JOINS = {
    "select p.id, name, branch_name from people as p natural join student natural join branch", // 0
    "select name, dept from people as p inner join student as s on p.id = s.people_id", // 1
    "select name, dept from people as p inner join student as s using (p.id)", // 2
    "select p.id, name, branch_name from people as p cross join student cross join branch", // 3
    "select p.id, dept from people as p left outer join student as s on p.id = s.people_id", // 4
    "select p.id, dept from people as p right outer join student as s on p.id = s.people_id", // 5
    "select p.id, dept from people as p join student as s on p.id = s.people_id", // 6
    "select p.id, dept from people as p left join student as s on p.id = s.people_id", // 7
    "select p.id, dept from people as p right join student as s on p.id= s.people_id" // 8
  };
  
  @Test
  public final void testNaturalJoinClause() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, JOINS[0]);
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.NATURAL, join.getJoinType());
    assertEquals("people", join.getLeft().getTableName());
    assertEquals("p", join.getLeft().getAlias());    
    assertTrue(join.hasRightJoin());

    assertEquals("student", join.getRightJoin().getLeft().getTableName());
    assertEquals("branch", join.getRightJoin().getRight().getTableName());
  }
  
  @Test
  public final void testInnerJoinClause() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, JOINS[1]);
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals("people", join.getLeft().getTableName());
    assertEquals("p", join.getLeft().getAlias());
    assertEquals("student", join.getRight().getTableName());
    assertEquals("s", join.getRight().getAlias());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalNode.Type.EQUAL, join.getJoinQual().getType());
    
    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, JOINS[2]);
    join = block.getJoinClause();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals("people", join.getLeft().getTableName());
    assertEquals("p", join.getLeft().getAlias());
    assertEquals("student", join.getRight().getTableName());
    assertEquals("s", join.getRight().getAlias());
    assertTrue(join.hasJoinColumns());
    assertEquals("id", join.getJoinColumns()[0].getColumnName());
  }
  
  @Test
  public final void testJoinClause() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, JOINS[6]);
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals("people", join.getLeft().getTableName());
    assertEquals("p", join.getLeft().getAlias());
    assertEquals("student", join.getRight().getTableName());
    assertEquals("s", join.getRight().getAlias());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalNode.Type.EQUAL, join.getJoinQual().getType());
  }
  
  @Test
  public final void testCrossJoinClause() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, JOINS[3]);
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.CROSS_JOIN, join.getJoinType());
    assertEquals("people", join.getLeft().getTableName());
    assertEquals("p", join.getLeft().getAlias());    
    assertTrue(join.hasRightJoin());

    assertEquals("student", join.getRightJoin().getLeft().getTableName());
    assertEquals("branch", join.getRightJoin().getRight().getTableName());
  }
  
  @Test
  public final void testLeftOuterJoinClause() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, JOINS[4]);
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.LEFT_OUTER, join.getJoinType());
    assertEquals("people", join.getLeft().getTableName());
    assertEquals("p", join.getLeft().getAlias());
    assertEquals("student", join.getRight().getTableName());
    assertEquals("s", join.getRight().getAlias());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalNode.Type.EQUAL, join.getJoinQual().getType());
  }
  
  @Test
  public final void testLeftJoinClause() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, JOINS[7]);
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.LEFT_OUTER, join.getJoinType());
    assertEquals("people", join.getLeft().getTableName());
    assertEquals("p", join.getLeft().getAlias());
    assertEquals("student", join.getRight().getTableName());
    assertEquals("s", join.getRight().getAlias());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalNode.Type.EQUAL, join.getJoinQual().getType());
  }
  
  @Test
  public final void testRightOuterJoinClause() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, JOINS[5]);
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.RIGHT_OUTER, join.getJoinType());
    assertEquals("people", join.getLeft().getTableName());
    assertEquals("p", join.getLeft().getAlias());
    assertEquals("student", join.getRight().getTableName());
    assertEquals("s", join.getRight().getAlias());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalNode.Type.EQUAL, join.getJoinQual().getType());
  }
  
  @Test
  public final void testRightJoinClause() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, JOINS[8]);
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.RIGHT_OUTER, join.getJoinType());
    assertEquals("people", join.getLeft().getTableName());
    assertEquals("p", join.getLeft().getAlias());
    assertEquals("student", join.getRight().getTableName());
    assertEquals("s", join.getRight().getAlias());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalNode.Type.EQUAL, join.getJoinQual().getType());
  }
  
  private final String [] setClauses = {
      "select id, people_id from student union select id, people_id from branch",
      "select id, people_id from student union select id, people_id from branch intersect select id, people_id from branch as b"
  };
  
  @Test
  public final void testUnionClause() {
    Context ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, setClauses[0]);
    assertEquals(StatementType.UNION, tree.getType());
    SetStmt union = (SetStmt) tree;
    assertEquals(StatementType.SELECT, union.getLeftTree().getType());
    assertEquals(StatementType.SELECT, union.getRightTree().getType());
    QueryBlock left = (QueryBlock) union.getLeftTree();
    assertEquals("student", left.getFromTables()[0].getTableName());
    QueryBlock right = (QueryBlock) union.getRightTree();
    assertEquals("branch", right.getFromTables()[0].getTableName());
    
    // multiple set statements
    ctx = factory.create();
    tree = analyzer.parse(ctx, setClauses[1]);
    assertEquals(StatementType.UNION, tree.getType());
    union = (SetStmt) tree;
    assertEquals(StatementType.SELECT, union.getLeftTree().getType());
    assertEquals(StatementType.INTERSECT, union.getRightTree().getType());
    left = (QueryBlock) union.getLeftTree();
    assertEquals("student", left.getFromTables()[0].getTableName());
    SetStmt rightSet = (SetStmt) union.getRightTree();
    left = (QueryBlock) rightSet.getLeftTree();
    assertEquals("branch", left.getFromTables()[0].getTableName());
    right = (QueryBlock) rightSet.getRightTree();
    assertEquals("b", right.getFromTables()[0].getAlias());
  }

  static final String [] setQualifier = {
      "select id, people_id from student",
      "select distinct id, people_id from student",
      "select all id, people_id from student",
  };

  @Test
  public final void testSetQulaifier() {
    Context ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx, setQualifier[0]);
    assertEquals(StatementType.SELECT, tree.getType());
    QueryBlock block = (QueryBlock) tree;
    assertFalse(block.isDistinct());

    ctx = factory.create();
    tree = analyzer.parse(ctx, setQualifier[1]);
    assertEquals(StatementType.SELECT, tree.getType());
    block = (QueryBlock) tree;
    assertTrue(block.isDistinct());

    ctx = factory.create();
    tree = analyzer.parse(ctx, setQualifier[2]);
    assertEquals(StatementType.SELECT, tree.getType());
    block = (QueryBlock) tree;
    assertFalse(block.isDistinct());
  }

  @Test
  public final void testTypeInferring() {
    Context ctx = factory.create();
    QueryBlock block = (QueryBlock) analyzer.parse(ctx, "select 1 from alltype where char_col = 'a'");
    assertEquals(DataType.CHAR, block.getWhereCondition().getRightExpr().getValueType());

    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, "select 1 from alltype where short_col = 1");
    assertEquals(DataType.SHORT, block.getWhereCondition().getRightExpr().getValueType());

    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, "select 1 from alltype where int_col = 1");
    assertEquals(DataType.INT, block.getWhereCondition().getRightExpr().getValueType());

    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, "select 1 from alltype where long_col = 1");
    assertEquals(DataType.LONG, block.getWhereCondition().getRightExpr().getValueType());

    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, "select 1 from alltype where float_col = 1");
    assertEquals(DataType.INT, block.getWhereCondition().getRightExpr().getValueType());

    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, "select 1 from alltype where float_col = 1.0");
    assertEquals(DataType.FLOAT, block.getWhereCondition().getRightExpr().getValueType());

    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, "select 1 from alltype where int_col = 1.0");
    assertEquals(DataType.DOUBLE, block.getWhereCondition().getRightExpr().getValueType());

    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, "select 1 from alltype where double_col = 1.0");
    assertEquals(DataType.DOUBLE, block.getWhereCondition().getRightExpr().getValueType());

    ctx = factory.create();
    block = (QueryBlock) analyzer.parse(ctx, "select 1 from alltype where string_col = 'a'");
    assertEquals(DataType.STRING, block.getWhereCondition().getRightExpr().getValueType());
  }

  @Test
  public void testCaseWhen() {
    Context ctx = factory.create();
    ParseTree tree = analyzer.parse(ctx,
        "select case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) "+
        "when p_type = 'MOCC' then l_extendedprice - 100 else 0 end as cond from lineitem, part");
    assertEquals(StatementType.SELECT, tree.getType());
    QueryBlock block = (QueryBlock) tree;
    assertTrue(block.getTargetList()[0].hasAlias());
    assertEquals("cond", block.getTargetList()[0].getAlias());
    assertEquals(DataType.DOUBLE, block.getTargetList()[0].getEvalTree().getValueType());
  }
}