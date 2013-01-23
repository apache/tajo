/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.parser;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.TajoTestingCluster;
import tajo.benchmark.TPCH;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.catalog.proto.CatalogProtos.IndexMethod;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.datum.DatumFactory;
import tajo.engine.eval.ConstEval;
import tajo.engine.eval.EvalNode;
import tajo.engine.eval.EvalNode.Type;
import tajo.engine.eval.TestEvalTree.TestSum;
import tajo.engine.parser.QueryBlock.GroupElement;
import tajo.engine.parser.QueryBlock.GroupType;
import tajo.engine.parser.QueryBlock.JoinClause;
import tajo.engine.planner.JoinType;
import tajo.engine.query.exception.InvalidQueryException;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * This unit test examines the correctness of QueryAnalyzer that analyzes 
 * an abstract syntax tree built from Antlr and generates a QueryBlock instance.
 * 
 * @see QueryAnalyzer
 * @see tajo.engine.parser.QueryBlock
 */
public class TestQueryAnalyzer {
  private static TajoTestingCluster util;
  private static CatalogService cat = null;
  private static Schema schema1 = null;
  private static QueryAnalyzer analyzer = null;
  
  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
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
    
    FunctionDesc funcMeta = new FunctionDesc("sumtest", TestSum.class, FunctionType.GENERAL,
        new DataType [] {DataType.INT},
        new DataType [] {DataType.INT});

    cat.registerFunction(funcMeta);
    
    analyzer = new QueryAnalyzer(cat);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
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
      // limit test
      "select id, name, score, age from people limit 3" // 7

  };
 
  @Test
  public final void testSelectStatement() {
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[0]).getParseTree();
    assertEquals(1, block.getFromTables().length);
    assertEquals("people", block.getFromTables()[0].getTableName());
  }

  @Test
  public final void testOnlyExpr() {
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[6]).getParseTree();
    EvalNode node = block.getTargetList()[0].getEvalTree();
    assertEquals(Type.PLUS, node.getType());
  }

  @Test
  public void testLimit() {
    QueryBlock queryBlock =
        (QueryBlock) analyzer.parse(QUERIES[7]).getParseTree();
    assertEquals(3, queryBlock.getLimitClause().getLimitRow());
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
    ParseTree tree = analyzer.parse(GROUP_BY[0]).getParseTree();
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
    ParseTree tree = analyzer.parse(GROUP_BY[1]).getParseTree();
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
    ParseTree tree = analyzer.parse(GROUP_BY[2]).getParseTree();
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
    ParseTree tree = analyzer.parse(GROUP_BY[3]).getParseTree();
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
    ParseTree tree = analyzer.parse(GROUP_BY[4]).getParseTree();
    assertEquals(StatementType.SELECT, tree.getType());
    QueryBlock block = (QueryBlock) tree;
    assertTrue(block.hasGroupbyClause());
    assertTrue(block.getGroupByClause().isEmptyGroupSet());
  }
  
  @Test
  public final void testSelectStatementWithAlias() {
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[4]).getParseTree();
    assertEquals(2, block.getFromTables().length);
    assertEquals("people", block.getFromTables()[0].getTableName());
    assertEquals("student", block.getFromTables()[1].getTableName());
  }
  
  @Test
  public final void testOrderByClause() {
    QueryBlock block = (QueryBlock) analyzer.parse(QUERIES[5]).getParseTree();
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

  static final String [] createTableStmts = {
    "create table table1 (name string, age int)",
    "create table table1 (name string, age int) using rcfile",
    "create table table1 (name string, age int) using rcfile with ('rcfile.buffer'=4096)",
    // create table test
    "create table store1 as select name, score from people order by score asc, age desc null first",// 0
    // create table test
    "create table store1 (c1 string, c2 long) as select name, score from people order by score asc, age desc null first",// 1
    // create table test
    "create table store2 using rcfile with ('rcfile.buffer' = 4096) as select name, score from people order by score asc, age desc null first", // 2
    // create table def
    "create table table1 (name string, age int, earn long, score float) using rcfile with ('rcfile.buffer' = 4096)", // 4
    // create table def with location
    "create external table table1 (name string, age int, earn long, score float) using csv with ('csv.delimiter'='|') location '/tmp/data'" // 5
  };

  @Test
  public final void testCreateTable1() {
    CreateTableStmt stmt = (CreateTableStmt) analyzer.parse(createTableStmts[0]).getParseTree();
    assertEquals("table1", stmt.getTableName());
    assertTrue(stmt.hasDefinition());

    stmt = (CreateTableStmt) analyzer.parse(createTableStmts[1]).getParseTree();
    assertEquals("table1", stmt.getTableName());
    assertTrue(stmt.hasDefinition());
    assertTrue(stmt.hasStoreType());
    assertEquals(StoreType.RCFILE, stmt.getStoreType());

    stmt = (CreateTableStmt) analyzer.parse(createTableStmts[2]).getParseTree();
    assertEquals("table1", stmt.getTableName());
    assertTrue(stmt.hasDefinition());
    assertTrue(stmt.hasStoreType());
    assertEquals(StoreType.RCFILE, stmt.getStoreType());
    assertTrue(stmt.hasOptions());
    assertEquals("4096", stmt.getOptions().get("rcfile.buffer"));
  }
  
  @Test
  public final void testCreateTableAsSelect() {
    CreateTableStmt stmt = (CreateTableStmt) analyzer.parse(createTableStmts[3]).getParseTree();
    assertEquals("store1", stmt.getTableName());
    testOrderByCluse(stmt.getSelectStmt());

    stmt = (CreateTableStmt) analyzer.parse(createTableStmts[4]).getParseTree();
    assertEquals("store1", stmt.getTableName());
    testOrderByCluse(stmt.getSelectStmt());
    assertTrue(stmt.hasTableDef());

    stmt = (CreateTableStmt) analyzer.parse(createTableStmts[5]).getParseTree();
    assertEquals("store2", stmt.getTableName());
    assertEquals(StoreType.RCFILE, stmt.getStoreType());
    assertEquals("4096", stmt.getOptions().get("rcfile.buffer"));
    testOrderByCluse(stmt.getSelectStmt());
  }

  @Test
  public final void testCreateTableDef1() {
    CreateTableStmt stmt = (CreateTableStmt) analyzer.parse(createTableStmts[6]).getParseTree();
    assertEquals("table1", stmt.getTableName());
    Schema def = stmt.getTableDef();
    assertEquals("name", def.getColumn(0).getColumnName());
    assertEquals(DataType.STRING, def.getColumn(0).getDataType());
    assertEquals("age", def.getColumn(1).getColumnName());
    assertEquals(DataType.INT, def.getColumn(1).getDataType());
    assertEquals("earn", def.getColumn(2).getColumnName());
    assertEquals(DataType.LONG, def.getColumn(2).getDataType());
    assertEquals("score", def.getColumn(3).getColumnName());
    assertEquals(DataType.FLOAT, def.getColumn(3).getDataType());
    assertEquals(StoreType.RCFILE, stmt.getStoreType());
    assertFalse(stmt.hasPath());
    assertTrue(stmt.hasOptions());
    assertEquals("4096", stmt.getOptions().get("rcfile.buffer"));
  }
  
  @Test
  public final void testCreateTableDef2() {
    CreateTableStmt stmt = (CreateTableStmt) analyzer.parse(createTableStmts[7]).getParseTree();
    assertEquals("table1", stmt.getTableName());
    Schema def = stmt.getTableDef();
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

  // create index
  final static String createIndexStmt =
      "create unique index score_idx on people using hash (score, age desc null first) with ('fillfactor' = 70)";
  
  @Test 
  public final void testCreateIndex() {
    CreateIndexStmt stmt = (CreateIndexStmt) analyzer.parse(createIndexStmt).getParseTree();
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
  
  private String [] INVALID_QUERIES = {
      "select * from invalid", // 0 - when a given table does not exist
      "select time, age from people", // 1 - when a given column does not exist
      "select age from people group by age2" // 2 - when a grouping field does not eixst
  };
  @Test(expected = InvalidQueryException.class)
  public final void testNoSuchTables()  {
    analyzer.parse(INVALID_QUERIES[0]);
  }
  
  @Test(expected = InvalidQueryException.class)
  public final void testNoSuchFields()  {
    analyzer.parse(INVALID_QUERIES[1]);
  }
  
  @Test(expected = InvalidQueryException.class)
  public final void testInvalidGroupFields() {
    QueryBlock block = (QueryBlock) analyzer.parse(INVALID_QUERIES[2]).getParseTree();
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
  /**
   *           join
   *          /    \
   *       join    branch
   *    /       \
   * people  student
   */
  public final void testNaturalJoinClause() {
    QueryBlock block = (QueryBlock) analyzer.parse(JOINS[0]).getParseTree();
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertTrue(join.isNatural());
    assertEquals("branch", join.getRight().getTableName());
    assertTrue(join.hasLeftJoin());
    assertEquals("people", join.getLeftJoin().getLeft().getTableName());
    assertEquals("student", join.getLeftJoin().getRight().getTableName());
  }
  
  @Test
  /**
   *       join
   *    /       \
   * people student
   */
  public final void testInnerJoinClause() {
    QueryBlock block = (QueryBlock) analyzer.parse(JOINS[1]).getParseTree();
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals("people", join.getLeft().getTableName());
    assertEquals("p", join.getLeft().getAlias());
    assertEquals("student", join.getRight().getTableName());
    assertEquals("s", join.getRight().getAlias());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalNode.Type.EQUAL, join.getJoinQual().getType());

    block = (QueryBlock) analyzer.parse(JOINS[2]).getParseTree();
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
    QueryBlock block = (QueryBlock) analyzer.parse(JOINS[6]).getParseTree();
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
    QueryBlock block = (QueryBlock) analyzer.parse(JOINS[3]).getParseTree();
    JoinClause join = block.getJoinClause();
    assertEquals(JoinType.CROSS_JOIN, join.getJoinType());
    assertEquals("branch", join.getRight().getTableName());
    assertTrue(join.hasLeftJoin());
    assertEquals("people", join.getLeftJoin().getLeft().getTableName());
    assertEquals("student", join.getLeftJoin().getRight().getTableName());
  }
  
  @Test
  public final void testLeftOuterJoinClause() {
    QueryBlock block = (QueryBlock) analyzer.parse(JOINS[4]).getParseTree();
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
    QueryBlock block = (QueryBlock) analyzer.parse(JOINS[7]).getParseTree();
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
    QueryBlock block = (QueryBlock) analyzer.parse(JOINS[5]).getParseTree();
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
    QueryBlock block = (QueryBlock) analyzer.parse(JOINS[8]).getParseTree();
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
    ParseTree tree = analyzer.parse(setClauses[0]).getParseTree();
    assertEquals(StatementType.UNION, tree.getType());
    SetStmt union = (SetStmt) tree;
    assertEquals(StatementType.SELECT, union.getLeftTree().getType());
    assertEquals(StatementType.SELECT, union.getRightTree().getType());
    QueryBlock left = (QueryBlock) union.getLeftTree();
    assertEquals("student", left.getFromTables()[0].getTableName());
    QueryBlock right = (QueryBlock) union.getRightTree();
    assertEquals("branch", right.getFromTables()[0].getTableName());
    
    // multiple set statements
    tree = analyzer.parse(setClauses[1]).getParseTree();
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
    ParseTree tree = analyzer.parse(setQualifier[0]).getParseTree();
    assertEquals(StatementType.SELECT, tree.getType());
    QueryBlock block = (QueryBlock) tree;
    assertFalse(block.isDistinct());

    tree = analyzer.parse(setQualifier[1]).getParseTree();
    assertEquals(StatementType.SELECT, tree.getType());
    block = (QueryBlock) tree;
    assertTrue(block.isDistinct());

    tree = analyzer.parse(setQualifier[2]).getParseTree();
    assertEquals(StatementType.SELECT, tree.getType());
    block = (QueryBlock) tree;
    assertFalse(block.isDistinct());
  }

  @Test
  public final void testTypeInferring() {
    QueryBlock block = (QueryBlock) analyzer.parse("select 1 from alltype where char_col = 'a'").getParseTree();
    assertEquals(DataType.CHAR, block.getWhereCondition().getRightExpr().getValueType()[0]);

    block = (QueryBlock) analyzer.parse("select 1 from alltype where short_col = 1").getParseTree();
    assertEquals(DataType.SHORT, block.getWhereCondition().getRightExpr().getValueType()[0]);

    block = (QueryBlock) analyzer.parse("select 1 from alltype where int_col = 1").getParseTree();
    assertEquals(DataType.INT, block.getWhereCondition().getRightExpr().getValueType()[0]);

    block = (QueryBlock) analyzer.parse("select 1 from alltype where long_col = 1").getParseTree();
    assertEquals(DataType.LONG, block.getWhereCondition().getRightExpr().getValueType()[0]);

    block = (QueryBlock) analyzer.parse("select 1 from alltype where float_col = 1").getParseTree();
    assertEquals(DataType.INT, block.getWhereCondition().getRightExpr().getValueType()[0]);

    block = (QueryBlock) analyzer.parse("select 1 from alltype where float_col = 1.0").getParseTree();
    assertEquals(DataType.FLOAT, block.getWhereCondition().getRightExpr().getValueType()[0]);

    block = (QueryBlock) analyzer.parse("select 1 from alltype where int_col = 1.0").getParseTree();
    assertEquals(DataType.DOUBLE, block.getWhereCondition().getRightExpr().getValueType()[0]);

    block = (QueryBlock) analyzer.parse("select 1 from alltype where double_col = 1.0").getParseTree();
    assertEquals(DataType.DOUBLE, block.getWhereCondition().getRightExpr().getValueType()[0]);

    block = (QueryBlock) analyzer.parse("select 1 from alltype where string_col = 'a'").getParseTree();
    assertEquals(DataType.STRING, block.getWhereCondition().getRightExpr().getValueType()[0]);
  }

  @Test
  public void testCaseWhen() {
    ParseTree tree = analyzer.parse(
        "select case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) "+
        "when p_type = 'MOCC' then l_extendedprice - 100 else 0 end as cond from lineitem, part").getParseTree();
    assertEquals(StatementType.SELECT, tree.getType());
    QueryBlock block = (QueryBlock) tree;
    assertTrue(block.getTargetList()[0].hasAlias());
    assertEquals("cond", block.getTargetList()[0].getAlias());
    assertEquals(DataType.DOUBLE, block.getTargetList()[0].getEvalTree().getValueType()[0]);
  }

  @Test
  public void testTarget() throws CloneNotSupportedException {
    QueryBlock.Target t1 = new QueryBlock.Target(new ConstEval(DatumFactory.createInt(5)), 0);
    QueryBlock.Target t2 = (QueryBlock.Target) t1.clone();
    assertEquals(t1,t2);
  }

  final static String [] CopyStmt = {
      "copy lineitem from '/tmp/tpch/lineitem' format rcfile",
      "copy lineitem from '/tmp/tpch/lineitem' format csv with ('csv.delimiter' = '|')"
  };

  @Test
  public void testCopy1() {
    CopyStmt copyStmt =
        (CopyStmt) analyzer.parse(CopyStmt[0]).getParseTree();
    assertEquals("lineitem", copyStmt.getTableName());
    assertEquals(new Path("/tmp/tpch/lineitem"), copyStmt.getPath());
    assertEquals(StoreType.RCFILE, copyStmt.getStoreType());
    assertFalse(copyStmt.hasParams());
  }

  @Test
  public void testCopy2() {
    CopyStmt copyStmt =
        (CopyStmt) analyzer.parse(CopyStmt[1]).getParseTree();
    assertEquals("lineitem", copyStmt.getTableName());
    assertEquals(new Path("/tmp/tpch/lineitem"), copyStmt.getPath());
    assertEquals(StoreType.CSV, copyStmt.getStoreType());
    assertTrue(copyStmt.hasParams());
    assertEquals("|", copyStmt.getParams().get("csv.delimiter"));
  }
}