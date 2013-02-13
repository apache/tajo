/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.frontend.sql;

import org.junit.BeforeClass;
import org.junit.Test;
import tajo.algebra.*;
import tajo.algebra.Aggregation.GroupElement;

import static junit.framework.Assert.*;
import static tajo.algebra.Aggregation.GroupType;

public class TestSQLAnalyzer {
  private static SQLAnalyzer analyzer = null;

  @BeforeClass
  public static void setup() {
    analyzer = new SQLAnalyzer();
  }

  private String[] QUERIES = {
      "select id, name, score, age from people", // 0
      "select name, score, age from people where score > 30", // 1
      "select name, score, age from people where 3 + 5 * 3", // 2
      "select age, sumtest(score) as total from people group by age having sumtest(score) > 30", // 3
      "select p.id, s.id, score, dept from people as p, student as s where p.id = s.id", // 4
      "select name, score from people order by score asc, age desc null first", // 5
      // only expr
      "select 7 + 8 as total", // 6
      // limit test
      "select id, name, score, age from people limit 3" // 7
  };

  @Test
  public final void testSelectStatement() throws SQLSyntaxError {
    Expr expr = analyzer.parse(QUERIES[0]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertEquals(ExprType.Relation, projection.getChild().getType());
    Relation relation = (Relation) projection.getChild();
    assertEquals("people", relation.getName());
  }

  @Test
  public final void testSelectStatementWithAlias() throws SQLSyntaxError {
    Expr expr = analyzer.parse(QUERIES[4]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertEquals(ExprType.Selection, projection.getChild().getType());
    Selection selection = (Selection) projection.getChild();
    assertEquals(ExprType.Join, selection.getChild().getType());
    Join join = (Join) selection.getChild();
    assertEquals(ExprType.Relation, join.getLeft().getType());
    Relation outer = (Relation) join.getLeft();
    assertEquals("p", outer.getAlias());
    assertEquals(ExprType.Relation, join.getRight().getType());
    Relation inner = (Relation) join.getRight();
    assertEquals("s", inner.getAlias());
  }

  @Test
  public final void testOrderByClause() throws SQLSyntaxError {
    Expr block = analyzer.parse(QUERIES[5]);
    testOrderByCluse(block);
  }

  @Test
  public final void testOnlyExpr() throws SQLSyntaxError {
    Expr expr = analyzer.parse(QUERIES[6]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertEquals(1, projection.getTargets().length);
    Target target = projection.getTargets()[0];
    assertEquals("total", target.getAlias());
    assertEquals(ExprType.Plus, target.getExpr().getType());
  }

  @Test
  public void testLimit() throws SQLSyntaxError {
    Expr expr = analyzer.parse(QUERIES[7]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertEquals(ExprType.Limit, projection.getChild().getType());
  }

  private String[] GROUP_BY = {
      "select age, sumtest(score) as total from people group by age having sumtest(score) > 30", // 0
      "select name, age, sumtest(score) total from people group by cube (name,age)", // 1
      "select name, age, sumtest(score) total from people group by rollup (name,age)", // 2
      "select id, name, age, sumtest(score) total from people group by id, cube (name), rollup (age)", // 3
      "select id, name, age, sumtest(score) total from people group by ()", // 4
  };

  @Test
  public final void testGroupByStatement() throws SQLSyntaxError {
    Expr expr = analyzer.parse(GROUP_BY[0]);
    assertEquals(ExprType.Aggregation, expr.getType());
    Aggregation aggregation = (Aggregation) expr;

    assertEquals(1, aggregation.getGroupSet().length);
    assertEquals("age", aggregation.getGroupSet()[0].getColumns()[0].getName());
    assertTrue(aggregation.hasHavingCondition());
    assertEquals(ExprType.GreaterThan, aggregation.getHavingCondition().getType());
  }

  @Test
  public final void testCubeByStatement() throws SQLSyntaxError {
    Expr expr = analyzer.parse(GROUP_BY[1]);
    assertEquals(ExprType.Aggregation, expr.getType());
    Aggregation aggregation = (Aggregation) expr;
    assertEquals(1, aggregation.getGroupSet().length);
    assertEquals(GroupType.CUBE, aggregation.getGroupSet()[0].getType());
    GroupElement[] groups = aggregation.getGroupSet();
    assertEquals("name", groups[0].getColumns()[0].getName());
    assertEquals("age", groups[0].getColumns()[1].getName());
  }

  @Test
  public final void testRollUpStatement() throws SQLSyntaxError {
    Expr expr = analyzer.parse(GROUP_BY[2]);
    assertEquals(ExprType.Aggregation, expr.getType());
    Aggregation aggregation = (Aggregation) expr;

    assertEquals(1, aggregation.getGroupSet().length);
    assertEquals(GroupType.ROLLUP, aggregation.getGroupSet()[0].getType());
    GroupElement [] groups = aggregation.getGroupSet();
    assertEquals("name", groups[0].getColumns()[0].getName());
    assertEquals("age", groups[0].getColumns()[1].getName());
  }

  @Test
  public final void testMixedGroupByStatement() throws SQLSyntaxError {
    Expr expr = analyzer.parse(GROUP_BY[3]);
    assertEquals(ExprType.Aggregation, expr.getType());
    Aggregation aggregation = (Aggregation) expr;
    assertEquals(3, aggregation.getGroupSet().length);
    int gid = 0;
    GroupElement group = aggregation.getGroupSet()[gid++];
    assertEquals(GroupType.CUBE, group.getType());
    assertEquals("name", group.getColumns()[0].getName());
    group = aggregation.getGroupSet()[gid++];
    assertEquals(GroupType.ROLLUP, group.getType());
    assertEquals("age", group.getColumns()[0].getName());
    group = aggregation.getGroupSet()[gid++];
    assertEquals(GroupType.GROUPBY, group.getType());
    assertEquals("id", group.getColumns()[0].getName());
  }

  @Test
  public final void testEmptyGroupSetStatement() throws SQLSyntaxError {
    Expr expr = analyzer.parse(GROUP_BY[4]);
    assertEquals(ExprType.Aggregation, expr.getType());
    Aggregation block = (Aggregation) expr;
    assertTrue(block.isEmptyGrouping());
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
      "select p.id, dept from people as p right join student as s on p.id= s.people_id", // 8
      "select * from table1 " +
          "cross join table2 " +
          "join table3 on table1.id = table3.id " +
          "inner join table4 on table1.id = table4.id " +
          "left outer join table5 on table1.id = table5.id " +
          "right outer join table6 on table1.id = table6.id " +
          "full outer join table7 on table1.id = table7.id " +
          "natural join table8 " +
          "natural inner join table9 " +
          "natural left outer join table10 " +
          "natural right outer join table11 " +
          "natural full outer join table12 ", // 9 - all possible join clauses*/
      "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, ps_supplycost " + // 10
          "from region join nation on n_regionkey = r_regionkey and r_name = 'EUROPE' " +
          "join supplier on s_nationekey = n_nationkey " +
          "join partsupp on s_suppkey = ps_ps_suppkey " +
          "join part on p_partkey = ps_partkey and p_type like '%BRASS' and p_size = 15"

  };

  @Test
  /**
   *           join
   *          /    \
   *       join    branch
   *     /     \
   * people  student
   *
   */
  public final void testNaturalJoinClause() throws SQLSyntaxError {
    Expr expr = analyzer.parse(JOINS[0]);


    Join join = commonJoinTest(expr);
    assertEquals(JoinType.INNER, join.getJoinType());
    assertTrue(join.isNatural());
    Relation branch = (Relation) join.getRight();
    assertEquals("branch", branch.getName());
    assertEquals(ExprType.Join, join.getLeft().getType());
    Join leftJoin = (Join) join.getLeft();
    Relation people = (Relation) leftJoin.getLeft();
    Relation student = (Relation) leftJoin.getRight();
    assertEquals("people", people.getName());
    assertEquals("student", student.getName());
  }

  private Join commonJoinTest(Expr expr) {
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;

    return (Join) projection.getChild();
  }

  @Test
  /**
   *       join
   *     /      \
   * people   student
   *
   */
  public final void testInnerJoinClause() throws SQLSyntaxError {
    Expr expr = analyzer.parse(JOINS[1]);

    Join join = commonJoinTest(expr);
    assertEquals(JoinType.INNER, join.getJoinType());
    Relation people = (Relation) join.getLeft();
    assertEquals("people", people.getName());
    assertEquals("p", people.getAlias());

    Relation student = (Relation) join.getRight();
    assertEquals("student", student.getName());
    assertEquals("s", student.getAlias());
    assertTrue(join.hasQual());
    assertEquals(ExprType.Equals, join.getQual().getType());

    Expr expr2 = analyzer.parse(JOINS[2]);
    join = commonJoinTest(expr2);
    assertEquals(JoinType.INNER, join.getJoinType());
    Relation people2 = (Relation) join.getLeft();
    assertEquals("people", people2.getName());
    assertEquals("p", people2.getAlias());
    Relation student2 = (Relation) join.getRight();
    assertEquals("student", student2.getName());
    assertEquals("s", student2.getAlias());
    assertTrue(join.hasJoinColumns());
    assertEquals("id", join.getJoinColumns()[0].getName());
  }

  @Test
  public final void testCrossJoinClause() throws SQLSyntaxError {
    Expr expr = analyzer.parse(JOINS[3]);

    Join join = commonJoinTest(expr);
    assertEquals(JoinType.CROSS_JOIN, join.getJoinType());
    Relation branch = (Relation) join.getRight();
    assertEquals("branch", branch.getName());
    assertEquals(ExprType.Join, join.getLeft().getType());
    Join leftJoin = (Join) join.getLeft();
    Relation people = (Relation) leftJoin.getLeft();
    Relation student = (Relation) leftJoin.getRight();
    assertEquals("people", people.getName());
    assertEquals("student", student.getName());
  }

  @Test
  public final void testLeftOuterJoinClause() throws SQLSyntaxError {
    Expr expr = analyzer.parse(JOINS[4]);

    Join join = commonJoinTest(expr);
    assertEquals(JoinType.LEFT_OUTER, join.getJoinType());
    Relation people = (Relation) join.getLeft();
    assertEquals("people", people.getName());
    assertEquals("p", people.getAlias());
    Relation student = (Relation) join.getRight();
    assertEquals("student", student.getName());
    assertEquals("s", student.getAlias());
    assertTrue(join.hasQual());
    assertEquals(ExprType.Equals, join.getQual().getType());
  }

  @Test
  public final void testRightOuterJoinClause() throws SQLSyntaxError {
    Expr expr = analyzer.parse(JOINS[5]);

    Join join = commonJoinTest(expr);
    assertEquals(JoinType.RIGHT_OUTER, join.getJoinType());
    Relation people = (Relation) join.getLeft();
    assertEquals("people", people.getName());
    assertEquals("p", people.getAlias());
    Relation student = (Relation) join.getRight();
    assertEquals("student", student.getName());
    assertEquals("s", student.getAlias());
    assertTrue(join.hasQual());
    assertEquals(ExprType.Equals, join.getQual().getType());
  }

  @Test
  public final void testLeftJoinClause() throws SQLSyntaxError {
    Expr expr = analyzer.parse(JOINS[7]);

    Join join = commonJoinTest(expr);
    assertEquals(JoinType.LEFT_OUTER, join.getJoinType());
    Relation people = (Relation) join.getLeft();
    assertEquals("people", people.getName());
    assertEquals("p", people.getAlias());
    Relation student = (Relation) join.getRight();
    assertEquals("student", student.getName());
    assertEquals("s", student.getAlias());
    assertTrue(join.hasQual());
    assertEquals(ExprType.Equals, join.getQual().getType());
  }

  @Test
  public final void testRightJoinClause() throws SQLSyntaxError {
    Expr expr = analyzer.parse(JOINS[8]);

    Join join = commonJoinTest(expr);
    assertEquals(JoinType.RIGHT_OUTER, join.getJoinType());
    Relation people = (Relation) join.getLeft();
    assertEquals("people", people.getName());
    assertEquals("p", people.getAlias());
    Relation student = (Relation) join.getRight();
    assertEquals("student", student.getName());
    assertEquals("s", student.getAlias());
    assertTrue(join.hasQual());
    assertEquals(ExprType.Equals, join.getQual().getType());
  }

  private final String [] setClauses = {
      "select id, people_id from student union select id, people_id from branch",
      "select id, people_id from student union select id, people_id from branch " +
          "intersect select id, people_id from branch as b"
  };

  @Test
  public final void testUnionClause() throws SQLSyntaxError {
    Expr expr = analyzer.parse(setClauses[0]);
    assertEquals(ExprType.Union, expr.getType());
    SetOperation union = (SetOperation) expr;
    Expr left = union.getLeft();
    Expr right = union.getRight();

    assertEquals(ExprType.Projection, left.getType());
    Projection leftProj = (Projection) left;
    Relation student = (Relation) leftProj.getChild();

    assertEquals(ExprType.Projection, right.getType());
    Projection rightProj = (Projection) right;
    Relation branch = (Relation) rightProj.getChild();

    assertEquals("student", student.getName());
    assertEquals("branch", branch.getName());

    // multiple set statements
    expr = analyzer.parse(setClauses[1]);
    assertEquals(ExprType.Union, expr.getType());
    union = (SetOperation) expr;

    assertEquals(ExprType.Projection, union.getLeft().getType());
    assertEquals(ExprType.Intersect, union.getRight().getType());
    leftProj = (Projection) union.getLeft();
    student = (Relation) leftProj.getChild();
    assertEquals("student", student.getName());

    SetOperation intersect = (SetOperation) union.getRight();
    Relation branch2 = (Relation) ((Projection)intersect.getLeft()).getChild();
    Relation branch3 = (Relation) ((Projection)intersect.getRight()).getChild();
    assertEquals("branch", branch2.getName());
    assertFalse(branch2.hasAlias());
    assertEquals("branch", branch3.getName());
    assertEquals("b", branch3.getAlias());
  }

  @Test
  public void testCaseWhen() throws SQLSyntaxError {
    Expr tree = analyzer.parse(
        "select case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) " +
          "when p_type = 'MOCC' then l_extendedprice - 100 else 0 end as cond from lineitem, part");

    assertEquals(ExprType.Projection, tree.getType());
    Projection projection = (Projection) tree;
    assertTrue(projection.getTargets()[0].hasAlias());
    assertEquals(ExprType.CaseWhen, projection.getTargets()[0].getExpr().getType());
    assertEquals("cond", projection.getTargets()[0].getAlias());
    CaseWhenExpr caseWhen = (CaseWhenExpr) projection.getTargets()[0].getExpr();
    assertEquals(2, caseWhen.getWhens().size());
  }

  public static String [] subQueries = {
      "select c1, c2, c3 from (select c1, c2, c3 from employee) as test",
      "select c1, c2, c3 from table1 where c3 < (select c4 from table2)"
  };

  @Test
  public void testTableSubQuery() throws SQLSyntaxError {
    Expr expr = analyzer.parse(subQueries[0]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertEquals(ExprType.TableSubQuery, projection.getChild().getType());
  }

  @Test
  public void testScalarSubQuery() throws SQLSyntaxError {
    Expr expr = analyzer.parse(subQueries[1]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertEquals(ExprType.Selection, projection.getChild().getType());
  }

  static final String [] setQualifier = {
      "select id, people_id from student",
      "select distinct id, people_id from student",
      "select all id, people_id from student",
  };

  @Test
  public final void testSetQulaifier() throws SQLSyntaxError {
    Expr expr = analyzer.parse(setQualifier[0]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertFalse(projection.isDistinct());

    expr = analyzer.parse(setQualifier[1]);
    assertEquals(ExprType.Projection, expr.getType());
    projection = (Projection) expr;
    assertTrue(projection.isDistinct());

    expr = analyzer.parse(setQualifier[2]);
    assertEquals(ExprType.Projection, expr.getType());
    projection = (Projection) expr;
    assertFalse(projection.isDistinct());
  }

  static final String [] createTableStmts = {
      "create table table1 (name varchar, age int)",
      "create table table1 (name string, age int) using rcfile",
      "create table table1 (name string, age int) using rcfile with ('rcfile.buffer' = 4096)",
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
  public final void testCreateTable1() throws SQLSyntaxError {
    CreateTable stmt = (CreateTable) analyzer.parse(createTableStmts[0]);
    assertEquals("table1", stmt.getRelationName());
    assertTrue(stmt.hasTableElements());

    stmt = (CreateTable) analyzer.parse(createTableStmts[1]);
    assertEquals("table1", stmt.getRelationName());
    assertTrue(stmt.hasTableElements());
    assertTrue(stmt.hasStorageType());
    assertEquals("rcfile", stmt.getStorageType());

    stmt = (CreateTable) analyzer.parse(createTableStmts[2]);
    assertEquals("table1", stmt.getRelationName());
    assertTrue(stmt.hasTableElements());
    assertTrue(stmt.hasStorageType());
    assertEquals("rcfile", stmt.getStorageType());
    assertTrue(stmt.hasParams());
    assertEquals("4096", stmt.getParams().get("rcfile.buffer"));
  }

  @Test
  public final void testCreateTableAsSelect() throws SQLSyntaxError {
    CreateTable stmt = (CreateTable) analyzer.parse(createTableStmts[3]);
    assertEquals("store1", stmt.getRelationName());
    assertTrue(stmt.hasSubQuery());
    testOrderByCluse(stmt.getSubQuery());

    stmt = (CreateTable) analyzer.parse(createTableStmts[4]);
    assertEquals("store1", stmt.getRelationName());
    assertTrue(stmt.hasSubQuery());
    testOrderByCluse(stmt.getSubQuery());
    assertTrue(stmt.hasTableElements());

    stmt = (CreateTable) analyzer.parse(createTableStmts[5]);
    assertEquals("store2", stmt.getRelationName());
    assertEquals("rcfile", stmt.getStorageType());
    assertEquals("4096", stmt.getParams().get("rcfile.buffer"));
    testOrderByCluse(stmt.getSubQuery());
  }

  private static void testOrderByCluse(Expr block) {
    Projection projection = (Projection) block;
    Sort sort = (Sort) projection.getChild();

    assertEquals(2, sort.getSortSpecs().length);
    Sort.SortSpec spec1 = sort.getSortSpecs()[0];
    assertEquals("score", spec1.getKey().getName());
    assertEquals(true, spec1.isAscending());
    assertEquals(false, spec1.isNullFirst());
    Sort.SortSpec spec2 = sort.getSortSpecs()[1];
    assertEquals("age", spec2.getKey().getName());
    assertEquals(false, spec2.isAscending());
    assertEquals(true, spec2.isNullFirst());
  }

  @Test
  public final void testCreateTableDef1() throws SQLSyntaxError {
    CreateTable stmt = (CreateTable) analyzer.parse(createTableStmts[6]);
    assertEquals("table1", stmt.getRelationName());
    CreateTable.ColumnDefinition[] elements = stmt.getTableElements();
    assertEquals("name", elements[0].getColumnName());
    assertEquals("string", elements[0].getDataType());
    assertEquals("age", elements[1].getColumnName());
    assertEquals("int", elements[1].getDataType());
    assertEquals("earn", elements[2].getColumnName());
    assertEquals("long", elements[2].getDataType());
    assertEquals("score", elements[3].getColumnName());
    assertEquals("float", elements[3].getDataType());
    assertEquals("rcfile", stmt.getStorageType());
    assertFalse(stmt.hasLocation());
    assertTrue(stmt.hasParams());
    assertEquals("4096", stmt.getParams().get("rcfile.buffer"));
  }

  @Test
  public final void testCreateTableDef2() throws SQLSyntaxError {
    CreateTable expr = (CreateTable) analyzer.parse(createTableStmts[7]);
    _testCreateTableDef2(expr);
    CreateTable restored = (CreateTable) AlgebraTestingUtil.testJsonSerializer(expr);
    _testCreateTableDef2(restored);
  }

  private void _testCreateTableDef2(CreateTable expr) {
    assertEquals("table1", expr.getRelationName());
    CreateTable.ColumnDefinition[] elements = expr.getTableElements();
    assertEquals("name", elements[0].getColumnName());
    assertEquals("string", elements[0].getDataType());
    assertEquals("age", elements[1].getColumnName());
    assertEquals("int", elements[1].getDataType());
    assertEquals("earn", elements[2].getColumnName());
    assertEquals("long", elements[2].getDataType());
    assertEquals("score", elements[3].getColumnName());
    assertEquals("float", elements[3].getDataType());
    assertEquals("csv", expr.getStorageType());
    assertEquals("/tmp/data", expr.getLocation());
    assertTrue(expr.hasParams());
    assertEquals("|", expr.getParams().get("csv.delimiter"));
  }
}
