package tajo.engine.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.junit.Test;
import tajo.engine.query.exception.TQLSyntaxError;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * 이 클래스는 작성된 Antlr룰에 따라 생성되는 파서의 
 * abstract syntext tree가 예상되로 구성되는지 테스트 한다.
 * 
 * @author Hyunsik Choi
 * 
 */
public class TestNQLParser {
  static final String[] selQueries = {
      "select id, name, age, gender from people", // 0
      "select title, ISBN from Books", // 1
      "select studentId from students", // 2
      "session clear", // 3
      "select id, name, age, gender from people as p, students as s", // 4
      "select name, addr, sum(score) from students group by name, addr", // 5
      "select name, addr, age from people where age > 30", // 6
      "select name, addr, age from people where age = 30", // 7
      "select name as n, sum(score, 3+4, 3>4) as total, 3+4 as id from people where age = 30", // 8
      "select ipv4:src_ip from test", // 9
      "select distinct id, name, age, gender from people", // 10
      "select all id, name, age, gender from people", // 11
  };

  static final String[] insQueries = {
      "insert into people values (1, 'hyunsik', 32)",
      "insert into people (id, name, age) values (1, 'hyunsik', 32)" };

  public static Tree parseQuery(String query) throws TQLSyntaxError {
    ANTLRStringStream input = new ANTLRStringStream(query);
    NQLLexer lexer = new NQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    NQLParser parser = new NQLParser(tokens);

    Tree tree;
    try {
      tree = ((Tree) parser.statement().getTree());
    } catch (RecognitionException e) {
      throw new TQLSyntaxError(query, e.getMessage());
    }

    return tree;
  }

  @Test
  public void testSelectClause() throws RecognitionException,
      TQLSyntaxError {
    Tree tree = parseQuery(selQueries[0]);
    assertEquals(tree.getType(), NQLParser.SELECT);
  }
  
  private final String groupingClause [] = {
      "select col0, col1, col2, col3, sum(col4) as total, avg(col5) from base group by col0, cube (col1, col2), rollup(col3) having total > 100"
  };
  
  @Test
  public void testCubeByClause() throws RecognitionException {
    Tree ast = parseQuery(groupingClause[0]);
    assertEquals(NQLParser.SELECT, ast.getType());
    int idx = 0;    
    assertEquals(NQLParser.FROM, ast.getChild(idx++).getType());
    assertEquals(NQLParser.SEL_LIST, ast.getChild(idx++).getType());
    assertEquals(NQLParser.GROUP_BY, ast.getChild(idx).getType());    
    
    Tree groupby = ast.getChild(idx);
    int grpIdx = 0;
    assertEquals(NQLParser.FIELD_NAME, groupby.getChild(grpIdx++).getType());
    assertEquals(NQLParser.CUBE, groupby.getChild(grpIdx).getType());
    
    int cubeIdx = 0;
    Tree cube = groupby.getChild(grpIdx);
    assertEquals(NQLParser.FIELD_NAME, cube.getChild(cubeIdx++).getType());
    assertEquals(NQLParser.FIELD_NAME, cube.getChild(cubeIdx++).getType());
    grpIdx++;
    assertEquals(NQLParser.ROLLUP, groupby.getChild(grpIdx).getType());
    
    int rollupIdx = 0;
    Tree rollup = groupby.getChild(grpIdx);
    assertEquals(NQLParser.FIELD_NAME, rollup.getChild(rollupIdx++).getType());
    
    idx++;
    assertEquals(NQLParser.HAVING, ast.getChild(idx).getType());
  }

  @Test
  public void testColumnFamily() throws TQLSyntaxError {
    Tree ast = parseQuery(selQueries[9]);
    assertEquals(NQLParser.SELECT, ast.getType());
    assertEquals(NQLParser.SEL_LIST, ast.getChild(1).getType());
    assertEquals(NQLParser.COLUMN, ast.getChild(1).getChild(0).getType());
    assertEquals(NQLParser.FIELD_NAME, ast.getChild(1).getChild(0).getChild(0)
        .getType());
    FieldName fieldName = new FieldName(ast.getChild(1).getChild(0).getChild(0));
    assertEquals("ipv4", fieldName.getFamilyName());
    assertEquals("src_ip", fieldName.getSimpleName());
    assertEquals("ipv4:src_ip", fieldName.getName());
  }

  @Test
  public void testSetQualifier() throws TQLSyntaxError {
    Tree ast = parseQuery(selQueries[10]);
    assertEquals(NQLParser.SELECT, ast.getType());
    assertEquals(NQLParser.SET_QUALIFIER, ast.getChild(1).getType());
    assertEquals(NQLParser.DISTINCT, ast.getChild(1).getChild(0).getType());
    assertSetListofSetQualifierTest(ast);

    ast = parseQuery(selQueries[11]);
    assertEquals(NQLParser.SELECT, ast.getType());
    assertEquals(NQLParser.SET_QUALIFIER, ast.getChild(1).getType());
    assertEquals(NQLParser.ALL, ast.getChild(1).getChild(0).getType());
    assertSetListofSetQualifierTest(ast);
  }

  private void assertSetListofSetQualifierTest(Tree ast) {
    assertEquals(NQLParser.SEL_LIST, ast.getChild(2).getType());
    assertEquals(NQLParser.COLUMN, ast.getChild(2).getChild(0).getType());
    assertEquals(NQLParser.FIELD_NAME, ast.getChild(2).getChild(0).getChild(0)
        .getType());
  }

  @Test
  public void testSessionClear() throws RecognitionException,
      TQLSyntaxError {
    Tree tree = parseQuery(selQueries[3]);
    assertEquals(tree.getType(), NQLParser.SESSION_CLEAR);
  }

  @Test
  public void testWhereClause() throws RecognitionException, TQLSyntaxError {
    Tree tree = parseQuery(selQueries[6]);

    assertEquals(tree.getType(), NQLParser.SELECT);
    tree = tree.getChild(2).getChild(0);

    assertEquals(tree.getType(), NQLParser.GTH);
    assertEquals(tree.getChild(0).getType(), NQLParser.FIELD_NAME);
    FieldName fieldName = new FieldName(tree.getChild(0));
    assertEquals(fieldName.getName(), "age");
    assertEquals(tree.getChild(1).getType(), NQLParser.DIGIT);
    assertEquals(tree.getChild(1).getText(), "30");
  }

  @Test
  public void testInsertClause() throws RecognitionException,
      TQLSyntaxError {
    Tree tree = parseQuery(insQueries[0]);
    assertEquals(tree.getType(), NQLParser.INSERT);
  }
  
  static String [] JOINS = {
    "select name, addr from people natural join student natural join professor", // 0
    "select name, addr from people inner join student on people.name = student.name", // 1
    "select name, addr from people inner join student using (id, name)", // 2
    "select name, addr from people join student using (id, name)", // 3
    "select name, addr from people cross join student", // 4
    "select name, addr from people left outer join student on people.name = student.name", // 5
    "select name, addr from people right outer join student on people.name = student.name", // 6
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
        "natural full outer join table12 ",
         // 7 - all possible join clauses*/
    "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, ps_supplycost " + // 8
      "from region join nation on n_regionkey = r_regionkey and r_name = 'EUROPE' " +
      "join supplier on s_nationekey = n_nationkey " +
      "join partsupp on s_suppkey = ps_ps_suppkey " +
      "join part on p_partkey = ps_partkey and p_type like '%BRASS' and p_size = 15"
  };
  
  @Test
  /**
   *                 from
   *        ---------------------
   *       /     |               \
   *      /      \                \
   *   TABLE     join              join
   *   /       /     \            /     \
   *  people  NATURAL TABLE     NATURAL TABLE
   *                    /                 /
   *                  student          professor
   */
  public void testNaturalJoinClause() {
    Tree tree = parseQuery(JOINS[0]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assertEquals(NQLParser.TABLE, fromAST.getChild(0).getType());
    assertEquals("people", fromAST.getChild(0).getChild(0).getText());
    assertEquals(NQLParser.JOIN, fromAST.getChild(1).getType());
    assertEquals(NQLParser.NATURAL, fromAST.getChild(1).getChild(0).getType());
    assertEquals("student", fromAST.getChild(1).getChild(1).getChild(0).getText());
    assertEquals(NQLParser.JOIN, fromAST.getChild(2).getType());
    assertEquals(NQLParser.NATURAL, fromAST.getChild(2).getChild(0).getType());
    assertEquals("professor", fromAST.getChild(2).getChild(1).getChild(0).getText());
  }
  
  @Test
  public void testInnerJoinClause() {
    Tree tree = parseQuery(JOINS[1]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assert2WayJoinAST(fromAST, NQLParser.INNER);
    assertEquals(NQLParser.ON, fromAST.getChild(1).getChild(2).getType());
    
    tree = parseQuery(JOINS[2]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    fromAST = (CommonTree) tree.getChild(0);
    assert2WayJoinAST(fromAST, NQLParser.INNER);
    assertEquals(NQLParser.USING, fromAST.getChild(1).getChild(2).getType());
    assertEquals(2, fromAST.getChild(1).getChild(2).getChildCount());

    /**
     *       from
     *       /     \
     *     TABLE          join
     *      /       /             \
     *    people   TABLE        using
     *              /       /              \
     *            student  FIELD_NAME  FIELD_NAME
     *                      /              \
     *                     id              name
     */
    tree = parseQuery(JOINS[3]);
    fromAST = (CommonTree) tree.getChild(0);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.TABLE, fromAST.getChild(0).getType());
    assertEquals("people", fromAST.getChild(0).getChild(0).getText());
    assertEquals(NQLParser.JOIN, fromAST.getChild(1).getType());
    assertEquals(NQLParser.TABLE, fromAST.getChild(1).getChild(0).getType());
    assertEquals("student", fromAST.getChild(1).getChild(0).getChild(0).getText());
    assertEquals(NQLParser.USING, fromAST.getChild(1).getChild(1).getType());
    assertEquals(2, fromAST.getChild(1).getChild(1).getChildCount());
  }

  private void assert2WayJoinAST(Tree fromAST, int joinType) {
    assertEquals(NQLParser.TABLE, fromAST.getChild(0).getType());
    assertEquals("people", fromAST.getChild(0).getChild(0).getText());
    assertEquals(NQLParser.JOIN, fromAST.getChild(1).getType());
    assertEquals(joinType, fromAST.getChild(1).getChild(0).getType());
    assertEquals(NQLParser.TABLE, fromAST.getChild(1).getChild(1).getType());
    assertEquals("student", fromAST.getChild(1).getChild(1).getChild(0).getText());
  }
  
  @Test
  public void testCrossJoinClause() {
    Tree tree = parseQuery(JOINS[4]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assert2WayJoinAST(fromAST, NQLParser.CROSS);
  }
  
  @Test
  public void testLeftOuterJoinClause() {
    Tree tree = parseQuery(JOINS[5]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assert2WayJoinAST(fromAST, NQLParser.OUTER);
    assertEquals(NQLParser.LEFT, fromAST.getChild(1).getChild(0).getChild(0).getType());
    assertEquals(NQLParser.ON, fromAST.getChild(1).getChild(2).getType());
    assertEquals(1, fromAST.getChild(1).getChild(2).getChildCount());
  }
  
  @Test
  public void testRightOuterJoinClause() {
    Tree tree = parseQuery(JOINS[6]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assert2WayJoinAST(fromAST, NQLParser.OUTER);
    assertEquals(NQLParser.RIGHT, fromAST.getChild(1).getChild(0).getChild(0).getType());
    assertEquals(NQLParser.ON, fromAST.getChild(1).getChild(2).getType());
    assertEquals(1, fromAST.getChild(1).getChild(2).getChildCount());
  }

  @Test
  public void testAllJoinTypes() {
    Tree tree = parseQuery(JOINS[7]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);

    assertEquals(12,fromAST.getChildCount());
    assertEquals(NQLParser.TABLE, fromAST.getChild(0).getType());

    assertEquals(NQLParser.JOIN, fromAST.getChild(1).getType());
    Tree join = fromAST.getChild(1);
    assertEquals(NQLParser.CROSS, join.getChild(0).getType());
    assertEquals("table2", join.getChild(1).getChild(0).getText());

    assertEquals(NQLParser.JOIN, fromAST.getChild(2).getType());
    join = fromAST.getChild(2);
    assertEquals(NQLParser.TABLE, join.getChild(0).getType());
    assertEquals("table3", join.getChild(0).getChild(0).getText());

    assertEquals(NQLParser.JOIN, fromAST.getChild(3).getType());
    join = fromAST.getChild(3);
    assertEquals(NQLParser.INNER, join.getChild(0).getType());
    assertEquals("table4", join.getChild(1).getChild(0).getText());

    assertEquals(NQLParser.JOIN, fromAST.getChild(4).getType());
    join = fromAST.getChild(4);
    assertEquals(NQLParser.OUTER, join.getChild(0).getType());
    assertEquals(NQLParser.LEFT, join.getChild(0).getChild(0).getType());
    assertEquals("table5", join.getChild(1).getChild(0).getText());

    assertEquals(NQLParser.JOIN, fromAST.getChild(5).getType());
    join = fromAST.getChild(5);
    assertEquals(NQLParser.OUTER, join.getChild(0).getType());
    assertEquals(NQLParser.RIGHT, join.getChild(0).getChild(0).getType());
    assertEquals("table6", join.getChild(1).getChild(0).getText());

    assertEquals(NQLParser.JOIN, fromAST.getChild(6).getType());
    join = fromAST.getChild(6);
    assertEquals(NQLParser.OUTER, join.getChild(0).getType());
    assertEquals(NQLParser.FULL, join.getChild(0).getChild(0).getType());
    assertEquals("table7", join.getChild(1).getChild(0).getText());

    assertEquals(NQLParser.JOIN, fromAST.getChild(7).getType());
    join = fromAST.getChild(7);
    assertEquals(NQLParser.NATURAL, join.getChild(0).getType());
    assertEquals("table8", join.getChild(1).getChild(0).getText());

    assertEquals(NQLParser.JOIN, fromAST.getChild(8).getType());
    join = fromAST.getChild(8);
    assertEquals(NQLParser.NATURAL, join.getChild(0).getType());
    assertEquals(NQLParser.INNER, join.getChild(1).getType());
    assertEquals("table9", join.getChild(2).getChild(0).getText());

    assertEquals(NQLParser.JOIN, fromAST.getChild(9).getType());
    join = fromAST.getChild(9);
    assertEquals(NQLParser.NATURAL, join.getChild(0).getType());
    assertEquals(NQLParser.OUTER, join.getChild(1).getType());
    assertEquals(NQLParser.LEFT, join.getChild(1).getChild(0).getType());
    assertEquals("table10", join.getChild(2).getChild(0).getText());

    assertEquals(NQLParser.JOIN, fromAST.getChild(10).getType());
    join = fromAST.getChild(10);
    assertEquals(NQLParser.NATURAL, join.getChild(0).getType());
    assertEquals(NQLParser.OUTER, join.getChild(1).getType());
    assertEquals(NQLParser.RIGHT, join.getChild(1).getChild(0).getType());
    assertEquals("table11", join.getChild(2).getChild(0).getText());

    assertEquals(NQLParser.JOIN, fromAST.getChild(11).getType());
    join = fromAST.getChild(11);
    assertEquals(NQLParser.NATURAL, join.getChild(0).getType());
    assertEquals(NQLParser.OUTER, join.getChild(1).getType());
    assertEquals(NQLParser.FULL, join.getChild(1).getChild(0).getType());
    assertEquals("table12", join.getChild(2).getChild(0).getText());
  }
  
  private final static String setClauses [] = {
    "select a,b,c from table1 union select a,b,c from table1", // 0
    "select a,b,c from table1 union all select a,b,c from table1", // 1
    "select a,b,c from table1 union distinct select a,b,c from table1", // 2
    "select a,b,c from table1 except select a,b,c from table1", // 3
    "select a,b,c from table1 except all select a,b,c from table1", // 4
    "select a,b,c from table1 except distinct select a,b,c from table1", // 5
    "select a,b,c from table1 union select a,b,c from table1 union select a,b,c from table2", // 6
    "select a,b,c from table1 union select a,b,c from table1 intersect select a,b,c from table2" // 7
  };
  
  @Test
  public void testUnionClause() throws RecognitionException {
    Tree tree = parseQuery(setClauses[0]);
    assertEquals(NQLParser.UNION, tree.getType());
    assertEquals(NQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(NQLParser.SELECT, tree.getChild(1).getType());
    
    tree = parseQuery(setClauses[1]);
    assertEquals(NQLParser.UNION, tree.getType());
    assertEquals(NQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(NQLParser.ALL, tree.getChild(1).getType());
    assertEquals(NQLParser.SELECT, tree.getChild(2).getType());
    
    tree = parseQuery(setClauses[2]);
    assertEquals(NQLParser.UNION, tree.getType());
    assertEquals(NQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(NQLParser.DISTINCT, tree.getChild(1).getType());
    assertEquals(NQLParser.SELECT, tree.getChild(2).getType());
    
    tree = parseQuery(setClauses[3]);
    assertEquals(NQLParser.EXCEPT, tree.getType());
    assertEquals(NQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(NQLParser.SELECT, tree.getChild(1).getType());
    
    tree = parseQuery(setClauses[4]);
    assertEquals(NQLParser.EXCEPT, tree.getType());
    assertEquals(NQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(NQLParser.ALL, tree.getChild(1).getType());
    assertEquals(NQLParser.SELECT, tree.getChild(2).getType());
    
    tree = parseQuery(setClauses[5]);
    assertEquals(NQLParser.EXCEPT, tree.getType());
    assertEquals(NQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(NQLParser.DISTINCT, tree.getChild(1).getType());
    assertEquals(NQLParser.SELECT, tree.getChild(2).getType());
    
    tree = parseQuery(setClauses[6]);
    assertEquals(NQLParser.UNION, tree.getType());
    assertEquals(NQLParser.UNION, tree.getChild(0).getType());
    assertEquals(NQLParser.SELECT, tree.getChild(1).getType());
    assertEquals(NQLParser.SELECT, tree.getChild(0).getChild(0).getType());
    assertEquals(NQLParser.SELECT, tree.getChild(0).getChild(1).getType());
    
    tree = parseQuery(setClauses[7]);
    assertEquals(NQLParser.UNION, tree.getType());
    assertEquals(NQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(NQLParser.INTERSECT, tree.getChild(1).getType());
  }

  static String[] schemaStmts = { 
    "drop table abc",
    "create table name (name string, age int)",
    "create table name (name string, age int) using rcfile",
    "create table name (name string, age int) using rcfile with ('rcfile.buffer'=4096)",
    "create table name as select * from test", // 4
    "create table name (name string, age int) as select * from test", // 5
    "create table name (name string, age int) using rcfile as select * from test", // 6
    "create table name (name string, age int) using rcfile with ('rcfile.buffer'= 4096) as select * from test", // 7
    "create external table table1 (name string, age int, earn long, score float) using csv location '/tmp/data'", // 8
  };

  @Test
  public void testCreateTableAsSelect1() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[1]);
    assertEquals(ast.getType(), NQLParser.CREATE_TABLE);
    assertEquals(NQLParser.ID, ast.getChild(0).getType());
    assertEquals(NQLParser.TABLE_DEF, ast.getChild(1).getType());
  }

  @Test
  public void testCreateTableAsSelect2() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[2]);
    assertEquals(ast.getType(), NQLParser.CREATE_TABLE);
    assertEquals(NQLParser.ID, ast.getChild(0).getType());
    assertEquals(NQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(NQLParser.USING, ast.getChild(2).getType());
    assertEquals("rcfile", ast.getChild(2).getChild(0).getText());
  }

  @Test
  public void testCreateTableAsSelect3() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[3]);
    assertEquals(ast.getType(), NQLParser.CREATE_TABLE);
    assertEquals(NQLParser.ID, ast.getChild(0).getType());
    assertEquals(NQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(NQLParser.USING, ast.getChild(2).getType());
    assertEquals("rcfile", ast.getChild(2).getChild(0).getText());
    assertEquals(NQLParser.PARAMS, ast.getChild(3).getType());
  }

  @Test
  public void testCreateTableAsSelect4() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[4]);
    assertEquals(ast.getType(), NQLParser.CREATE_TABLE);
    assertEquals(NQLParser.ID, ast.getChild(0).getType());
    assertEquals(NQLParser.AS, ast.getChild(1).getType());
    assertEquals(NQLParser.SELECT, ast.getChild(1).getChild(0).getType());
  }

  @Test
  public void testCreateTableAsSelect5() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[5]);
    assertEquals(ast.getType(), NQLParser.CREATE_TABLE);
    assertEquals(NQLParser.ID, ast.getChild(0).getType());
    assertEquals(NQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(NQLParser.AS, ast.getChild(2).getType());
    assertEquals(NQLParser.SELECT, ast.getChild(2).getChild(0).getType());
  }

  @Test
  public void testCreateTableAsSelect6() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[6]);
    assertEquals(ast.getType(), NQLParser.CREATE_TABLE);
    assertEquals(NQLParser.ID, ast.getChild(0).getType());
    assertEquals(NQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(NQLParser.USING, ast.getChild(2).getType());
    assertEquals(NQLParser.AS, ast.getChild(3).getType());
    assertEquals(NQLParser.SELECT, ast.getChild(3).getChild(0).getType());
  }

  @Test
  public void testCreateTableAsSelect7() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[7]);
    assertEquals(ast.getType(), NQLParser.CREATE_TABLE);
    assertEquals(NQLParser.ID, ast.getChild(0).getType());
    assertEquals(NQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(NQLParser.USING, ast.getChild(2).getType());
    assertEquals(NQLParser.PARAMS, ast.getChild(3).getType());
    assertEquals(NQLParser.AS, ast.getChild(4).getType());
    assertEquals(NQLParser.SELECT, ast.getChild(4).getChild(0).getType());
  }
  
  @Test
  public void testCreateTableLocation1() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[8]);
    assertEquals(ast.getType(), NQLParser.CREATE_TABLE);
    assertEquals(NQLParser.ID, ast.getChild(0).getType());
    assertEquals(NQLParser.EXTERNAL, ast.getChild(1).getType());
    assertEquals(NQLParser.TABLE_DEF, ast.getChild(2).getType());
    assertEquals(NQLParser.USING, ast.getChild(3).getType());
    assertEquals(NQLParser.LOCATION, ast.getChild(4).getType());
    assertEquals("/tmp/data", ast.getChild(4).getChild(0).getText());
  }

  @Test
  public void testDropTable() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[0]);
    assertEquals(ast.getType(), NQLParser.DROP_TABLE);
    assertEquals(ast.getChild(0).getText(), "abc");
  }

  static String[] copyStmts = {
      "copy employee from '/tmp/table1' format csv with ('csv.delimiter' = '|')"
  };

  @Test
  public void testCopyTable() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(copyStmts[0]);
    assertEquals(ast.getType(), NQLParser.COPY);
    int idx = 0;
    assertEquals("employee", ast.getChild(idx++).getText());
    assertEquals("/tmp/table1", ast.getChild(idx++).getText());
    assertEquals("csv", ast.getChild(idx++).getText());
    assertEquals(NQLParser.PARAMS, ast.getChild(idx).getType());
    Tree params = ast.getChild(idx);
    assertEquals(NQLParser.PARAM, params.getChild(0).getType());
    assertEquals("csv.delimiter", params.getChild(0).getChild(0).getText());
    assertEquals("|", params.getChild(0).getChild(1).getText());
  }


  static String[] controlStmts = { "\\t", "\\t abc" };

  @Test
  public void testShowTable() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(controlStmts[0]);
    assertEquals(ast.getType(), NQLParser.SHOW_TABLE);
    assertEquals(ast.getChildCount(), 0);

    ast = parseQuery(controlStmts[1]);
    assertEquals(ast.getType(), NQLParser.SHOW_TABLE);
    assertEquals(ast.getChild(0).getText(), "abc");
  }

  static String[] exprs = { 
      "1 + 2", // 0
      "3 - 4", // 1
      "5 * 6", // 2
      "7 / 8", // 3
      "10 % 2", // 4
      "1 * 2 > 3 / 4", // 5
      "1 * 2 < 3 / 4", // 6
      "1 * 2 = 3 / 4", // 7
      "1 * 2 != 3 / 4", // 8
      "1 * 2 <> 3 / 4", // 9
      "(3, 4)", // 10
      "('male', 'female')", // 11
      "gender in ('male', 'female')", // 12
      "gender not in ('male', 'female')", // 13
      "score > 90 and age < 20", // 14
      "score > 90 and age < 20 and name != 'hyunsik'", // 15
      "score > 90 or age < 20", // 16
      "score > 90 or age < 20 and name != 'hyunsik'", // 17
      "((a+3 > 1) or 1=1) and (3 != (abc + 4) and type in (3,4))", // 18
      "3", // 19
      "1.2", // 20
      "sum(age)", // 21
      "now()", // 22
      "not (90 > 100)", // 23
      "type like '%top'", // 24
      "type not like 'top%'", // 25
      "col = 'value'", // 26
      "col is null", // 27
      "col is not null", // 28
      "col = null", // 29
      "col != null" // 30

  };

  public static NQLParser parseExpr(String expr) {
    ANTLRStringStream input = new ANTLRStringStream(expr);
    NQLLexer lexer = new NQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    return new NQLParser(tokens);
  }

  @Test
  public void testArithEvalTree() throws RecognitionException {
    NQLParser p = parseExpr(exprs[0]);
    CommonTree node = (CommonTree) p.expr().getTree();

    assertEquals(node.getText(), "+");
    assertEquals(node.getChild(0).getText(), "1");
    assertEquals(node.getChild(1).getText(), "2");

    p = parseExpr(exprs[1]);
    node = (CommonTree) p.expr().getTree();
    assertEquals(node.getText(), "-");
    assertEquals(node.getChild(0).getText(), "3");
    assertEquals(node.getChild(1).getText(), "4");

    p = parseExpr(exprs[2]);
    node = (CommonTree) p.expr().getTree();
    assertEquals(node.getText(), "*");
    assertEquals(node.getChild(0).getText(), "5");
    assertEquals(node.getChild(1).getText(), "6");

    p = parseExpr(exprs[3]);
    node = (CommonTree) p.expr().getTree();
    assertEquals(node.getText(), "/");
    assertEquals(node.getChild(0).getText(), "7");
    assertEquals(node.getChild(1).getText(), "8");

    p = parseExpr(exprs[4]);
    node = (CommonTree) p.expr().getTree();
    assertEquals(node.getText(), "%");
    assertEquals(node.getChild(0).getText(), "10");
    assertEquals(node.getChild(1).getText(), "2");
  }

  @Test
  public void testCompEvalTree() throws RecognitionException {
    NQLParser p = parseExpr(exprs[5]);
    CommonTree node = (CommonTree) p.comparison_predicate().getTree();
    assertEquals(node.getText(), ">");
    assertEquals("*", node.getChild(0).getText());
    assertEquals("/", node.getChild(1).getText());

    p = parseExpr(exprs[6]);
    node = (CommonTree) p.comparison_predicate().getTree();
    assertEquals(node.getText(), "<");
    assertEquals(node.getChild(0).getText(), "*");
    assertEquals(node.getChild(1).getText(), "/");

    p = parseExpr(exprs[7]);
    node = (CommonTree) p.comparison_predicate().getTree();
    assertEquals(node.getText(), "=");
    assertEquals(node.getChild(0).getText(), "*");
    assertEquals(node.getChild(1).getText(), "/");

    p = parseExpr(exprs[8]);
    node = (CommonTree) p.comparison_predicate().getTree();
    assertEquals(node.getText(), "!=");
    assertEquals(node.getChild(0).getText(), "*");
    assertEquals(node.getChild(1).getText(), "/");

    p = parseExpr(exprs[9]);
    node = (CommonTree) p.comparison_predicate().getTree();
    assertEquals(node.getText(), "<>");
    assertEquals(node.getChild(0).getText(), "*");
    assertEquals(node.getChild(1).getText(), "/");
  }

  @Test
  public void testAtomArray() throws RecognitionException {
    NQLParser p = parseExpr(exprs[10]);
    CommonTree node = (CommonTree) p.array().getTree();
    assertEquals(node.getChild(0).getText(), "3");
    assertEquals(node.getChild(1).getText(), "4");

    p = parseExpr(exprs[11]);
    node = (CommonTree) p.array().getTree();
    assertEquals(node.getChild(0).getText(), "male");
    assertEquals(node.getChild(1).getText(), "female");
  }

  @Test
  public void testInEvalTree() throws RecognitionException {
    NQLParser p = parseExpr(exprs[12]);
    CommonTree node = (CommonTree) p.in_predicate().getTree();
    assertEquals(node.getType(), NQLParser.IN);
    assertEquals(node.getChild(1).getText(), "male");
    assertEquals(node.getChild(2).getText(), "female");

    p = parseExpr(exprs[13]);
    node = (CommonTree) p.in_predicate().getTree();
    assertEquals(node.getType(), NQLParser.IN);
    assertEquals(node.getChild(0).getType(), NQLParser.FIELD_NAME);
    FieldName fieldName = new FieldName(node.getChild(0));
    assertEquals(fieldName.getName(), "gender");
    assertEquals(node.getChild(1).getText(), "male");
    assertEquals(node.getChild(2).getText(), "female");
    assertEquals(node.getChild(3).getType(), NQLParser.NOT);
  }

  @Test
  public void testAndEvalTree() throws RecognitionException {
    NQLParser p = parseExpr(exprs[14]);
    CommonTree node = (CommonTree) p.and_predicate().getTree();
    assertEquals(node.getText(), "and");
    assertEquals(node.getChild(0).getText(), ">");
    assertEquals(node.getChild(1).getText(), "<");

    p = parseExpr(exprs[15]);
    node = (CommonTree) p.and_predicate().getTree();
    assertEquals(node.getText(), "and");
    assertEquals(node.getChild(0).getText(), "and");
    assertEquals(node.getChild(1).getText(), "!=");
  }

  @Test
  public void testOrEvalTree() throws RecognitionException {
    NQLParser p = parseExpr(exprs[16]);
    CommonTree node = (CommonTree) p.bool_expr().getTree();
    assertEquals(node.getText(), "or");
    assertEquals(node.getChild(0).getText(), ">");
    assertEquals(node.getChild(1).getText(), "<");

    p = parseExpr(exprs[17]);
    node = (CommonTree) p.bool_expr().getTree();
    assertEquals(node.getText(), "or");
    assertEquals(node.getChild(0).getText(), ">");
    assertEquals(node.getChild(1).getText(), "and");
  }

  @Test
  public void testComplexEvalTree() throws RecognitionException {
    NQLParser p = parseExpr(exprs[18]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(node.getText(), "and");
    assertEquals(node.getChild(0).getText(), "or");
    assertEquals(node.getChild(1).getText(), "and");
  }
  
  @Test
  public void testNotEvalTree() throws RecognitionException {
    NQLParser p = parseExpr(exprs[23]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(node.getType(), NQLParser.NOT);
    assertEquals(node.getChild(0).getType(), NQLParser.GTH);
    CommonTree gth = (CommonTree) node.getChild(0);
    assertEquals(gth.getChild(0).getType(), NQLParser.DIGIT);
    assertEquals(gth.getChild(1).getType(), NQLParser.DIGIT);
  }

  @Test
  public void testFuncCallEvalTree() throws RecognitionException {
    NQLParser p = parseExpr(exprs[21]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(node.getType(), NQLParser.FUNCTION);
    assertEquals(node.getText(), "sum");
    assertEquals(node.getChild(0).getType(), NQLParser.FIELD_NAME);
    FieldName fieldName = new FieldName(node.getChild(0));
    assertEquals(fieldName.getName(), "age");

    p = parseExpr(exprs[22]);
    node = (CommonTree) p.search_condition().getTree();
    assertEquals(node.getType(), NQLParser.FUNCTION);
    assertEquals(node.getText(), "now");
    assertNull(node.getChild(1));
  }
  
  @Test
  public void testLikeEvalTree() throws RecognitionException {
    NQLParser p = parseExpr(exprs[24]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(NQLParser.LIKE, node.getType());
    assertEquals(NQLParser.FIELD_NAME, node.getChild(0).getType());
    FieldName fieldName = new FieldName(node.getChild(0));
    assertEquals(fieldName.getName(), "type");
    assertEquals(NQLParser.STRING, node.getChild(1).getType());
    
    p = parseExpr(exprs[25]);
    node = (CommonTree) p.search_condition().getTree();    
    assertEquals(NQLParser.NOT, node.getChild(0).getType());
    assertEquals(NQLParser.LIKE, node.getType());
    assertEquals(NQLParser.FIELD_NAME, node.getChild(1).getType());
    fieldName = new FieldName(node.getChild(1));
    assertEquals(fieldName.getName(), "type");
    assertEquals(NQLParser.STRING, node.getChild(2).getType());
  }

  @Test
  /**
   * TODO - needs more tests
   */
  public void testConstEval() throws RecognitionException {
    NQLParser p = parseExpr(exprs[26]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(NQLParser.EQUAL, node.getType());
    assertEquals(NQLParser.FIELD_NAME, node.getChild(0).getType());
    assertEquals(NQLParser.STRING, node.getChild(1).getType());
  }

  @Test
  public void testIsNull() throws RecognitionException {
    NQLParser p = parseExpr(exprs[27]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(NQLParser.IS, node.getType());
    assertEquals(NQLParser.FIELD_NAME, node.getChild(0).getType());
    assertEquals(NQLParser.NULL, node.getChild(1).getType());
  }

  @Test
  public void testIsNotNull() throws RecognitionException {
    NQLParser p = parseExpr(exprs[28]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(NQLParser.IS, node.getType());
    assertEquals(NQLParser.FIELD_NAME, node.getChild(0).getType());
    assertEquals(NQLParser.NULL, node.getChild(1).getType());
    assertEquals(NQLParser.NOT, node.getChild(2).getType());
  }

  @Test
  public void testEqualNull() throws RecognitionException {
    NQLParser p = parseExpr(exprs[29]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(NQLParser.EQUAL, node.getType());
    assertEquals(NQLParser.FIELD_NAME, node.getChild(0).getType());
    assertEquals(NQLParser.NULL, node.getChild(1).getType());
  }

  @Test
  public void testNotEqualNull() throws RecognitionException {
    NQLParser p = parseExpr(exprs[30]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(NQLParser.NOT_EQUAL, node.getType());
    assertEquals(NQLParser.FIELD_NAME, node.getChild(0).getType());
    assertEquals(NQLParser.NULL, node.getChild(1).getType());
  }

  public static String [] caseStatements = {
      "select " +
          "case " +
          "when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) " +
          "when p_type = 'MOCC' then l_extendedprice - 100 " +
          "else 0 end " +
          "as cond " +
          "from lineitem"
  };

  @Test
  public void testCaseWhen() throws RecognitionException {
    NQLParser p = parseExpr(caseStatements[0]);
    CommonTree node = (CommonTree) p.statement().getTree();

    assertEquals(NQLParser.SEL_LIST, node.getChild(1).getType());
    CommonTree selList = (CommonTree) node.getChild(1);
    assertEquals(1, selList.getChildCount());
    assertEquals(NQLParser.COLUMN, selList.getChild(0).getType());

    CommonTree column = (CommonTree) selList.getChild(0);
    assertEquals(NQLParser.CASE, column.getChild(0).getType());
    CommonTree caseStatement = (CommonTree) column.getChild(0);

    assertEquals(3, caseStatement.getChildCount());
    assertEquals(NQLParser.WHEN, caseStatement.getChild(0).getType());
    assertEquals(NQLParser.WHEN, caseStatement.getChild(1).getType());
    assertEquals(NQLParser.ELSE, caseStatement.getChild(2).getType());

    CommonTree cond1 = (CommonTree) caseStatement.getChild(0);
    CommonTree cond2 = (CommonTree) caseStatement.getChild(1);
    CommonTree elseStmt = (CommonTree) caseStatement.getChild(2);

    assertEquals(NQLParser.LIKE, cond1.getChild(0).getType());
    assertEquals(NQLParser.EQUAL, cond2.getChild(0).getType());
    assertEquals(NQLParser.DIGIT, elseStmt.getChild(0).getType());
  }

  public class FieldName {
    private final String tableName;
    private final String familyName;
    private final String fieldName;

    public FieldName(Tree tree) {
      if(tree.getChild(1) == null) {
        this.tableName = null;
      } else {
        this.tableName = tree.getChild(1).getText();
      }

      String name = tree.getChild(0).getText();
      if(name.contains(":")) {
        String [] splits = name.split(":");
        this.familyName = splits[0];
        this.fieldName = splits[1];
      } else {
        this.familyName = null;
        this.fieldName = name;
      }
    }

    public boolean hasTableName() {
      return this.tableName != null;
    }

    public String getTableName(){
      return this.tableName;
    }

    public boolean hasFamilyName() {
      return this.familyName != null;
    }

    public String getFamilyName() {
      return this.familyName;
    }

    public String getFullName() {
      StringBuilder sb = new StringBuilder();
      if(tableName != null) {
        sb.append(tableName)
            .append(".");
      }
      if(familyName != null) {
        sb.append(familyName).
            append(":");
      }
      sb.append(fieldName);

      return sb.toString();
    }

    public String getName() {
      if(familyName == null) {
        return fieldName;
      } else
        return familyName+":"+fieldName;
    }

    public String getSimpleName() {
      return this.fieldName;
    }

    public String toString() {
      return getName();
    }
  }
}