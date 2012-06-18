package nta.engine.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import nta.catalog.FieldName;
import nta.engine.query.exception.NQLSyntaxException;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.junit.Test;

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

  public static Tree parseQuery(String query) throws NQLSyntaxException {
    ANTLRStringStream input = new ANTLRStringStream(query);
    NQLLexer lexer = new NQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    NQLParser parser = new NQLParser(tokens);

    Tree tree;
    try {
      tree = ((Tree) parser.statement().getTree());
    } catch (RecognitionException e) {
      throw new NQLSyntaxException(query);
    }

    return tree;
  }

  @Test
  public void testSelectClause() throws RecognitionException,
      NQLSyntaxException {
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
  public void testColumnFamily() throws NQLSyntaxException {
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
  public void testSetQualifier() throws NQLSyntaxException {
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
      NQLSyntaxException {
    Tree tree = parseQuery(selQueries[3]);
    assertEquals(tree.getType(), NQLParser.SESSION_CLEAR);
  }

  @Test
  public void testWhereClause() throws RecognitionException, NQLSyntaxException {
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
      NQLSyntaxException {
    Tree tree = parseQuery(insQueries[0]);
    assertEquals(tree.getType(), NQLParser.INSERT);
  }
  
  static String [] JOINS = {
    "select name, addr from people natural join student natural join professor", // 0
    "select name, addr from people inner join student on people.name = student.name", // 1
    "select name, addr from people inner join student using (id, name)", // 2
    "select name, addr from people cross join student cross join professor", // 3
    "select name, addr from people left outer join student on people.name = student.name", // 4
    "select name, addr from people right outer join student on people.name = student.name" // 5
  };
  
  @Test
  public void testNaturalJoinClause() {
    // l=derivedTable 'natural' t=join_type? 'join' r=tableRef -> ^(JOIN NATURAL_JOIN $t? $l $r)
    Tree tree = parseQuery(JOINS[0]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assertEquals(NQLParser.JOIN, fromAST.getChild(0).getType());
    CommonTree joinAST = (CommonTree) fromAST.getChild(0);
    assertEquals(NQLParser.NATURAL_JOIN, joinAST.getChild(0).getType());
    assertEquals("people", joinAST.getChild(1).getChild(0).getText());
    assertEquals(NQLParser.JOIN, joinAST.getChild(2).getType());
    joinAST = (CommonTree) joinAST.getChild(2);
    assertEquals("student", joinAST.getChild(1).getChild(0).getText());
    assertEquals("professor", joinAST.getChild(2).getChild(0).getText());
  }
  
  @Test
  public void testInnerJoinClause() {
    // l=derivedTable t=join_type? JOIN r=tableRef s=join_specification -> ^(JOIN $t? $l $r $s)
    // 
    Tree tree = parseQuery(JOINS[1]);    
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assertEquals(NQLParser.JOIN, fromAST.getChild(0).getType());
    CommonTree joinAST = (CommonTree) fromAST.getChild(0);
    assertEquals(NQLParser.INNER_JOIN, joinAST.getChild(0).getType());
    assertEquals("people", joinAST.getChild(1).getChild(0).getText());
    assertEquals("student", joinAST.getChild(2).getChild(0).getText());
    assertEquals(NQLParser.ON, joinAST.getChild(3).getType());
    
    tree = parseQuery(JOINS[2]);
    assertEquals(tree.getType(), NQLParser.SELECT);
  }
  
  @Test
  public void testCrossJoinClause() {
    // l=derivedTable CROSS JOIN r=tableRef -> ^(JOIN CROSS_JOIN $l $r)
    Tree tree = parseQuery(JOINS[3]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assertEquals(NQLParser.JOIN, fromAST.getChild(0).getType());
    CommonTree joinAST = (CommonTree) fromAST.getChild(0);
    assertEquals(NQLParser.CROSS_JOIN, joinAST.getChild(0).getType());
    assertEquals("people", joinAST.getChild(1).getChild(0).getText());
    joinAST = (CommonTree) joinAST.getChild(2);
    assertEquals(NQLParser.CROSS_JOIN, joinAST.getChild(0).getType());
    assertEquals("student", joinAST.getChild(1).getChild(0).getText());
    assertEquals("professor", joinAST.getChild(2).getChild(0).getText());    
  }
  
  @Test
  public void testLeftOuterJoinClause() {
    Tree tree = parseQuery(JOINS[4]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assertEquals(NQLParser.JOIN, fromAST.getChild(0).getType());
    CommonTree joinAST = (CommonTree) fromAST.getChild(0);
    assertEquals(NQLParser.OUTER_JOIN, joinAST.getChild(0).getType());
    assertEquals(NQLParser.LEFT, joinAST.getChild(0).getChild(0).getType());
    assertEquals("people", joinAST.getChild(1).getChild(0).getText());
    assertEquals("student", joinAST.getChild(2).getChild(0).getText());
    assertEquals(NQLParser.ON, joinAST.getChild(3).getType());
  }
  
  @Test
  public void testRightOuterJoinClause() {
    Tree tree = parseQuery(JOINS[5]);
    assertEquals(tree.getType(), NQLParser.SELECT);
    assertEquals(NQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assertEquals(NQLParser.JOIN, fromAST.getChild(0).getType());
    CommonTree joinAST = (CommonTree) fromAST.getChild(0);
    assertEquals(NQLParser.OUTER_JOIN, joinAST.getChild(0).getType());
    assertEquals(NQLParser.RIGHT, joinAST.getChild(0).getChild(0).getType());
    assertEquals("people", joinAST.getChild(1).getChild(0).getText());
    assertEquals("student", joinAST.getChild(2).getChild(0).getText());
    assertEquals(NQLParser.ON, joinAST.getChild(3).getType());
  }
  
  private final static String setClauses [] = {
    "select a,b,c from table1 union select a,b,c from table1", // 0
    "select a,b,c from table1 union all select a,b,c from table1", // 1
    "select a,b,c from table1 union distinct select a,b,c from table1", // 2
    "select a,b,c from table1 except select a,b,c from table1", // 3
    "select a,b,c from table1 except all select a,b,c from table1", // 4
    "select a,b,c from table1 except distinct select a,b,c from table1", // 5
    "select a,b,c from table1 union select a,b,c from table1 union select a,b,c from table 2", // 6
    "select a,b,c from table1 union select a,b,c from table1 intersect select a,b,c from table 2" // 7
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
    "create table name as select * from test",
    "create table table1 (name string, age int, earn long, score float) using csv location '/tmp/data'"
  };

  @Test
  public void testCreateTableAsSelect() throws RecognitionException, NQLSyntaxException {
    Tree ast = parseQuery(schemaStmts[1]);
    assertEquals(ast.getType(), NQLParser.CREATE_TABLE);
    assertEquals(ast.getChild(0).getType(), NQLParser.ID);
    assertEquals(ast.getChild(1).getType(), NQLParser.SELECT);
  }
  
  @Test
  public void testCreateTableDef() throws RecognitionException, NQLSyntaxException {
    Tree ast = parseQuery(schemaStmts[2]);
    assertEquals(ast.getType(), NQLParser.CREATE_TABLE);
    assertEquals(ast.getChild(0).getType(), NQLParser.ID);
    assertEquals(ast.getChild(1).getType(), NQLParser.TABLE_DEF);
    assertEquals(ast.getChild(2).getType(), NQLParser.ID);
    assertEquals(ast.getChild(3).getType(), NQLParser.STRING);
  }

  @Test
  public void testDropTable() throws RecognitionException, NQLSyntaxException {
    Tree ast = parseQuery(schemaStmts[0]);
    assertEquals(ast.getType(), NQLParser.DROP_TABLE);
    assertEquals(ast.getChild(0).getText(), "abc");
  }

  static String[] controlStmts = { "\\t", "\\t abc" };

  @Test
  public void testShowTable() throws RecognitionException, NQLSyntaxException {
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
      "col = 'value'" // 26
  };

  public static NQLParser parseExpr(String expr) {
    ANTLRStringStream input = new ANTLRStringStream(expr);
    NQLLexer lexer = new NQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    NQLParser parser = new NQLParser(tokens);
    return parser;
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

  public static String [] caseStatements = {
      "select case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) when p_type = 'MOCC' then l_extendedprice - 100 else 0 end as cond from lineitem",
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
}