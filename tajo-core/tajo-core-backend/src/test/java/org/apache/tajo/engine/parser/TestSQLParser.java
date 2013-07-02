/**
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

package org.apache.tajo.engine.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.junit.Test;
import org.apache.tajo.engine.query.exception.TQLSyntaxError;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestSQLParser {
  static final String[] selQueries = {
      "select id, name, age, gender from people", // 0
      "select title, ISBN from Books", // 1
      "select studentId from students", // 2
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
    SQLLexer lexer = new SQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);

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
    assertEquals(tree.getType(), SQLParser.SELECT);
  }
  
  private final String groupingClause [] = {
      "select col0, col1, col2, col3, sum(col4) as total, avg(col5) from base group by col0, cube (col1, col2), rollup(col3) having total > 100"
  };
  
  @Test
  public void testCubeByClause() throws RecognitionException {
    Tree ast = parseQuery(groupingClause[0]);
    assertEquals(SQLParser.SELECT, ast.getType());
    int idx = 0;    
    assertEquals(SQLParser.FROM, ast.getChild(idx++).getType());
    assertEquals(SQLParser.SEL_LIST, ast.getChild(idx++).getType());
    assertEquals(SQLParser.GROUP_BY, ast.getChild(idx).getType());
    
    Tree groupby = ast.getChild(idx);
    int grpIdx = 0;
    assertEquals(SQLParser.FIELD_NAME, groupby.getChild(grpIdx++).getType());
    assertEquals(SQLParser.CUBE, groupby.getChild(grpIdx).getType());
    
    int cubeIdx = 0;
    Tree cube = groupby.getChild(grpIdx);
    assertEquals(SQLParser.FIELD_NAME, cube.getChild(cubeIdx++).getType());
    assertEquals(SQLParser.FIELD_NAME, cube.getChild(cubeIdx).getType());
    grpIdx++;
    assertEquals(SQLParser.ROLLUP, groupby.getChild(grpIdx).getType());
    
    int rollupIdx = 0;
    Tree rollup = groupby.getChild(grpIdx);
    assertEquals(SQLParser.FIELD_NAME, rollup.getChild(rollupIdx).getType());
    
    idx++;
    assertEquals(SQLParser.HAVING, ast.getChild(idx).getType());
  }

  @Test
  public void testColumnFamily() throws TQLSyntaxError {
    Tree ast = parseQuery(selQueries[8]);
    assertEquals(SQLParser.SELECT, ast.getType());
    assertEquals(SQLParser.SEL_LIST, ast.getChild(1).getType());
    assertEquals(SQLParser.COLUMN, ast.getChild(1).getChild(0).getType());
    assertEquals(SQLParser.FIELD_NAME, ast.getChild(1).getChild(0).getChild(0)
        .getType());
    FieldName fieldName = new FieldName(ast.getChild(1).getChild(0).getChild(0));
    assertEquals("ipv4", fieldName.getFamilyName());
    assertEquals("src_ip", fieldName.getSimpleName());
    assertEquals("ipv4:src_ip", fieldName.getName());
  }

  @Test
  public void testSetQualifier() throws TQLSyntaxError {
    Tree ast = parseQuery(selQueries[9]);
    assertEquals(SQLParser.SELECT, ast.getType());
    assertEquals(SQLParser.SET_QUALIFIER, ast.getChild(1).getType());
    assertEquals(SQLParser.DISTINCT, ast.getChild(1).getChild(0).getType());
    assertSetListofSetQualifierTest(ast);

    ast = parseQuery(selQueries[10]);
    assertEquals(SQLParser.SELECT, ast.getType());
    assertEquals(SQLParser.SET_QUALIFIER, ast.getChild(1).getType());
    assertEquals(SQLParser.ALL, ast.getChild(1).getChild(0).getType());
    assertSetListofSetQualifierTest(ast);
  }

  private void assertSetListofSetQualifierTest(Tree ast) {
    assertEquals(SQLParser.SEL_LIST, ast.getChild(2).getType());
    assertEquals(SQLParser.COLUMN, ast.getChild(2).getChild(0).getType());
    assertEquals(SQLParser.FIELD_NAME, ast.getChild(2).getChild(0).getChild(0)
        .getType());
  }

  @Test
  public void testWhereClause() throws RecognitionException, TQLSyntaxError {
    Tree tree = parseQuery(selQueries[5]);

    assertEquals(tree.getType(), SQLParser.SELECT);
    tree = tree.getChild(2).getChild(0);

    assertEquals(tree.getType(), SQLParser.GTH);
    assertEquals(tree.getChild(0).getType(), SQLParser.FIELD_NAME);
    FieldName fieldName = new FieldName(tree.getChild(0));
    assertEquals(fieldName.getName(), "age");
    assertEquals(tree.getChild(1).getType(), SQLParser.NUMBER);
    assertEquals(tree.getChild(1).getText(), "30");
  }

  @Test
  public void testInsertClause() throws RecognitionException,
      TQLSyntaxError {
    Tree tree = parseQuery(insQueries[0]);
    assertEquals(tree.getType(), SQLParser.INSERT);
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
    assertEquals(tree.getType(), SQLParser.SELECT);
    assertEquals(SQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assertEquals(SQLParser.TABLE, fromAST.getChild(0).getType());
    assertEquals("people", fromAST.getChild(0).getChild(0).getText());
    assertEquals(SQLParser.JOIN, fromAST.getChild(1).getType());
    assertEquals(SQLParser.NATURAL, fromAST.getChild(1).getChild(0).getType());
    assertEquals("student", fromAST.getChild(1).getChild(1).getChild(0).getText());
    assertEquals(SQLParser.JOIN, fromAST.getChild(2).getType());
    assertEquals(SQLParser.NATURAL, fromAST.getChild(2).getChild(0).getType());
    assertEquals("professor", fromAST.getChild(2).getChild(1).getChild(0).getText());
  }
  
  @Test
  public void testInnerJoinClause() {
    Tree tree = parseQuery(JOINS[1]);
    assertEquals(tree.getType(), SQLParser.SELECT);
    assertEquals(SQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assert2WayJoinAST(fromAST, SQLParser.INNER);
    assertEquals(SQLParser.ON, fromAST.getChild(1).getChild(2).getType());
    
    tree = parseQuery(JOINS[2]);
    assertEquals(tree.getType(), SQLParser.SELECT);
    fromAST = (CommonTree) tree.getChild(0);
    assert2WayJoinAST(fromAST, SQLParser.INNER);
    assertEquals(SQLParser.USING, fromAST.getChild(1).getChild(2).getType());
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
    assertEquals(tree.getType(), SQLParser.SELECT);
    assertEquals(SQLParser.TABLE, fromAST.getChild(0).getType());
    assertEquals("people", fromAST.getChild(0).getChild(0).getText());
    assertEquals(SQLParser.JOIN, fromAST.getChild(1).getType());
    assertEquals(SQLParser.TABLE, fromAST.getChild(1).getChild(0).getType());
    assertEquals("student", fromAST.getChild(1).getChild(0).getChild(0).getText());
    assertEquals(SQLParser.USING, fromAST.getChild(1).getChild(1).getType());
    assertEquals(2, fromAST.getChild(1).getChild(1).getChildCount());
  }

  private void assert2WayJoinAST(Tree fromAST, int joinType) {
    assertEquals(SQLParser.TABLE, fromAST.getChild(0).getType());
    assertEquals("people", fromAST.getChild(0).getChild(0).getText());
    assertEquals(SQLParser.JOIN, fromAST.getChild(1).getType());
    assertEquals(joinType, fromAST.getChild(1).getChild(0).getType());
    assertEquals(SQLParser.TABLE, fromAST.getChild(1).getChild(1).getType());
    assertEquals("student", fromAST.getChild(1).getChild(1).getChild(0).getText());
  }
  
  @Test
  public void testCrossJoinClause() {
    Tree tree = parseQuery(JOINS[4]);
    assertEquals(tree.getType(), SQLParser.SELECT);
    assertEquals(SQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assert2WayJoinAST(fromAST, SQLParser.CROSS);
  }
  
  @Test
  public void testLeftOuterJoinClause() {
    Tree tree = parseQuery(JOINS[5]);
    assertEquals(tree.getType(), SQLParser.SELECT);
    assertEquals(SQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assert2WayJoinAST(fromAST, SQLParser.OUTER);
    assertEquals(SQLParser.LEFT, fromAST.getChild(1).getChild(0).getChild(0).getType());
    assertEquals(SQLParser.ON, fromAST.getChild(1).getChild(2).getType());
    assertEquals(1, fromAST.getChild(1).getChild(2).getChildCount());
  }
  
  @Test
  public void testRightOuterJoinClause() {
    Tree tree = parseQuery(JOINS[6]);
    assertEquals(tree.getType(), SQLParser.SELECT);
    assertEquals(SQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);
    assert2WayJoinAST(fromAST, SQLParser.OUTER);
    assertEquals(SQLParser.RIGHT, fromAST.getChild(1).getChild(0).getChild(0).getType());
    assertEquals(SQLParser.ON, fromAST.getChild(1).getChild(2).getType());
    assertEquals(1, fromAST.getChild(1).getChild(2).getChildCount());
  }

  @Test
  public void testAllJoinTypes() {
    Tree tree = parseQuery(JOINS[7]);
    assertEquals(tree.getType(), SQLParser.SELECT);
    assertEquals(SQLParser.FROM, tree.getChild(0).getType());
    CommonTree fromAST = (CommonTree) tree.getChild(0);

    assertEquals(12,fromAST.getChildCount());
    assertEquals(SQLParser.TABLE, fromAST.getChild(0).getType());

    assertEquals(SQLParser.JOIN, fromAST.getChild(1).getType());
    Tree join = fromAST.getChild(1);
    assertEquals(SQLParser.CROSS, join.getChild(0).getType());
    assertEquals("table2", join.getChild(1).getChild(0).getText());

    assertEquals(SQLParser.JOIN, fromAST.getChild(2).getType());
    join = fromAST.getChild(2);
    assertEquals(SQLParser.TABLE, join.getChild(0).getType());
    assertEquals("table3", join.getChild(0).getChild(0).getText());

    assertEquals(SQLParser.JOIN, fromAST.getChild(3).getType());
    join = fromAST.getChild(3);
    assertEquals(SQLParser.INNER, join.getChild(0).getType());
    assertEquals("table4", join.getChild(1).getChild(0).getText());

    assertEquals(SQLParser.JOIN, fromAST.getChild(4).getType());
    join = fromAST.getChild(4);
    assertEquals(SQLParser.OUTER, join.getChild(0).getType());
    assertEquals(SQLParser.LEFT, join.getChild(0).getChild(0).getType());
    assertEquals("table5", join.getChild(1).getChild(0).getText());

    assertEquals(SQLParser.JOIN, fromAST.getChild(5).getType());
    join = fromAST.getChild(5);
    assertEquals(SQLParser.OUTER, join.getChild(0).getType());
    assertEquals(SQLParser.RIGHT, join.getChild(0).getChild(0).getType());
    assertEquals("table6", join.getChild(1).getChild(0).getText());

    assertEquals(SQLParser.JOIN, fromAST.getChild(6).getType());
    join = fromAST.getChild(6);
    assertEquals(SQLParser.OUTER, join.getChild(0).getType());
    assertEquals(SQLParser.FULL, join.getChild(0).getChild(0).getType());
    assertEquals("table7", join.getChild(1).getChild(0).getText());

    assertEquals(SQLParser.JOIN, fromAST.getChild(7).getType());
    join = fromAST.getChild(7);
    assertEquals(SQLParser.NATURAL, join.getChild(0).getType());
    assertEquals("table8", join.getChild(1).getChild(0).getText());

    assertEquals(SQLParser.JOIN, fromAST.getChild(8).getType());
    join = fromAST.getChild(8);
    assertEquals(SQLParser.NATURAL, join.getChild(0).getType());
    assertEquals(SQLParser.INNER, join.getChild(1).getType());
    assertEquals("table9", join.getChild(2).getChild(0).getText());

    assertEquals(SQLParser.JOIN, fromAST.getChild(9).getType());
    join = fromAST.getChild(9);
    assertEquals(SQLParser.NATURAL, join.getChild(0).getType());
    assertEquals(SQLParser.OUTER, join.getChild(1).getType());
    assertEquals(SQLParser.LEFT, join.getChild(1).getChild(0).getType());
    assertEquals("table10", join.getChild(2).getChild(0).getText());

    assertEquals(SQLParser.JOIN, fromAST.getChild(10).getType());
    join = fromAST.getChild(10);
    assertEquals(SQLParser.NATURAL, join.getChild(0).getType());
    assertEquals(SQLParser.OUTER, join.getChild(1).getType());
    assertEquals(SQLParser.RIGHT, join.getChild(1).getChild(0).getType());
    assertEquals("table11", join.getChild(2).getChild(0).getText());

    assertEquals(SQLParser.JOIN, fromAST.getChild(11).getType());
    join = fromAST.getChild(11);
    assertEquals(SQLParser.NATURAL, join.getChild(0).getType());
    assertEquals(SQLParser.OUTER, join.getChild(1).getType());
    assertEquals(SQLParser.FULL, join.getChild(1).getChild(0).getType());
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
    assertEquals(SQLParser.UNION, tree.getType());
    assertEquals(SQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(SQLParser.SELECT, tree.getChild(1).getType());
    
    tree = parseQuery(setClauses[1]);
    assertEquals(SQLParser.UNION, tree.getType());
    assertEquals(SQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(SQLParser.ALL, tree.getChild(1).getType());
    assertEquals(SQLParser.SELECT, tree.getChild(2).getType());
    
    tree = parseQuery(setClauses[2]);
    assertEquals(SQLParser.UNION, tree.getType());
    assertEquals(SQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(SQLParser.DISTINCT, tree.getChild(1).getType());
    assertEquals(SQLParser.SELECT, tree.getChild(2).getType());
    
    tree = parseQuery(setClauses[3]);
    assertEquals(SQLParser.EXCEPT, tree.getType());
    assertEquals(SQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(SQLParser.SELECT, tree.getChild(1).getType());
    
    tree = parseQuery(setClauses[4]);
    assertEquals(SQLParser.EXCEPT, tree.getType());
    assertEquals(SQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(SQLParser.ALL, tree.getChild(1).getType());
    assertEquals(SQLParser.SELECT, tree.getChild(2).getType());
    
    tree = parseQuery(setClauses[5]);
    assertEquals(SQLParser.EXCEPT, tree.getType());
    assertEquals(SQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(SQLParser.DISTINCT, tree.getChild(1).getType());
    assertEquals(SQLParser.SELECT, tree.getChild(2).getType());
    
    tree = parseQuery(setClauses[6]);
    assertEquals(SQLParser.UNION, tree.getType());
    assertEquals(SQLParser.UNION, tree.getChild(0).getType());
    assertEquals(SQLParser.SELECT, tree.getChild(1).getType());
    assertEquals(SQLParser.SELECT, tree.getChild(0).getChild(0).getType());
    assertEquals(SQLParser.SELECT, tree.getChild(0).getChild(1).getType());
    
    tree = parseQuery(setClauses[7]);
    assertEquals(SQLParser.UNION, tree.getType());
    assertEquals(SQLParser.SELECT, tree.getChild(0).getType());
    assertEquals(SQLParser.INTERSECT, tree.getChild(1).getType());
  }

  static String[] schemaStmts = { 
    "drop table abc", // 0
    "create table name (name text, age int)", // 1
    "create table name (name text, age int) using rcfile", // 2
    "create table name (name text, age int) using rcfile with ('rcfile.buffer'=4096)", // 3
    "create table name as select * from test", // 4
    "create table name (name text, age int) as select * from test", // 5
    "create table name (name text, age int) using rcfile as select * from test", // 6
    "create table name (name text, age int) using rcfile with ('rcfile.buffer'= 4096) as select * from test", // 7
    "create table widetable (" +
        "col0 bit,\n" +
        "col0 BIT(10),\n" +
        "col0 bit varying,\n" +
        "col0 bit VARYING(10),\n" +
        "col1 tinyint,\n" +
        "col2 smallInt,\n" +
        "col3 integer,\n" +
        "col4 biginT,\n" +
        "col5 real,\n" +
        "col5 float,\n" +
        "col5 float(53),\n" +
        "col6 double,\n" +
        "col6 doublE precision,\n" +
        "col7 numeric,\n" +
        "col7 numeric(10),\n" +
        "col7 numeric(10,2),\n" +
        "col8 decimal,\n" +
        "col8 decimal(10),\n" +
        "col8 decimal(10,2),\n" +
        "col9 char,\n" +
        "col9 character,\n" +
        "col10 chaR(10),\n" +
        "col10 character(10),\n" +
        "col11 varchar,\n" +
        "col11 character varying,\n" +
        "col12 varchar(255),\n" +
        "col11 character varying (255),\n" +
        "col11 nchar,\n" +
        "col11 nchar(255),\n" +
        "col11 national character,\n" +
        "col11 national character(255),\n" +
        "col11 nvarchar,\n" +
        "col11 nvarchar(255),\n" +
        "col11 natIonal character varying,\n" +
        "col11 national character varying (255),\n" +
        "col11 date,\n" +
        "col11 time,\n" +
        "col11 timetz,\n" +
        "col11 time With time zone,\n" +
        "col11 timesTamptz,\n" +
        "col11 timestamp with time zone,\n" +
        "col11 binary,\n" +
        "col11 binary(10),\n" +
        "col11 varbinary(10),\n" +
        "col11 binary Varying(10),\n" +
        "col11 blOb\n" +
        ") as select * from test", // 8
    "create table widetable (col1 float(10), col2 float) as select * from test", // 9
    "create external table table1 (name text, age int, earn bigint, score float) using csv location '/tmp/data'", // 10
  };

  @Test
  public void testCreateTableAsSelect1() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[1]);
    assertEquals(ast.getType(), SQLParser.CREATE_TABLE);
    assertEquals(SQLParser.Identifier, ast.getChild(0).getType());
    assertEquals(SQLParser.TABLE_DEF, ast.getChild(1).getType());
  }

  @Test
  public void testCreateTableAsSelect2() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[2]);
    assertEquals(ast.getType(), SQLParser.CREATE_TABLE);
    assertEquals(SQLParser.Identifier, ast.getChild(0).getType());
    assertEquals(SQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(SQLParser.USING, ast.getChild(2).getType());
    assertEquals("rcfile", ast.getChild(2).getChild(0).getText());
  }

  @Test
  public void testCreateTableAsSelect3() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[3]);
    assertEquals(ast.getType(), SQLParser.CREATE_TABLE);
    assertEquals(SQLParser.Identifier, ast.getChild(0).getType());
    assertEquals(SQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(SQLParser.USING, ast.getChild(2).getType());
    assertEquals("rcfile", ast.getChild(2).getChild(0).getText());
    assertEquals(SQLParser.PARAMS, ast.getChild(3).getType());
  }

  @Test
  public void testCreateTableAsSelect4() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[4]);
    assertEquals(ast.getType(), SQLParser.CREATE_TABLE);
    assertEquals(SQLParser.Identifier, ast.getChild(0).getType());
    assertEquals(SQLParser.AS, ast.getChild(1).getType());
    assertEquals(SQLParser.SELECT, ast.getChild(1).getChild(0).getType());
  }

  @Test
  public void testCreateTableAsSelect5() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[5]);
    assertEquals(ast.getType(), SQLParser.CREATE_TABLE);
    assertEquals(SQLParser.Identifier, ast.getChild(0).getType());
    assertEquals(SQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(SQLParser.AS, ast.getChild(2).getType());
    assertEquals(SQLParser.SELECT, ast.getChild(2).getChild(0).getType());
  }

  @Test
  public void testCreateTableAsSelect6() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[6]);
    assertEquals(ast.getType(), SQLParser.CREATE_TABLE);
    assertEquals(SQLParser.Identifier, ast.getChild(0).getType());
    assertEquals(SQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(SQLParser.USING, ast.getChild(2).getType());
    assertEquals(SQLParser.AS, ast.getChild(3).getType());
    assertEquals(SQLParser.SELECT, ast.getChild(3).getChild(0).getType());
  }

  @Test
  public void testCreateTableAsSelect7() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[7]);
    assertEquals(ast.getType(), SQLParser.CREATE_TABLE);
    assertEquals(SQLParser.Identifier, ast.getChild(0).getType());
    assertEquals(SQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(SQLParser.USING, ast.getChild(2).getType());
    assertEquals(SQLParser.PARAMS, ast.getChild(3).getType());
    assertEquals(SQLParser.AS, ast.getChild(4).getType());
    assertEquals(SQLParser.SELECT, ast.getChild(4).getChild(0).getType());
  }

  @Test
  public void testCreateTableWithVariousDataType1() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[8]);
    assertEquals(ast.getType(), SQLParser.CREATE_TABLE);
    assertEquals(SQLParser.Identifier, ast.getChild(0).getType());
    assertEquals(SQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals(SQLParser.AS, ast.getChild(2).getType());
    assertEquals(SQLParser.SELECT, ast.getChild(2).getChild(0).getType());
  }

  @Test
  public void testCreateTableWithVariousDataType2() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[9]);
    assertEquals(ast.getType(), SQLParser.CREATE_TABLE);
    assertEquals(SQLParser.Identifier, ast.getChild(0).getType());
    assertEquals(SQLParser.TABLE_DEF, ast.getChild(1).getType());
    assertEquals("10", ast.getChild(1).getChild(0).getChild(1).getChild(0).getText());
    assertEquals(SQLParser.AS, ast.getChild(2).getType());
    assertEquals(SQLParser.SELECT, ast.getChild(2).getChild(0).getType());
  }
  
  @Test
  public void testCreateTableLocation1() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[10]);
    assertEquals(ast.getType(), SQLParser.CREATE_TABLE);
    assertEquals(SQLParser.Identifier, ast.getChild(0).getType());
    assertEquals(SQLParser.EXTERNAL, ast.getChild(1).getType());
    assertEquals(SQLParser.TABLE_DEF, ast.getChild(2).getType());
    assertEquals(SQLParser.USING, ast.getChild(3).getType());
    assertEquals(SQLParser.LOCATION, ast.getChild(4).getType());
    assertEquals("/tmp/data", ast.getChild(4).getChild(0).getText());
  }

  @Test
  public void testDropTable() throws RecognitionException, TQLSyntaxError {
    Tree ast = parseQuery(schemaStmts[0]);
    assertEquals(ast.getType(), SQLParser.DROP_TABLE);
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

  public static SQLParser parseExpr(String expr) {
    ANTLRStringStream input = new ANTLRStringStream(expr);
    SQLLexer lexer = new SQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    return new SQLParser(tokens);
  }

  @Test
  public void testArithEvalTree() throws RecognitionException {
    SQLParser p = parseExpr(exprs[0]);
    CommonTree node = (CommonTree) p.numeric_value_expression().getTree();

    assertEquals(node.getText(), "+");
    assertEquals(node.getChild(0).getText(), "1");
    assertEquals(node.getChild(1).getText(), "2");

    p = parseExpr(exprs[1]);
    node = (CommonTree) p.numeric_value_expression().getTree();
    assertEquals(node.getText(), "-");
    assertEquals(node.getChild(0).getText(), "3");
    assertEquals(node.getChild(1).getText(), "4");

    p = parseExpr(exprs[2]);
    node = (CommonTree) p.numeric_value_expression().getTree();
    assertEquals(node.getText(), "*");
    assertEquals(node.getChild(0).getText(), "5");
    assertEquals(node.getChild(1).getText(), "6");

    p = parseExpr(exprs[3]);
    node = (CommonTree) p.numeric_value_expression().getTree();
    assertEquals(node.getText(), "/");
    assertEquals(node.getChild(0).getText(), "7");
    assertEquals(node.getChild(1).getText(), "8");

    p = parseExpr(exprs[4]);
    node = (CommonTree) p.numeric_value_expression().getTree();
    assertEquals(node.getText(), "%");
    assertEquals(node.getChild(0).getText(), "10");
    assertEquals(node.getChild(1).getText(), "2");
  }

  @Test
  public void testCompEvalTree() throws RecognitionException {
    SQLParser p = parseExpr(exprs[5]);
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
    SQLParser p = parseExpr(exprs[10]);
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
    SQLParser p = parseExpr(exprs[12]);
    CommonTree node = (CommonTree) p.in_predicate().getTree();
    assertEquals(node.getType(), SQLParser.IN);
    assertEquals(node.getChild(1).getText(), "male");
    assertEquals(node.getChild(2).getText(), "female");

    p = parseExpr(exprs[13]);
    node = (CommonTree) p.in_predicate().getTree();
    assertEquals(node.getType(), SQLParser.IN);
    assertEquals(node.getChild(0).getType(), SQLParser.FIELD_NAME);
    FieldName fieldName = new FieldName(node.getChild(0));
    assertEquals(fieldName.getName(), "gender");
    assertEquals(node.getChild(1).getText(), "male");
    assertEquals(node.getChild(2).getText(), "female");
    assertEquals(node.getChild(3).getType(), SQLParser.NOT);
  }

  @Test
  public void testAndEvalTree() throws RecognitionException {
    SQLParser p = parseExpr(exprs[14]);
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
    SQLParser p = parseExpr(exprs[16]);
    CommonTree node = (CommonTree) p.boolean_value_expression().getTree();
    assertEquals(node.getText(), "or");
    assertEquals(node.getChild(0).getText(), ">");
    assertEquals(node.getChild(1).getText(), "<");

    p = parseExpr(exprs[17]);
    node = (CommonTree) p.boolean_value_expression().getTree();
    assertEquals(node.getText(), "or");
    assertEquals(node.getChild(0).getText(), ">");
    assertEquals(node.getChild(1).getText(), "and");
  }

  @Test
  public void testComplexEvalTree() throws RecognitionException {
    SQLParser p = parseExpr(exprs[18]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(node.getText(), "and");
    assertEquals(node.getChild(0).getText(), "or");
    assertEquals(node.getChild(1).getText(), "and");
  }
  
  @Test
  public void testNotEvalTree() throws RecognitionException {
    SQLParser p = parseExpr(exprs[23]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(node.getType(), SQLParser.NOT);
    assertEquals(node.getChild(0).getType(), SQLParser.GTH);
    CommonTree gth = (CommonTree) node.getChild(0);
    assertEquals(gth.getChild(0).getType(), SQLParser.NUMBER);
    assertEquals(gth.getChild(1).getType(), SQLParser.NUMBER);
  }

  @Test
  public void testFuncCallEvalTree() throws RecognitionException {
    SQLParser p = parseExpr(exprs[21]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(node.getType(), SQLParser.FUNCTION);
    assertEquals(node.getText(), "sum");
    assertEquals(node.getChild(0).getType(), SQLParser.FIELD_NAME);
    FieldName fieldName = new FieldName(node.getChild(0));
    assertEquals(fieldName.getName(), "age");

    p = parseExpr(exprs[22]);
    node = (CommonTree) p.search_condition().getTree();
    assertEquals(node.getType(), SQLParser.FUNCTION);
    assertEquals(node.getText(), "now");
    assertNull(node.getChild(1));
  }
  
  @Test
  public void testLikeEvalTree() throws RecognitionException {
    SQLParser p = parseExpr(exprs[24]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(SQLParser.LIKE, node.getType());
    assertEquals(SQLParser.FIELD_NAME, node.getChild(0).getType());
    FieldName fieldName = new FieldName(node.getChild(0));
    assertEquals(fieldName.getName(), "type");
    assertEquals(SQLParser.Character_String_Literal, node.getChild(1).getType());
    
    p = parseExpr(exprs[25]);
    node = (CommonTree) p.search_condition().getTree();    
    assertEquals(SQLParser.NOT, node.getChild(0).getType());
    assertEquals(SQLParser.LIKE, node.getType());
    assertEquals(SQLParser.FIELD_NAME, node.getChild(1).getType());
    fieldName = new FieldName(node.getChild(1));
    assertEquals(fieldName.getName(), "type");
    assertEquals(SQLParser.Character_String_Literal, node.getChild(2).getType());
  }

  @Test
  /**
   * TODO - needs more tests
   */
  public void testConstEval() throws RecognitionException {
    SQLParser p = parseExpr(exprs[26]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(SQLParser.EQUAL, node.getType());
    assertEquals(SQLParser.FIELD_NAME, node.getChild(0).getType());
    assertEquals(SQLParser.Character_String_Literal, node.getChild(1).getType());
  }

  @Test
  public void testIsNull() throws RecognitionException {
    SQLParser p = parseExpr(exprs[27]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(SQLParser.IS, node.getType());
    assertEquals(SQLParser.FIELD_NAME, node.getChild(0).getType());
    assertEquals(SQLParser.NULL, node.getChild(1).getType());
  }

  @Test
  public void testIsNotNull() throws RecognitionException {
    SQLParser p = parseExpr(exprs[28]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(SQLParser.IS, node.getType());
    assertEquals(SQLParser.FIELD_NAME, node.getChild(0).getType());
    assertEquals(SQLParser.NULL, node.getChild(1).getType());
    assertEquals(SQLParser.NOT, node.getChild(2).getType());
  }

  @Test
  public void testEqualNull() throws RecognitionException {
    SQLParser p = parseExpr(exprs[29]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(SQLParser.EQUAL, node.getType());
    assertEquals(SQLParser.FIELD_NAME, node.getChild(0).getType());
    assertEquals(SQLParser.NULL, node.getChild(1).getType());
  }

  @Test
  public void testNotEqualNull() throws RecognitionException {
    SQLParser p = parseExpr(exprs[30]);
    CommonTree node = (CommonTree) p.search_condition().getTree();
    assertEquals(SQLParser.NOT_EQUAL, node.getType());
    assertEquals(SQLParser.FIELD_NAME, node.getChild(0).getType());
    assertEquals(SQLParser.NULL, node.getChild(1).getType());
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
    SQLParser p = parseExpr(caseStatements[0]);
    CommonTree node = (CommonTree) p.statement().getTree();

    assertEquals(SQLParser.SEL_LIST, node.getChild(1).getType());
    CommonTree selList = (CommonTree) node.getChild(1);
    assertEquals(1, selList.getChildCount());
    assertEquals(SQLParser.COLUMN, selList.getChild(0).getType());

    CommonTree column = (CommonTree) selList.getChild(0);
    assertEquals(SQLParser.CASE, column.getChild(0).getType());
    CommonTree caseStatement = (CommonTree) column.getChild(0);

    assertEquals(3, caseStatement.getChildCount());
    assertEquals(SQLParser.WHEN, caseStatement.getChild(0).getType());
    assertEquals(SQLParser.WHEN, caseStatement.getChild(1).getType());
    assertEquals(SQLParser.ELSE, caseStatement.getChild(2).getType());

    CommonTree cond1 = (CommonTree) caseStatement.getChild(0);
    CommonTree cond2 = (CommonTree) caseStatement.getChild(1);
    CommonTree elseStmt = (CommonTree) caseStatement.getChild(2);

    assertEquals(SQLParser.LIKE, cond1.getChild(0).getType());
    assertEquals(SQLParser.EQUAL, cond2.getChild(0).getType());
    assertEquals(SQLParser.NUMBER, elseStmt.getChild(0).getType());
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