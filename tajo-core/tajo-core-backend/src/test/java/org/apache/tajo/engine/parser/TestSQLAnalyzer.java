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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.engine.parser.SQLParser.Boolean_value_expressionContext;
import org.apache.tajo.engine.parser.SQLParser.SqlContext;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * This unit tests uses a number of query files located in tajo/tajo-core/tajo-core-backend/test/queries.
 * So, you must set tajo/tajo-core/tajo-core-backend as the working directory.
 */
public class TestSQLAnalyzer {

  public static Expr parseQuery(String sql) {
    ANTLRInputStream input = new ANTLRInputStream(sql);
    SQLLexer lexer = new SQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    parser.setBuildParseTree(true);
    SQLAnalyzer visitor = new SQLAnalyzer();
    SqlContext context = parser.sql();
    return visitor.visitSql(context);
  }


  @Test
  public void testSelect1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/select_1.sql"));
    parseQuery(sql);
  }

  @Test
  public void testSelect2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/select_2.sql"));
    parseQuery(sql);
  }

  @Test
  public void testSelect3() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/select_3.sql"));
    parseQuery(sql);
  }

  @Test
  public void testSelect4() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/select_4.sql"));
    parseQuery(sql);
  }

  @Test
  public void testSelect5() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/select_5.sql"));
    parseQuery(sql);
  }

  @Test
  public void testGroupby1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/groupby_1.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_1.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_2.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin3() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_3.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin4() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_4.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin5() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_5.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin6() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_6.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin7() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_7.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin8() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_8.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin9() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_9.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin10() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_10.sql"));
    parseQuery(sql);
  }

  @Test
  public void testJoin11() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/join_11.sql"));
    parseQuery(sql);
  }

  @Test
  public void testSet1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/set_1.sql"));
    parseQuery(sql);
  }

  @Test
  public void testSet2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/set_2.sql"));
    parseQuery(sql);
  }

  @Test
  public void testSet3() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/set_3.sql"));
    parseQuery(sql);
  }

  @Test
  public void testSet4() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/set_4.sql"));
    System.out.println(parseQuery(sql));
  }

  @Test
  public void testDropTable() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/drop_table.sql"));
    parseQuery(sql);
  }

  @Test
  public void testCreateTable1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/create_table_1.sql"));
    parseQuery(sql);
  }

  @Test
  public void testCreateTable2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/create_table_2.sql"));
    parseQuery(sql);
  }

  @Test
  public void testCreateTable3() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/create_table_3.sql"));
    parseQuery(sql);
  }

  @Test
  public void testCreateTable4() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/create_table_4.sql"));
    parseQuery(sql);
  }

  @Test
  public void testCreateTable5() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/create_table_5.sql"));
    parseQuery(sql);
  }

  @Test
  public void testCreateTable6() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/create_table_6.sql"));
    parseQuery(sql);
  }

  @Test
  public void testCreateTable7() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/create_table_7.sql"));
    parseQuery(sql);
  }

  @Test
  public void testCreateTable8() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/create_table_8.sql"));
    parseQuery(sql);
  }

  @Test
  public void testCreateTable9() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/create_table_9.sql"));
    parseQuery(sql);
  }

  @Test
  public void testCreateTable10() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/create_table_10.sql"));
    parseQuery(sql);
  }

  @Test
  public void testTableSubQuery1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/table_subquery1.sql"));
    parseQuery(sql);
  }

  @Test
  public void testTableSubQuery2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/table_subquery2.sql"));
    parseQuery(sql);
  }

  @Test
  public void testInSubquery1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/in_subquery_1.sql"));
    parseQuery(sql);
  }

  @Test
  public void testInSubquery2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/in_subquery_2.sql"));
    parseQuery(sql);
  }

  @Test
  public void testExistsPredicate1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/exists_predicate_1.sql"));
    parseQuery(sql);
  }

  @Test
  public void testExistsPredicate2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/exists_predicate_2.sql"));
    parseQuery(sql);
  }

  @Test
  public void testInsertIntoTable() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/insert_into_select_1.sql"));
    parseQuery(sql);
  }

  @Test
  public void testInsertIntoLocation() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/insert_into_select_2.sql"));
    parseQuery(sql);
  }

  @Test
  public void testInsertIntoTable2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/insert_into_select_3.sql"));
    parseQuery(sql);
  }

  @Test
  public void testInsertOverwriteIntoTable() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/insert_overwrite_into_select_1.sql"));
    parseQuery(sql);
  }

  @Test
  public void testInsertOverwriteIntoLocation() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/insert_overwrite_into_select_2.sql"));
    parseQuery(sql);
  }

  @Test
  public void testInsertOverwriteIntoTable2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/queries/insert_overwrite_into_select_3.sql"));
    parseQuery(sql);
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
      "gender in ('male', 'female')", // 10
      "gender not in ('male', 'female')", // 11
      "score > 90 and age < 20", // 12
      "score > 90 and age < 20 and name != 'hyunsik'", // 13
      "score > 90 or age < 20", // 14
      "score > 90 or age < 20 and name != 'hyunsik'", // 15
      "((a+3 > 1) or 1=1) and (3 != (abc + 4) and type in (3,4))", // 16
      "3", // 17
      "1.2", // 18
      "sum(age)", // 19
      "now()", // 20
      "not (90 > 100)", // 21
      "type like '%top'", // 22
      "type not like 'top%'", // 23
      "col = 'value'", // 24
      "col is null", // 25
      "col is not null", // 26
      "col = null", // 27
      "col != null" // 38
  };

  public static Expr parseExpr(String sql) {
    ANTLRInputStream input = new ANTLRInputStream(sql);
    SQLLexer lexer = new SQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    parser.setBuildParseTree(true);
    SQLAnalyzer visitor = new SQLAnalyzer();
    Boolean_value_expressionContext context = parser.boolean_value_expression();
    return visitor.visitBoolean_value_expression(context);
  }

  @Test
  public void testExprs() {
    for (int i = 0; i < exprs.length; i++) {
      parseExpr(exprs[i]);
    }
  }
}
