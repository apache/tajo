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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.engine.parser.SQLParser.Boolean_value_expressionContext;
import org.apache.tajo.engine.parser.SQLParser.SqlContext;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestHiveConverter {
  private static final Log LOG = LogFactory.getLog(TestHiveConverter.class.getName());

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

  public static Expr parseHiveQL(String sql) {
    HiveConverter converter = new HiveConverter();
    return converter.parse(sql);
  }

  public static String getMethodName(int depth) {
    final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
    return ste[depth].getMethodName();
  }

  public static void compareJsonResult(Expr expr, Expr hiveExpr) throws IOException {
    if (expr != null && hiveExpr != null) {
      if (!expr.toJson().equals(hiveExpr.toJson())) {
        LOG.info("### Tajo Parse Result ### \n" + expr.toJson());
        LOG.info("### Hive Parse Result ### \n" + hiveExpr.toJson());
        throw new IOException(getMethodName(3));
      }
    } else {
      LOG.info("### Tajo Parse Result ### \n" + expr.toJson());
      LOG.info("### Hive Parse Result ### \n" + hiveExpr.toJson());
      throw new IOException(getMethodName(3));
    }
  }

  @Test
  public void testSelect1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_1.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testSelect3() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_3.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testSelect4() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_4.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testSelect5() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_5.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testSelect7() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_7.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testSelect8() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_8.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testSelect9() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_9.sql"));
    Expr expr = parseQuery(sql);
    sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_9.hiveql"));
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testSelect10() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_10.sql"));
    Expr expr = parseQuery(sql);
    sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_10.hiveql"));
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  //@Test
  public void testSelect11() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_11.sql"));
    Expr expr = parseQuery(sql);
    sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_11.hiveql"));
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testSelect12() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_12.hiveql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testSelect13() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_13.sql"));
    Expr expr = parseQuery(sql);
    sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_13.hiveql"));
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testSelect14() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/select_14.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testGroupby1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/groupby_1.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testJoin2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/join_2.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testJoin5() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/join_5.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testJoin6() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/join_6.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testJoin7() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/join_7.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  //@Test
  public void testJoin9() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/join_9.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    assertEquals(expr, hiveExpr);
  }

  @Test
  public void testJoin12() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/join_12.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    assertEquals(expr, hiveExpr);
  }

  @Test
  public void testJoin13() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/join_13.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testJoin14() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/join_14.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testJoin15() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/join_15.sql"));
    Expr expr = parseQuery(sql);
    sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/join_15.hiveql"));
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testUnion1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/union_1.hiveql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testInsert1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/insert_into_select_1.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);

  }

  @Test
  public void testInsert2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/insert_overwrite_into_select_2.sql"));
    Expr expr = parseQuery(sql);
    sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/insert_overwrite_into_select_2.hiveql"));
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testCreate1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/create_table_1.sql"));
    Expr expr = parseQuery(sql);
    sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/create_table_1.hiveql"));
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testCreate2() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/create_table_2.sql"));
    Expr expr = parseQuery(sql);
    sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/create_table_2.hiveql"));
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testCreate11() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/create_table_11.sql"));
    Expr expr = parseQuery(sql);
    sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/create_table_11.hiveql"));
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testCreate12() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/create_table_12.sql"));
    Expr expr = parseQuery(sql);
    sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/create_table_12.hiveql"));
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }

  @Test
  public void testDrop() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/drop_table.sql"));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(sql);
    compareJsonResult(expr, hiveExpr);
  }
}
