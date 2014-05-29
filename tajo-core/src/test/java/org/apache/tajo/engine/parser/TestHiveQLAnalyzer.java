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

import com.google.common.base.Preconditions;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.engine.parser.SQLParser.SqlContext;
import org.apache.tajo.algebra.CreateTable;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestHiveQLAnalyzer {
  private static final Log LOG = LogFactory.getLog(TestHiveQLAnalyzer.class.getName());
  protected static final String BASE_PATH = "src/test/resources/queries/default/";

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
    HiveQLAnalyzer converter = new HiveQLAnalyzer();
    return converter.parse(sql);
  }

  public static String getMethodName(int depth) {
    final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
    return ste[depth].getMethodName();
  }

  public static void compareJsonResult(String sqlPath) throws IOException {
      Preconditions.checkNotNull(sqlPath);
      compareJsonResult(sqlPath, sqlPath);
  }

  public static void compareJsonResult(String sqlPath, String hiveqlPath) throws IOException {
    Preconditions.checkNotNull(sqlPath, hiveqlPath);
    String sql = FileUtil.readTextFile(new File(BASE_PATH + sqlPath));
    String hiveQL = FileUtil.readTextFile(new File(BASE_PATH + hiveqlPath));
    Expr expr = parseQuery(sql);
    Expr hiveExpr = parseHiveQL(hiveQL);
    assertEquals(expr.toJson(), hiveExpr.toJson());
  }

  @Test
  public void testSelect1() throws IOException {
    compareJsonResult("select_1.sql");
  }

  @Test
  public void testSelect3() throws IOException {
    compareJsonResult("select_3.sql");
  }

  @Test
  public void testSelect4() throws IOException {
    compareJsonResult("select_4.sql");
  }

  @Test
  public void testSelect5() throws IOException {
    compareJsonResult("select_5.sql");
  }

  @Test
  public void testSelect7() throws IOException {
    compareJsonResult("select_7.sql");
  }

  @Test
  public void testSelect8() throws IOException {
    compareJsonResult("select_8.sql");
  }

  @Test
  public void testSelect9() throws IOException {
    compareJsonResult("select_9.sql", "select_9.hiveql");
  }

  @Test
  public void testSelect10() throws IOException {
    compareJsonResult("select_10.sql", "select_10.hiveql");
  }

  //TODO: support beween condition
  //@Test
//  public void testSelect11() throws IOException {
//    compareJsonResult("select_11.sql", "select_11.hiveql");
//  }

  @Test
  public void testSelect12() throws IOException {
    compareJsonResult("select_12.hiveql");
  }

  @Test
  public void testSelect13() throws IOException {
    compareJsonResult("select_13.sql", "select_13.hiveql");
  }

  @Test
  public void testSelect14() throws IOException {
    compareJsonResult("select_14.sql");
  }

  @Test
  public void testSelect15() throws IOException {
    compareJsonResult("select_15.sql", "select_15.hiveql");
  }

  @Test
  public void testAsterisk1() throws IOException {
    compareJsonResult("asterisk_1.sql");
  }

  @Test
  public void testAsterisk2() throws IOException {
    compareJsonResult("asterisk_2.sql");
  }

  @Test
  public void testAsterisk3() throws IOException {
    compareJsonResult("asterisk_3.sql");
  }

  @Test
  public void testAsterisk4() throws IOException {
    compareJsonResult("asterisk_4.sql");
  }

  @Test
  public void testGroupby1() throws IOException {
    compareJsonResult("groupby_1.sql");
  }

  @Test
  public void testGroupby2() throws IOException {
    compareJsonResult("groupby_2.sql");
  }

  @Test
  public void testGroupby3() throws IOException {
    compareJsonResult("groupby_3.sql");
  }

  @Test
  public void testGroupby4() throws IOException {
    compareJsonResult("groupby_4.sql");
  }

  @Test
  public void testGroupby5() throws IOException {
    compareJsonResult("groupby_5.sql");
  }

  @Test
  public void testJoin2() throws IOException {
    compareJsonResult("join_2.sql");
  }

  @Test
  public void testJoin5() throws IOException {
    compareJsonResult("join_5.sql");
  }

  @Test
  public void testJoin6() throws IOException {
    compareJsonResult("join_6.sql");
  }

  @Test
  public void testJoin7() throws IOException {
    compareJsonResult("join_7.sql");
  }

    //TODO: support complex join conditions
    //@Test
//  public void testJoin9() throws IOException {
//    compareJsonResult("join_9.sql");
//  }

  @Test
  public void testJoin12() throws IOException {
    compareJsonResult("join_12.sql");
  }

  @Test
  public void testJoin13() throws IOException {
    compareJsonResult("join_13.sql");
  }

  @Test
  public void testJoin14() throws IOException {
    compareJsonResult("join_14.sql");
  }

  @Test
  public void testJoin15() throws IOException {
    compareJsonResult("join_15.sql", "join_15.hiveql");
  }

  @Test
  public void testUnion1() throws IOException {
    compareJsonResult("union_1.hiveql");
  }

  @Test
  public void testInsert1() throws IOException {
    compareJsonResult("insert_into_select_1.sql");
  }

  @Test
  public void testInsert2() throws IOException {
    compareJsonResult("insert_overwrite_into_select_2.sql", "insert_overwrite_into_select_2.hiveql");
  }

  @Test
  public void testCreate1() throws IOException {
    compareJsonResult("create_table_1.sql", "create_table_1.hiveql");
  }

  @Test
  public void testCreate2() throws IOException {
    compareJsonResult("create_table_2.sql", "create_table_2.hiveql");
  }

  @Test
  public void testCreate11() throws IOException {
    compareJsonResult("create_table_11.sql", "create_table_11.hiveql");
  }

  @Test
  public void testCreate12() throws IOException {
    compareJsonResult("create_table_12.sql", "create_table_12.hiveql");
  }

  @Test
  public void testDrop() throws IOException {
    compareJsonResult("drop_table.sql");
  }

  @Test
  public void testCreateTableLike1() throws IOException {
    String sql = FileUtil.readTextFile(new File("src/test/resources/queries/default/create_table_like_1.sql"));
    Expr expr = parseQuery(sql);
    assertEquals(OpType.CreateTable, expr.getType());
    CreateTable createTable = (CreateTable) expr;
    assertEquals("orig_name", createTable.getLikeParentTableName());
  }

}
