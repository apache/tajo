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
import org.apache.tajo.algebra.*;
import org.apache.tajo.engine.parser.SQLParser.SqlContext;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This unit tests uses a number of query files located in tajo/tajo-core/queries.
 * So, you must set tajo/tajo-core/ as the working directory.
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

  public void assertParseResult(String sqlFileName, String resultFileName) throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/TestSQLAnalyzer/" + sqlFileName);
    String result = FileUtil.readTextFileFromResource("results/TestSQLAnalyzer/" + resultFileName);

    Expr expr = parseQuery(sql);
    assertEquals(result.trim(), expr.toJson().trim());
  }


  @Test
  public void testSelect1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/select_1.sql");
    parseQuery(sql);
  }

  @Test
  public void testSelect2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/select_2.sql");
    parseQuery(sql);
  }

  @Test
  public void testSelect3() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/select_3.sql");
    parseQuery(sql);
  }

  @Test
  public void testSelect4() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/select_4.sql");
    parseQuery(sql);
  }

  @Test
  public void testSelect5() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/select_5.sql");
    parseQuery(sql);
  }

  @Test
  public void testAsterisk1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/asterisk_1.sql");
    parseQuery(sql);
  }

  @Test
  public void testAsterisk2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/asterisk_2.sql");
    parseQuery(sql);
  }

  @Test
  public void testAsterisk3() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/asterisk_3.sql");
    parseQuery(sql);
  }

  @Test
  public void testAsterisk4() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/asterisk_4.sql");
    parseQuery(sql);
  }

  @Test
  public void testGroupby1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/groupby_1.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_1.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_2.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin3() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_3.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin4() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_4.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin5() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_5.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin6() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_6.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin7() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_7.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin8() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_8.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin9() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_9.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin10() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_10.sql");
    parseQuery(sql);
  }

  @Test
  public void testJoin11() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/join_11.sql");
    parseQuery(sql);
  }

  @Test
  public void testSet1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/set_1.sql");
    parseQuery(sql);
  }

  @Test
  public void testSet2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/set_2.sql");
    parseQuery(sql);
  }

  @Test
  public void testSet3() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/set_3.sql");
    parseQuery(sql);
  }

  @Test
  public void testSet4() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/set_4.sql");
    parseQuery(sql);
  }

  @Test
  public void testDropTable() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/drop_table.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTable1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_1.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTable2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_2.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTable3() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_3.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTable4() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_4.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTable5() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_5.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTable6() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_6.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTable7() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_7.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTable8() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_8.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTable9() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_9.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTable10() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_10.sql");
    parseQuery(sql);
  }

  @Test
  public void testCreateTableLike1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_like_1.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.CreateTable, expr.getType());
    CreateTable createTable = (CreateTable) expr;
    assertEquals("orig_name", createTable.getLikeParentTableName());
  }

  @Test
  public void testCreateTablePartitionByHash1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_partition_by_hash_1.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.CreateTable, expr.getType());
    CreateTable createTable = (CreateTable) expr;
    assertTrue(createTable.hasPartition());
    assertEquals(CreateTable.PartitionType.HASH, createTable.getPartitionMethod().getPartitionType());
    CreateTable.HashPartition hashPartition = createTable.getPartitionMethod();
    assertEquals("col1", hashPartition.getColumns()[0].getCanonicalName());
    assertTrue(hashPartition.hasQuantifier());
  }

  @Test
  public void testCreateTablePartitionByHash2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_partition_by_hash_2.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.CreateTable, expr.getType());
    CreateTable createTable = (CreateTable) expr;
    assertTrue(createTable.hasPartition());
    assertEquals(CreateTable.PartitionType.HASH, createTable.getPartitionMethod().getPartitionType());
    CreateTable.HashPartition hashPartition = createTable.getPartitionMethod();
    assertEquals("col1", hashPartition.getColumns()[0].getCanonicalName());
    assertTrue(hashPartition.hasSpecifiers());
    assertEquals(3, hashPartition.getSpecifiers().size());
  }

  @Test
  public void testCreateTablePartitionByRange() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_partition_by_range.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.CreateTable, expr.getType());
    CreateTable createTable = (CreateTable) expr;
    assertTrue(createTable.hasPartition());
    assertEquals(CreateTable.PartitionType.RANGE, createTable.getPartitionMethod().getPartitionType());
    CreateTable.RangePartition rangePartition = createTable.getPartitionMethod();
    assertEquals("col1", rangePartition.getColumns()[0].getCanonicalName());
    assertEquals(3, rangePartition.getSpecifiers().size());
  }

  @Test
  public void testCreateTablePartitionByList() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_partition_by_list.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.CreateTable, expr.getType());
    CreateTable createTable = (CreateTable) expr;
    assertTrue(createTable.hasPartition());
    assertEquals(CreateTable.PartitionType.LIST, createTable.getPartitionMethod().getPartitionType());
    CreateTable.ListPartition listPartition = createTable.getPartitionMethod();
    assertEquals("col1", listPartition.getColumns()[0].getCanonicalName());
    assertEquals(2, listPartition.getSpecifiers().size());
    Iterator<CreateTable.ListPartitionSpecifier> iterator = listPartition.getSpecifiers().iterator();
    CreateTable.ListPartitionSpecifier specifier = iterator.next();
    LiteralValue value1 = (LiteralValue) specifier.getValueList().getValues()[0];
    LiteralValue value2 = (LiteralValue) specifier.getValueList().getValues()[1];
    assertEquals("Seoul", value1.getValue());
    assertEquals("서울", value2.getValue());

    specifier = iterator.next();
    value1 = (LiteralValue) specifier.getValueList().getValues()[0];
    value2 = (LiteralValue) specifier.getValueList().getValues()[1];
    assertEquals("Busan", value1.getValue());
    assertEquals("부산", value2.getValue());
  }

  @Test
  public void testCreateTablePartitionByColumn() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/create_table_partition_by_column.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.CreateTable, expr.getType());
    CreateTable createTable = (CreateTable) expr;
    assertTrue(createTable.hasPartition());
    assertEquals(CreateTable.PartitionType.COLUMN, createTable.getPartitionMethod().getPartitionType());
    CreateTable.ColumnPartition columnPartition = createTable.getPartitionMethod();
    assertEquals(3, columnPartition.getColumns().length);
    assertEquals("col3", columnPartition.getColumns()[0].getColumnName());
    assertEquals("col4", columnPartition.getColumns()[1].getColumnName());
    assertEquals("col5", columnPartition.getColumns()[2].getColumnName());
  }

  @Test
  public void testAlterTableAddPartition1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/alter_table_add_partition_1.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.AlterTable, expr.getType());
    AlterTable alterTable = (AlterTable)expr;
    assertEquals(alterTable.getAlterTableOpType(), AlterTableOpType.ADD_PARTITION);
    assertEquals(2, alterTable.getColumns().length);
    assertEquals(2, alterTable.getValues().length);
    assertEquals("col1", alterTable.getColumns()[0].getName());
    assertEquals("col2", alterTable.getColumns()[1].getName());
    LiteralValue value1 = (LiteralValue)alterTable.getValues()[0];
    assertEquals("1", value1.getValue());
    LiteralValue value2 = (LiteralValue)alterTable.getValues()[1];
    assertEquals("2", value2.getValue());
  }

  @Test
  public void testAlterTableAddPartition2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/alter_table_add_partition_2.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.AlterTable, expr.getType());
    AlterTable alterTable = (AlterTable)expr;
    assertEquals(alterTable.getAlterTableOpType(), AlterTableOpType.ADD_PARTITION);
    assertEquals(2, alterTable.getColumns().length);
    assertEquals(2, alterTable.getValues().length);
    assertEquals("col1", alterTable.getColumns()[0].getName());
    assertEquals("col2", alterTable.getColumns()[1].getName());
    LiteralValue value1 = (LiteralValue)alterTable.getValues()[0];
    assertEquals("1", value1.getValue());
    LiteralValue value2 = (LiteralValue)alterTable.getValues()[1];
    assertEquals("2", value2.getValue());
    assertEquals(alterTable.getLocation(), "hdfs://xxx.com/warehouse/table1/col1=1/col2=2");
  }

  @Test
  public void testAlterTableAddPartition3() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/alter_table_add_partition_3.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.AlterTable, expr.getType());
    AlterTable alterTable = (AlterTable)expr;
    assertEquals(alterTable.getAlterTableOpType(), AlterTableOpType.ADD_PARTITION);
    assertEquals(3, alterTable.getColumns().length);
    assertEquals(3, alterTable.getValues().length);
    assertEquals("col1", alterTable.getColumns()[0].getName());
    assertEquals("col2", alterTable.getColumns()[1].getName());
    assertEquals("col3", alterTable.getColumns()[2].getName());
    LiteralValue value1 = (LiteralValue)alterTable.getValues()[0];
    assertEquals("2015", value1.getValue());
    LiteralValue value2 = (LiteralValue)alterTable.getValues()[1];
    assertEquals("01", value2.getValue());
    LiteralValue value3 = (LiteralValue)alterTable.getValues()[2];
    assertEquals("11", value3.getValue());
    assertEquals(alterTable.getLocation(), "hdfs://xxx.com/warehouse/table1/col1=2015/col2=01/col3=11");
  }

  @Test
  public void testAlterTableAddPartition4() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/alter_table_add_partition_4.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.AlterTable, expr.getType());
    AlterTable alterTable = (AlterTable)expr;
    assertEquals(alterTable.getAlterTableOpType(), AlterTableOpType.ADD_PARTITION);
    assertEquals(1, alterTable.getColumns().length);
    assertEquals(1, alterTable.getValues().length);
    assertEquals("col1", alterTable.getColumns()[0].getName());
    LiteralValue value1 = (LiteralValue)alterTable.getValues()[0];
    assertEquals("TAJO", value1.getValue());
  }

  @Test
  public void testAlterTableDropPartition1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/alter_table_drop_partition_1.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.AlterTable, expr.getType());
    AlterTable alterTable = (AlterTable)expr;
    assertEquals(alterTable.getAlterTableOpType(), AlterTableOpType.DROP_PARTITION);
    assertEquals(2, alterTable.getColumns().length);
    assertEquals(2, alterTable.getValues().length);
    assertEquals("col1", alterTable.getColumns()[0].getName());
    assertEquals("col2", alterTable.getColumns()[1].getName());
    LiteralValue value1 = (LiteralValue)alterTable.getValues()[0];
    assertEquals("1", value1.getValue());
    LiteralValue value2 = (LiteralValue)alterTable.getValues()[1];
    assertEquals("2", value2.getValue());
  }

  @Test
  public void testAlterTableDropPartition2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/alter_table_drop_partition_2.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.AlterTable, expr.getType());
    AlterTable alterTable = (AlterTable)expr;
    assertEquals(alterTable.getAlterTableOpType(), AlterTableOpType.DROP_PARTITION);
    assertEquals(3, alterTable.getColumns().length);
    assertEquals(3, alterTable.getValues().length);
    assertEquals("col1", alterTable.getColumns()[0].getName());
    assertEquals("col2", alterTable.getColumns()[1].getName());
    assertEquals("col3", alterTable.getColumns()[2].getName());
    LiteralValue value1 = (LiteralValue)alterTable.getValues()[0];
    assertEquals("2015", value1.getValue());
    LiteralValue value2 = (LiteralValue)alterTable.getValues()[1];
    assertEquals("01", value2.getValue());
    LiteralValue value3 = (LiteralValue)alterTable.getValues()[2];
    assertEquals("11", value3.getValue());
  }

  @Test
  public void testAlterTableDropPartition3() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/alter_table_drop_partition_3.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.AlterTable, expr.getType());
    AlterTable alterTable = (AlterTable)expr;
    assertEquals(alterTable.getAlterTableOpType(), AlterTableOpType.DROP_PARTITION);
    assertEquals(1, alterTable.getColumns().length);
    assertEquals(1, alterTable.getValues().length);
    assertEquals("col1", alterTable.getColumns()[0].getName());
    LiteralValue value1 = (LiteralValue)alterTable.getValues()[0];
    assertEquals("TAJO", value1.getValue());
  }

  @Test
  public void testAlterTableSetProperty1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/alter_table_set_property_1.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.AlterTable, expr.getType());
    AlterTable alterTable = (AlterTable)expr;
    assertEquals(alterTable.getAlterTableOpType(), AlterTableOpType.SET_PROPERTY);
    assertTrue(alterTable.hasParams());
    assertTrue(alterTable.getParams().containsKey("timezone"));
    assertEquals("GMT-7", alterTable.getParams().get("timezone"));
  }

  @Test
  public void testAlterTableSetProperty2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/alter_table_set_property_2.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.AlterTable, expr.getType());
    AlterTable alterTable = (AlterTable)expr;
    assertEquals(alterTable.getAlterTableOpType(), AlterTableOpType.SET_PROPERTY);
    assertTrue(alterTable.hasParams());
    assertTrue(alterTable.getParams().containsKey("text.delimiter"));
    assertEquals("&", alterTable.getParams().get("text.delimiter"));
  }

  @Test
  public void testAlterTableSetProperty3() throws IOException {
    // update multiple table properties with a single 'SET PROPERTY' sql
    String sql = FileUtil.readTextFileFromResource("queries/default/alter_table_set_property_3.sql");
    Expr expr = parseQuery(sql);
    assertEquals(OpType.AlterTable, expr.getType());
    AlterTable alterTable = (AlterTable)expr;
    assertEquals(alterTable.getAlterTableOpType(), AlterTableOpType.SET_PROPERTY);
    assertTrue(alterTable.hasParams());
    assertTrue(alterTable.getParams().containsKey("compression.type"));
    assertEquals("RECORD", alterTable.getParams().get("compression.type"));
    assertTrue(alterTable.getParams().containsKey("compression.codec"));
    assertEquals("org.apache.hadoop.io.compress.SnappyCodec", alterTable.getParams().get("compression.codec"));
  }

  @Test
  public void testTableSubQuery1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/table_subquery1.sql");
    parseQuery(sql);
  }

  @Test
  public void testTableSubQuery2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/table_subquery2.sql");
    parseQuery(sql);
  }

  @Test
  public void testInSubquery1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/in_subquery_1.sql");
    parseQuery(sql);
  }

  @Test
  public void testInSubquery2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/in_subquery_2.sql");
    parseQuery(sql);
  }

  @Test
  public void testExistsPredicate1() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/exists_predicate_1.sql");
    parseQuery(sql);
  }

  @Test
  public void testExistsPredicate2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/exists_predicate_2.sql");
    parseQuery(sql);
  }

  @Test
  public void testInsertIntoTable() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/insert_into_select_1.sql");
    parseQuery(sql);
  }

  @Test
  public void testInsertIntoLocation() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/insert_into_select_2.sql");
    parseQuery(sql);
  }

  @Test
  public void testInsertIntoTable2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/insert_into_select_3.sql");
    parseQuery(sql);
  }

  @Test
  public void testInsertOverwriteIntoTable() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/insert_overwrite_into_select_1.sql");
    parseQuery(sql);
  }

  @Test
  public void testInsertOverwriteIntoLocation() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/insert_overwrite_into_select_2.sql");
    parseQuery(sql);
  }

  @Test
  public void testInsertOverwriteIntoTable2() throws IOException {
    String sql = FileUtil.readTextFileFromResource("queries/default/insert_overwrite_into_select_3.sql");
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
      "col != null", // 38
  };

  public static Expr parseExpr(String sql) {
    ANTLRInputStream input = new ANTLRInputStream(sql);
    SQLLexer lexer = new SQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    parser.setBuildParseTree(true);
    SQLAnalyzer visitor = new SQLAnalyzer();
    SQLParser.Value_expressionContext context = parser.value_expression();
    return visitor.visitValue_expression(context);
  }

  @Test
  public void testExprs() {
    for (int i = 0; i < exprs.length; i++) {
      parseExpr(exprs[i]);
    }
  }

  @Test
  public void windowFunction1() throws IOException {
    assertParseResult("window1.sql", "window1.result");
  }

  @Test
  public void windowFunction2() throws IOException {
    assertParseResult("window2.sql", "window2.result");
  }

  @Test
  public void windowFunction3() throws IOException {
    assertParseResult("window3.sql", "window3.result");
  }

  @Test
  public void windowFunction4() throws IOException {
    assertParseResult("window4.sql", "window4.result");
  }

  @Test
  public void windowFunction5() throws IOException {
    assertParseResult("window5.sql", "window5.result");
  }

  @Test
  public void windowFunction6() throws IOException {
    assertParseResult("window6.sql", "window6.result");
  }

  @Test
  public void windowFunction7() throws IOException {
    assertParseResult("window7.sql", "window7.result");
  }

  @Test
  public void windowFunction8() throws IOException {
    assertParseResult("window8.sql", "window8.result");
  }

  @Test
  public void windowFunction9() throws IOException {
    assertParseResult("window9.sql", "window9.result");
  }

  @Test
  public void testSetCatalog1() throws IOException {
    assertParseResult("setcatalog1.sql", "setcatalog1.result");
  }

  @Test
  public void testSetCatalog2() throws IOException {
    assertParseResult("setcatalog2.sql", "setcatalog2.result");
  }

  @Test
  public void testTimezone1() throws IOException {
    assertParseResult("settimezone1.sql", "settimezone1.result");
  }

  @Test
  public void testTimezone2() throws IOException {
    assertParseResult("settimezone2.sql", "settimezone2.result");
  }

  @Test
  public void testTimezone3() throws IOException {
    assertParseResult("settimezone3.sql", "settimezone3.result");
  }

  @Test
  public void testSetSession1() throws IOException {
    assertParseResult("setsession1.sql", "setsession1.result");
  }

  @Test
  public void testSetSession2() throws IOException {
    assertParseResult("setsession2.sql", "setsession2.result");
  }

  @Test
  public void testSetSession3() throws IOException {
    assertParseResult("setsession3.sql", "setsession3.result");
  }

  @Test
  public void testSetSession4() throws IOException {
    assertParseResult("setsession4.sql", "setsession4.result");
  }

  @Test
  public void testSetSession5() throws IOException {
    assertParseResult("setsession5.sql", "setsession5.result");
  }

  @Test
  public void testSetSession6() throws IOException {
    assertParseResult("setsession6.sql", "setsession6.result");
  }

  @Test
  public void testSetSession7() throws IOException {
    assertParseResult("setsession7.sql", "setsession7.result");
  }

  @Test
  public void testCreateTableWithNested1() throws IOException {
    assertParseResult("create_table_nested_1.sql", "create_table_nested_1.result");
  }

  @Test
  public void testCreateTableWithNested2() throws IOException {
    assertParseResult("create_table_nested_2.sql", "create_table_nested_2.result");
  }
}
