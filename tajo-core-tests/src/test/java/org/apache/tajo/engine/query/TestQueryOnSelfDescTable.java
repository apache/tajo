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

package org.apache.tajo.engine.query;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.exception.AmbiguousColumnException;
import org.apache.tajo.exception.TajoException;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

public class TestQueryOnSelfDescTable extends QueryTestCaseBase {

  public TestQueryOnSelfDescTable() throws IOException, TajoException, SQLException {
    super();

    executeString(String.format("create external table if not exists self_desc_table1 (*) using json location '%s'",
        getDataSetFile("sample1"))).close();

    executeString(String.format("create external table if not exists self_desc_table2 (*) using json location '%s'",
        getDataSetFile("sample2"))).close();

    executeString(String.format("create external table if not exists self_desc_table3 (*) using json location '%s'",
        getDataSetFile("tweets"))).close();

    executeString(String.format("create external table if not exists github (*) using json location '%s'",
        getDataSetFile("github"))).close();
  }

  @After
  public void teardown() throws TajoException, SQLException {
    executeString("drop table if exists self_desc_table1").close();
    executeString("drop table if exists self_desc_table2").close();
    executeString("drop table if exists self_desc_table3").close();
    executeString("drop table if exists github").close();
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testSelect() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testSelect2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testGroupby() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testGroupby2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testGroupby3() throws Exception {
    runSimpleTests();
  }

  @Test
  @SimpleTest
  public final void testSort() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testCrossJoin() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testJoinWithSchemaFullTable() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testJoinWithSchemaFullTable2() throws Exception {
    runSimpleTests();
  }

  @Test(expected = AmbiguousColumnException.class)
  public final void testJoinWithSchemaFullTable3() throws Exception {
    executeString("" +
        "select " +
        "  user.favourites_count::int8, " +
        "  l_linenumber, " +
        "  l_comment " +
        "from " +
        "  default.lineitem, " +
        "  self_desc_table1, " +
        "  self_desc_table3, " +
        "  default.orders, " +
        "  default.supplier " +
        "where " +
        "  user.favourites_count::int8 = (l_orderkey - 1) and " +
        "  l_orderkey = o_orderkey and " +
        "  l_linenumber = s_suppkey and " +
        "  self_desc_table3.user.favourites_count = self_desc_table1.name.first_name");
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testJoinWithSchemaFullTable4() throws Exception {
    runSimpleTests();
  }

  @Test(expected = AmbiguousColumnException.class)
  public final void testJoinOfSelfDescTables() throws Exception {
    executeString("" +
        "select " +
        "  user.favourites_count::int8 " +
        "from " +
        "  self_desc_table1, " +
        "  self_desc_table3 " +
        "where " +
        "  user.favourites_count = name.first_name");
  }

  @Test
  @Option(sort = true)
  @SimpleTest
  public final void testJoinOfSelfDescTablesWithQualifiedColumns() throws Exception {
    runSimpleTests();
  }

  @Test(expected = AmbiguousColumnException.class)
  public final void testJoinWithSingleQualifiedColumn() throws Exception {
    executeString("" +
        "select " +
        "  self_desc_table3.user.favourites_count::int8 " +
        "from " +
        "  self_desc_table1, " +
        "  self_desc_table3 " +
        "where " +
        "  self_desc_table3.user.favourites_count = name.first_name");
  }

  @Test
  @Option(sort = true)
  @SimpleTest(queries = {
      @QuerySpec("select * from default.lineitem where l_orderkey in (select user.favourites_count::int8 + 1 from self_desc_table3)")
  })
  public final void testInSubquery() throws Exception {
    runSimpleTests();
  }
}
