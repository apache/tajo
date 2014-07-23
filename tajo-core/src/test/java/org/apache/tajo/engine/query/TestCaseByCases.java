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
import org.apache.tajo.TajoConstants;
import org.junit.Test;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;

public class TestCaseByCases extends QueryTestCaseBase {

  public TestCaseByCases() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testTAJO415Case() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO418Case() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  /**
   * It's an unit test to reproduce TAJO-619 (https://issues.apache.org/jira/browse/TAJO-619).
   */
  @Test
  public final void testTAJO619Case() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO718Case() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO739Case() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO880Case1() throws Exception {
    //TAJO-880: NULL in CASE clause occurs Exception.
    ResultSet res = executeString(
        "select case when l_returnflag != 'R' then l_orderkey else null end from lineitem"
    );

    String expected =
        "?casewhen\n" +
        "-------------------------------\n" +
        "1\n" +
        "1\n" +
        "2\n" +
        "null\n" +
        "null\n";

    assertEquals(expected, resultSetToString(res));
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO880Case2() throws Exception {
    //TAJO-880: NULL in CASE clause occurs Exception.
    ResultSet res = executeString(
        "select case when l_returnflag != 'R' then null else l_orderkey end from lineitem"
    );

    String expected =
        "?casewhen\n" +
        "-------------------------------\n" +
        "null\n" +
        "null\n" +
        "null\n" +
        "3\n" +
        "3\n";

    assertEquals(expected, resultSetToString(res));
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO880Case3() throws Exception {
    //TAJO-880: NULL in CASE clause occurs Exception.
    ResultSet res = executeString(
        "select case " +
            "when l_orderkey = 1 then null " +
            "when l_orderkey = 2 then l_orderkey " +
            "else null end " +
        "from lineitem"
    );

    String expected =
        "?casewhen\n" +
            "-------------------------------\n" +
            "null\n" +
            "null\n" +
            "2\n" +
            "null\n" +
            "null\n";

    assertEquals(expected, resultSetToString(res));
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO914Case1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO914Case2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO914Case3() throws Exception {
    executeString("CREATE TABLE T3 (l_orderkey bigint, col1 text);").close();
    ResultSet res = executeQuery();
    res.close();

    res = executeString("select * from T3;");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO914Case4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO917Case1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}
