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
import org.junit.Test;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestSelectNestedRecord extends QueryTestCaseBase {

  @Test
  public final void testSelect0() throws Exception {
    List<String> tables = executeDDL("sample1_ddl.sql", "sample1", "sample1");
    assertEquals(Arrays.asList("sample1"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelect1() throws Exception {
    List<String> tables = executeDDL("sample1_ddl.sql", "sample1", "sample2");
    assertEquals(Arrays.asList("sample2"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelect2() throws Exception {
    List<String> tables = executeDDL("tweets_ddl.sql", "tweets", "tweets");
    assertEquals(Arrays.asList("tweets"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelect3() throws Exception {
    List<String> tables = executeDDL("sample2_ddl.sql", "sample2", "sample5");
    assertEquals(Arrays.asList("sample5"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testTAJO_1610() throws Exception {
    executeString("CREATE DATABASE tweets").close();
    List<String> tables = executeDDL("tweets_ddl.sql", "tweets", "tweets.tweets");
    assertEquals(Arrays.asList("tweets.tweets"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNestedFieldAsGroupbyKey1() throws Exception {
    List<String> tables = executeDDL("tweets_ddl.sql", "tweets", "tweets");
    assertEquals(Arrays.asList("tweets"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNestedFieldAsJoinKey1() throws Exception {
    List<String> tables = executeDDL("tweets_ddl.sql", "tweets", "tweets");
    assertEquals(Arrays.asList("tweets"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInsertType1() throws Exception {
    // all columns
    List<String> tables = executeDDL("sample1_ddl.sql", "sample1", "sample3");
    assertEquals(Arrays.asList("sample3"), tables);

    executeString("CREATE TABLE clone (title TEXT, name RECORD (first_name TEXT, last_name TEXT)) USING JSON;").close();

    executeString("INSERT INTO clone (title, name.first_name, name.last_name) SELECT title, name.first_name, name.last_name from sample3").close();
    ResultSet res = executeString("select title, name.first_name, name.last_name from clone");
    assertResultSet(res);
    res.close();
  }

  @Test
  public final void testInsertType2() throws Exception {
    // some columns
    List<String> tables = executeDDL("sample1_ddl.sql", "sample1", "sample4");
    assertEquals(Arrays.asList("sample4"), tables);

    executeString("CREATE TABLE clone2 (title TEXT, name RECORD (first_name TEXT, last_name TEXT)) USING JSON;").close();

    executeString("INSERT INTO clone2 (title, name.last_name) SELECT title, name.last_name from sample4").close();
    ResultSet res = executeString("select title, name.first_name, name.last_name from clone2");
    assertResultSet(res);
    res.close();
  }
}
