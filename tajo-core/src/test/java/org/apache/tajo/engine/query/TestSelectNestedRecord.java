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
import org.apache.tajo.util.TUtil;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestSelectNestedRecord extends QueryTestCaseBase {

  @Test
  public final void testSelect1() throws Exception {
    List<String> tables = executeDDL("sample1_ddl.sql", "sample1", "sample1");
    assertEquals(TUtil.newList("sample1"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelect2() throws Exception {
    List<String> tables = executeDDL("tweets_ddl.sql", "tweets", "tweets");
    assertEquals(TUtil.newList("tweets"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNestedFieldAsGroupbyKey1() throws Exception {
    List<String> tables = executeDDL("tweets_ddl.sql", "tweets", "tweets");
    assertEquals(TUtil.newList("tweets"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNestedFieldAsJoinKey1() throws Exception {
    List<String> tables = executeDDL("tweets_ddl.sql", "tweets", "tweets");
    assertEquals(TUtil.newList("tweets"), tables);

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}
