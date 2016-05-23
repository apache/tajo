/*
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

package org.apache.tajo.storage;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

@Category(IntegrationTest.class)
public class TestQueryOnOrcFile extends QueryTestCaseBase {

  @Before
  public void setup() throws Exception {
    executeDDL("datetime_table_timezoned_ddl.sql", "timezoned", "timezoned");
    executeDDL("datetime_table_timezoned_orc_ddl.sql", null, "timezoned_orc");

    executeString("INSERT OVERWRITE INTO timezoned_orc SELECT t_timestamp, t_date FROM timezoned");
  }

  @After
  public void teardown() throws Exception {
    executeString("DROP TABLE IF EXISTS timezoned");
    executeString("DROP TABLE IF EXISTS timezoned_orc PURGE");
  }

  @Test
  public void testTimezone1() throws Exception {
    executeString("SET TIME ZONE 'GMT+9'");
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testTimezone2() throws Exception {
    executeString("SET TIME ZONE 'GMT+1'");
    ResultSet res = executeString("select * from timezoned_orc");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testTimezone3() throws Exception {
    executeString("SET TIME ZONE 'GMT'");
    ResultSet res = executeString("select * from timezoned_orc");
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testTimezone4() throws Exception {
    executeString("\\set TIMEZONE 'GMT-5'");
    ResultSet res = executeString("select * from timezoned_orc");
    assertResultSet(res);
    cleanupQuery(res);
  }
}
