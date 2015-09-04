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

import com.google.common.collect.Lists;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.SessionVars;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class TestJsonWithTimezone extends QueryTestCaseBase {
  @Test
  public void testTimezonedTable1() throws Exception {
    // Table - GMT (No table property or no system timezone)
    // Client - GMT (default client time zone is used if no TIME ZONE session variable is given.)
    try {
      executeDDL("datetime_table_ddl.sql", "timezoned", new String[]{"timezoned1"});
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned1");
    }
  }

  @Test
  public void testTimezonedTable2() throws Exception {
    // Table - timezone = GMT+9
    // Client - GMT (SET TIME ZONE 'GMT';)
    try {
      executeDDL("datetime_table_timezoned_ddl.sql", "timezoned", new String[]{"timezoned2"});
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned2");
    }
  }

  @Test
  public void testTimezonedTable3() throws Exception {
    // Table - timezone = GMT+9
    // Client - GMT+9 through TajoClient API

    Map<String,String> sessionVars = new HashMap<String, String>();
    sessionVars.put(SessionVars.TIMEZONE.name(), "GMT+9");
    getClient().updateSessionVariables(sessionVars);

    try {
      executeDDL("datetime_table_timezoned_ddl.sql", "timezoned", new String[]{"timezoned3"});
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned3");
    }

    getClient().unsetSessionVariables(Lists.newArrayList("TIMEZONE"));
  }

  @Test
  public void testTimezonedTable4() throws Exception {
    // Table - timezone = GMT+9
    // Client - GMT+9 (SET TIME ZONE 'GMT+9';)

    try {
      executeDDL("datetime_table_timezoned_ddl.sql", "timezoned", new String[]{"timezoned4"});
      ResultSet res = executeQuery();
      assertResultSet(res, "testTimezonedTable3.result");
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned4");
    }
  }

  @Test
  public void testTimezonedTable5() throws Exception {
    // Table - timezone = GMT+9 (by a specified system timezone)
    // TajoClient uses JVM default timezone (GMT+9)

    try {
      testingCluster.getConfiguration().setSystemTimezone(TimeZone.getTimeZone("GMT+9"));

      executeDDL("datetime_table_ddl.sql", "timezoned", new String[]{"timezoned5"});
      ResultSet res = executeQuery();
      assertResultSet(res, "testTimezonedTable3.result");
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE IF EXISTS timezoned5");

      // restore the config
      testingCluster.getConfiguration().setSystemTimezone(TimeZone.getTimeZone("GMT"));
    }
  }
}
