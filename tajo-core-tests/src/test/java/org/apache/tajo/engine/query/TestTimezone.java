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

package org.apache.tajo.engine.query;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.exception.TajoException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
public class TestTimezone extends QueryTestCaseBase {
  private String timezone;

  public TestTimezone(String timezone) {
    this.timezone = timezone;
  }

  @Test
  public void testToTimestamp() throws TajoException, SQLException, IOException {
    executeString(String.format("SET TIME ZONE TO '%s'", timezone)).close();

    try (ResultSet res =
             executeString("select to_timestamp('2015/08/12 14:00:00', 'FMYYYY/FMMM/FMDD HH24:FMMI:FMSS');")) {
      assertTrue(res.next());
      assertEquals("2015-08-12 14:00:00", res.getString(1));
    }
  }

  @Test
  public void testCastWithTimezone() throws TajoException, SQLException, IOException {
    executeString(String.format("SET TIME ZONE TO '%s'", timezone)).close();

    try (ResultSet res = executeString("select CAST('2015-08-12 14:00:00' AS timestamp);")) {
      assertTrue(res.next());
      assertEquals("2015-08-12 14:00:00", res.getString(1));
    }
  }

  @Test
  public void testCastWithTimezone2() throws TajoException, SQLException, IOException {
    executeString(String.format("SET TIME ZONE TO '%s'", timezone)).close();

    try (ResultSet res = executeString("select '2015-08-12 14:00:00'::TIMESTAMP;")) {
      assertTrue(res.next());
      assertEquals("2015-08-12 14:00:00", res.getString(1));
    }
  }

  @Test
  public void testToChar() throws TajoException, SQLException, IOException {
    executeString(String.format("SET TIME ZONE TO '%s'", timezone)).close();

    try (ResultSet res = executeString("select to_char('2015-08-12 14:00:00'::TIMESTAMP, 'yyyy-mm-dd hh24:mi:ss')")) {
      assertTrue(res.next());
      assertEquals("2015-08-12 14:00:00", res.getString(1));
    }
  }

  @Test
  public void testCTASWithTimezone() throws TajoException, SQLException, IOException {
    executeString(String.format("SET TIME ZONE TO '%s'", timezone)).close();
    try {
      executeString("create table test1 (col1 TIMESTAMP)").close();
      executeString("insert overwrite into test1 select '2015-08-12 14:00:00'::TIMESTAMP").close();
      try (ResultSet res = executeString("select * from test1")) {
        assertTrue(res.next());
        assertEquals("2015-08-12 14:00:00", res.getString(1));
      }

      executeString("create table test2 as select * from test1").close();
      try (ResultSet res = executeString("select * from test2")) {
        assertTrue(res.next());
        assertEquals("2015-08-12 14:00:00", res.getString(1));
      }
    } finally {
      executeString("drop table test1 purge").close();
      executeString("drop table test2 purge").close();
    }
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<Object []> getParameters() {
    return Arrays.asList(new Object[][]{
        {"GMT"},
        {"GMT+9"}
    });
  }
}
