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

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class TestTruncateTable extends QueryTestCaseBase {
  public TestTruncateTable() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }
  @Test
  public final void testTruncateTable() throws Exception {
    try {
      ResultSet res = executeFile("table1_ddl.sql");
      res.close();
      assertTableExists("truncate_table1");

      res = executeString("select * from truncate_table1");
      int numRows = 0;
      while (res.next()) {
        numRows++;
      }
      assertEquals(5, numRows);
      res.close();

      executeString("truncate table truncate_table1");
      assertTableExists("truncate_table1");

      res = executeString("select * from truncate_table1");
      numRows = 0;
      while (res.next()) {
        numRows++;
      }
      assertEquals(0, numRows);
      res.close();
    } finally {
      executeString("DROP TABLE truncate_table1 PURGE");
    }
  }


  /*
  Currently TajoClient can't throw exception when plan error.
  The following test cast should be uncommented after https://issues.apache.org/jira/browse/TAJO-762

  @Test
  public final void testTruncateExternalTable() throws Exception {
    try {
      List<String> createdNames = executeDDL("table2_ddl.sql", "truncate_table2", "truncate_table2");
      assertTableExists(createdNames.get(0));

      ResultSet res = executeString("select * from truncate_table2");
      int numRows = 0;
      while (res.next()) {
        numRows++;
      }
      assertEquals(4, numRows);
      res.close();

      executeString("truncate table truncate_table2");

      fail("Can't truncate external table");
    } catch (Exception e) {
      // succeeded
      assertTableExists("truncate_table2");

      ResultSet res = executeString("select * from truncate_table2");
      int numRows = 0;
      while (res.next()) {
        numRows++;
      }
      assertEquals(4, numRows);
      res.close();
    } finally {
      executeString("DROP TABLE truncate_table2 PURGE");
    }
  }
  */
}
