/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.engine.planner;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.exception.SQLSyntaxError;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class TestQueryValidation extends QueryTestCaseBase {
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testInsertWithWrongTargetColumn() throws Exception {
    executeString("CREATE TABLE T1 (col1 int, col2 int)").close();
    assertInvalidSQL("INSERT INTO T1 (col1, col3) select l_orderkey, l_partkey from default.lineitem");
  }

  @Test
  public void testLimitClauses() throws IOException {
    // select * from lineitem limit 3;
    assertValidSQLFromFile("valid_limit_1.sql");

    // select * from lineitem limit l_orderkey;
    assertInvalidSQLFromFile("invalid_limit_1.sql");
  }

  @Test
  public void testGroupByClauses() throws IOException {
    // select l_orderkey from lineitem group by l_orderkey;
    assertValidSQLFromFile("valid_groupby_1.sql");

    // select * from lineitem group by l_orderkey;
    assertPlanError("error_groupby_1.sql");
    // select l_orderkey from lineitem group by l_paerkey;
    assertPlanError("error_groupby_2.sql");
  }

  @Test
  public void testCaseWhenExprs() throws IOException {
    // See TAJO-1098
    assertInvalidSQLFromFile("invalid_casewhen_1.sql");
  }

  @Test
  public void testUnsupportedStoreType() throws IOException {
    // See TAJO-1249
    assertInvalidSQLFromFile("invalid_store_format.sql");
  }

  @Test
  public void testCreateExternalTableWithTablespace() throws Exception {
    exception.expect(SQLSyntaxError.class);
    exception.expectMessage("Tablespace clause is not allowed for an external table.");

    executeFile("create_external_table_with_tablespace.sql");
  }

  @Test
  public void testCreateExternalTableWithoutLocation() throws Exception {
    exception.expect(SQLSyntaxError.class);
    exception.expectMessage("LOCATION clause must be required for an external table.");

    executeFile("create_external_table_without_location.sql");
  }
}
