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
}
