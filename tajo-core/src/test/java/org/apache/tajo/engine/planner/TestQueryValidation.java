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
import org.apache.tajo.plan.PlanningException;
import org.junit.Test;

import java.io.IOException;

public class TestQueryValidation extends QueryTestCaseBase {
  @Test
  public void testLimitClauses() throws PlanningException, IOException {
    // select * from lineitem limit 3;
    assertValidSQL("valid_limit_1.sql");

    // select * from lineitem limit l_orderkey;
    assertInvalidSQL("invalid_limit_1.sql");
  }

  @Test
  public void testGroupByClauses() throws PlanningException, IOException {
    // select l_orderkey from lineitem group by l_orderkey;
    assertValidSQL("valid_groupby_1.sql");

    // select * from lineitem group by l_orderkey;
    assertPlanError("error_groupby_1.sql");
    // select l_orderkey from lineitem group by l_paerkey;
    assertPlanError("error_groupby_2.sql");
  }

  @Test
  public void testCaseWhenExprs() throws PlanningException, IOException {
    // See TAJO-1098
    assertInvalidSQL("invalid_casewhen_1.sql");
  }

  @Test
  public void testUnsupportedStoreType() throws PlanningException, IOException {
    // See TAJO-1249
    assertInvalidSQL("invalid_store_format.sql");
  }
}
