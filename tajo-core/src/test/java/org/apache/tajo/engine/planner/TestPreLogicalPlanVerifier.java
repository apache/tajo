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

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.GlobalEngine;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestPreLogicalPlanVerifier extends QueryTestCaseBase {

  private static SQLAnalyzer analyzer;
  private static PreLogicalPlanVerifier verifier;

  @BeforeClass
  public static void setUp() {
    GlobalEngine engine = testingCluster.getMaster().getContext().getGlobalEngine();
    analyzer = engine.getAnalyzer();
    verifier = engine.getPreLogicalPlanVerifier();
  }

  public static VerificationState verify(String query) throws PlanningException {

    VerificationState state = new VerificationState();
    QueryContext context = LocalTajoTestingUtility.createDummyContext(conf);

    Expr expr = analyzer.parse(query);
    verifier.verify(context, state, expr);

    return state;
  }

  public static void valid(String query) throws PlanningException {
    VerificationState state = verify(query);
    if (state.errorMessages.size() > 0) {
      fail(state.getErrorMessages().get(0));
    }
  }

  public static void invalid(String query) throws PlanningException {
    VerificationState state = verify(query);
    if (state.errorMessages.size() == 0) {
      fail(PreLogicalPlanVerifier.class.getSimpleName() + " cannot catch any verification error: " + query);
    }
  }

  @Test
  public void testLimitWithFieldReference() throws PlanningException {
    valid("select * from lineitem limit 3");
    invalid("select * from lineitem limit l_orderkey");
  }
}
