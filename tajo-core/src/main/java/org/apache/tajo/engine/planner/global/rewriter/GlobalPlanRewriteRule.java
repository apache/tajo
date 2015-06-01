/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner.global.rewriter;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.plan.PlanningException;

/**
 * A rewrite rule for global plans
 */
public interface GlobalPlanRewriteRule {

  /**
   * Return rule name
   * @return Rule name
   */
  String getName();

  /**
   * Check if this rule should be applied.
   *
   * @param queryContext Query context
   * @param plan Global Plan
   * @return
   */
  boolean isEligible(OverridableConf queryContext, MasterPlan plan);

  /**
   * Rewrite a global plan
   *
   * @param plan Global Plan
   * @return
   */
  MasterPlan rewrite(MasterPlan plan) throws PlanningException;
}
