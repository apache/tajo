/**
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

package org.apache.tajo.plan.rewrite;

import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;

/**
 * An interface for a rewrite rule.
 */
public interface LogicalPlanRewriteRule {

  /**
   * It returns the rewrite rule name. It will be used for debugging and
   * building a optimization history.
   *
   * @return The rewrite rule name
   */
  String getName();

  /**
   * This method checks if this rewrite rule can be applied to a given query plan.
   * For example, the selection push down can not be applied to the query plan without any filter.
   * In such case, it will return false.
   *
   * @param context rewrite rule context.
   * @return True if this rule can be applied to a given plan. Otherwise, false.
   */
  boolean isEligible(LogicalPlanRewriteRuleContext context);

  /**
   * Updates a logical plan and returns an updated logical plan rewritten by this rule.
   * It must be guaranteed that the input logical plan is not modified even after rewrite.
   * In other words, the rewrite has to modify an plan copied from the input plan.
   *
   * @param context rewrite rule context.
   * @return The rewritten logical plan.
   */
  LogicalPlan rewrite(LogicalPlanRewriteRuleContext context) throws TajoException;
}
