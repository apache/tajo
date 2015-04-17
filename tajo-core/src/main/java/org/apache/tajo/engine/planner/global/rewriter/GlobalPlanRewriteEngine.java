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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.util.ReflectionUtil;

import java.util.LinkedHashMap;
import java.util.Map;

public class GlobalPlanRewriteEngine {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(GlobalPlanRewriteEngine.class);

  /** a map for query rewrite rules  */
  private final Map<String, GlobalPlanRewriteRule> rewriteRules = new LinkedHashMap<String, GlobalPlanRewriteRule>();

  /**
   * Add a query rewrite rule to this engine.
   *
   * @param rules Rule classes
   */
  public void addRewriteRule(Iterable<Class<? extends GlobalPlanRewriteRule>> rules) {
    for (Class<? extends GlobalPlanRewriteRule> clazz : rules) {
      try {
        GlobalPlanRewriteRule rule = ReflectionUtil.newInstance(clazz);
        addRewriteRule(rule);
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }

  /**
   * Add a query rewrite rule to this engine.
   *
   * @param rule The rule to be added to this engine.
   */
  public void addRewriteRule(GlobalPlanRewriteRule rule) {
    if (!rewriteRules.containsKey(rule.getName())) {
      rewriteRules.put(rule.getName(), rule);
    }
  }

  /**
   * Rewrite a global plan with all query rewrite rules added to this engine.
   *
   * @param plan The plan to be rewritten with all query rewrite rule.
   * @return The rewritten plan.
   */
  public MasterPlan rewrite(OverridableConf queryContext, MasterPlan plan) throws PlanningException {
    GlobalPlanRewriteRule rule;
    for (Map.Entry<String, GlobalPlanRewriteRule> rewriteRule : rewriteRules.entrySet()) {
      rule = rewriteRule.getValue();
      if (rule.isEligible(queryContext, plan)) {
        plan = rule.rewrite(plan);
        if (LOG.isDebugEnabled()) {
          LOG.debug("The rule \"" + rule.getName() + " \" rewrites the query.");
        }
      }
    }

    return plan;
  }
}
