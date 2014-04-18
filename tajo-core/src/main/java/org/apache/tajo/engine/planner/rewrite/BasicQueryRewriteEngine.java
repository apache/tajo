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

package org.apache.tajo.engine.planner.rewrite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlanningException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This is a basic query rewrite rule engine. This rewrite rule engine
 * rewrites a logical plan with various query rewrite rules.
 */
public class BasicQueryRewriteEngine implements QueryRewriteEngine {
  /** class logger */
  private Log LOG = LogFactory.getLog(BasicQueryRewriteEngine.class);

  /** a map for query rewrite rules  */
  private Map<String, RewriteRule> rewriteRules = new LinkedHashMap<String, RewriteRule>();

  /**
   * Add a query rewrite rule to this engine.
   *
   * @param rule The rule to be added to this engine.
   */
  public void addRewriteRule(RewriteRule rule) {
    if (!rewriteRules.containsKey(rule.getName())) {
      rewriteRules.put(rule.getName(), rule);
    }
  }

  /**
   * Rewrite a logical plan with all query rewrite rules added to this engine.
   *
   * @param plan The plan to be rewritten with all query rewrite rule.
   * @return The rewritten plan.
   */
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    RewriteRule rule;
    for (Entry<String, RewriteRule> rewriteRule : rewriteRules.entrySet()) {
      rule = rewriteRule.getValue();
      if (rule.isEligible(plan)) {
        plan = rule.rewrite(plan);
        LOG.info("The rule \"" + rule.getName() + " \" rewrites the query.");
      }
    }

    return plan;
  }
}
