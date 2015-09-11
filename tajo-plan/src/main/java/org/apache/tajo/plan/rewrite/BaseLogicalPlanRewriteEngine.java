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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This is a basic query rewrite rule engine. This rewrite rule engine
 * rewrites a logical plan with various query rewrite rules.
 */
public class BaseLogicalPlanRewriteEngine implements LogicalPlanRewriteEngine {
  /** class logger */
  private Log LOG = LogFactory.getLog(BaseLogicalPlanRewriteEngine.class);

  /** a map for query rewrite rules  */
  private Map<String, LogicalPlanRewriteRule> rewriteRules = new LinkedHashMap<>();

  /**
   * Add a query rewrite rule to this engine.
   *
   * @param rules Rule classes
   */
  public void addRewriteRule(Iterable<Class<? extends LogicalPlanRewriteRule>> rules) {
    for (Class<? extends LogicalPlanRewriteRule> clazz : rules) {
      try {
        LogicalPlanRewriteRule rule = clazz.newInstance();
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
  public void addRewriteRule(LogicalPlanRewriteRule rule) {
    if (!rewriteRules.containsKey(rule.getName())) {
      rewriteRules.put(rule.getName(), rule);
    }
  }

  /**
   * Rewrite a logical plan with all query rewrite rules added to this engine.
   *
   * @param context
   * @return The rewritten plan.
   */
  public LogicalPlan rewrite(LogicalPlanRewriteRuleContext context) throws TajoException {
    LogicalPlanRewriteRule rule;
    LogicalPlan plan = null;
    for (Entry<String, LogicalPlanRewriteRule> rewriteRule : rewriteRules.entrySet()) {
      rule = rewriteRule.getValue();
      if (rule.isEligible(context)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The rule \"" + rule.getName() + " \" rewrites the query.");
        }
        plan = rule.rewrite(context);
      }
    }

    return plan;
  }
}
