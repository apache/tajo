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
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.ExprAnnotator;
import org.apache.tajo.plan.LogicalPlanner.PlanContext;
import org.apache.tajo.plan.logical.LogicalNode;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class BaseLogicalPlanPreprocessEngine implements LogicalPlanPreprocessEngine {
  private final CatalogService catalogService;
  private final ExprAnnotator exprAnnotator;

  public BaseLogicalPlanPreprocessEngine(CatalogService catalogService, ExprAnnotator exprAnnotator) {
    this.catalogService = catalogService;
    this.exprAnnotator = exprAnnotator;
  }

  /** class logger */
  private Log LOG = LogFactory.getLog(BaseLogicalPlanPreprocessEngine.class);

  /** a map for pre-process phases */
  private Map<String, LogicalPlanPreprocessPhase> preprocessPhases = new LinkedHashMap<>();

  /**
   * Add a pre-process phase to this engine.
   *
   * @param phases pre-process phase
   */
  public void addProcessPhase(Iterable<Class<? extends LogicalPlanPreprocessPhase>> phases) {
    for (Class<? extends LogicalPlanPreprocessPhase> clazz : phases) {
      try {
        Constructor cons = clazz.getConstructor(CatalogService.class, ExprAnnotator.class);
        LogicalPlanPreprocessPhase rule = (LogicalPlanPreprocessPhase) cons.newInstance(catalogService, exprAnnotator);
        addProcessPhase(rule);
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }

  /**
   * Add a pre-process phase to this engine.
   *
   * @param phase The pre-process phase to be added to this engine.
   */
  public void addProcessPhase(LogicalPlanPreprocessPhase phase) {
    if (!preprocessPhases.containsKey(phase.getName())) {
      preprocessPhases.put(phase.getName(), phase);
    }
  }

  /**
   * Do every pre-process phase added to this engine.
   *
   * @param context
   * @return The rewritten logical node.
   */
  @Override
  public LogicalNode process(PlanContext context, Expr expr) throws TajoException {
    LogicalPlanPreprocessPhase rule;
    LogicalNode node = null;
    for (Entry<String, LogicalPlanPreprocessPhase> preprocessPhase : preprocessPhases.entrySet()) {
      rule = preprocessPhase.getValue();
      if (rule.isEligible(context, expr)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The rule \"" + rule.getName() + " \" rewrites the query.");
        }
        node = rule.process(context, expr);
      }
    }

    return node;
  }
}
