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

package org.apache.tajo.plan;

import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlanner.PlanContext;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.rewrite.BaseLogicalPlanPreprocessEngine;
import org.apache.tajo.plan.rewrite.BaseLogicalPlanPreprocessPhaseProvider;

/**
 * It finds all relations for each block and builds base schema information.
 */
public class LogicalPlanPreprocessor {
  private BaseLogicalPlanPreprocessEngine engine;
  private BaseLogicalPlanPreprocessPhaseProvider provider;

  LogicalPlanPreprocessor(CatalogService catalog, ExprAnnotator annotator) {
    provider = new BaseLogicalPlanPreprocessPhaseProvider();
    engine = new BaseLogicalPlanPreprocessEngine(catalog, annotator);
    engine.addProcessPhase(provider.getPhases());
  }

  public LogicalNode process(PlanContext context, Expr expr) throws TajoException {
    return engine.process(context, expr);
  }
}
