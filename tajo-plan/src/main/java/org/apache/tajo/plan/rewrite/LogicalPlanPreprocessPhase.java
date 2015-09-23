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

import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.ExprAnnotator;
import org.apache.tajo.plan.LogicalPlanner.PlanContext;
import org.apache.tajo.plan.logical.LogicalNode;

public abstract class LogicalPlanPreprocessPhase {

  protected final CatalogService catalog;
  protected final ExprAnnotator annotator;

  public LogicalPlanPreprocessPhase(CatalogService catalog, ExprAnnotator annotator) {
    this.catalog = catalog;
    this.annotator = annotator;
  }
  /**
   * It returns the pre-process phase name. It will be used for debugging and
   * building a optimization history.
   *
   * @return The pre-process phase name
   */
  public abstract String getName();

  /**
   * This method checks if this pre-process phase can be applied to the given expression tree.
   *
   * @param context
   * @param expr
   * @return
   */
  public abstract boolean isEligible(PlanContext context, Expr expr) throws TajoException;

  /**
   * Do a pre-process phase for an expression tree and returns it.
   * It must be guaranteed that the input expression tree is not modified even after rewrite.
   * In other words, the rewrite has to modify a copy of the expression tree.
   *
   * @param context
   * @param expr
   * @return The rewritten logical plan.
   */
  public abstract LogicalNode process(PlanContext context, Expr expr) throws TajoException;
}
