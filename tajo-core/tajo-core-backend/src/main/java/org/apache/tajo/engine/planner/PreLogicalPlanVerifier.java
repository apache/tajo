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

package org.apache.tajo.engine.planner;

import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.Insert;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.Projection;

import java.util.Stack;

public class PreLogicalPlanVerifier extends BaseAlgebraVisitor <VerificationState, Expr> {

  public Expr visitInsert(VerificationState ctx, Stack<Expr> stack, Insert expr) throws PlanningException {
    Expr child = super.visitInsert(ctx, stack, expr);

    if (child != null && child.getType() == OpType.Projection) {
      if (expr.hasTargetColumns()) {
        Projection projection = (Projection) child;
        int projectColumnNum = projection.getNamedExprs().length;
        int targetColumnNum = expr.getTargetColumns().length;

        if (targetColumnNum > projectColumnNum)  {
          ctx.addVerification("INSERT has more target columns than expressions");
        } else if (targetColumnNum < projectColumnNum) {
          ctx.addVerification("INSERT has more expressions than target columns");
        }
      }
    }

    return child;
  }
}
