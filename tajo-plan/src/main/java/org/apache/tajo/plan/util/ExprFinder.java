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

package org.apache.tajo.plan.util;

import org.apache.tajo.algebra.BinaryOperator;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.UnaryOperator;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class ExprFinder extends SimpleAlgebraVisitor<ExprFinder.Context, Object> {

  static class Context<T> {
    List<T> set = new ArrayList<>();
    OpType targetType;

    Context(OpType type) {
      this.targetType = type;
    }
  }

  public static <T extends Expr> Set<T> finds(Expr expr, OpType type) {
    return new HashSet<>(findsInOrder(expr, type));
  }

  public static <T extends Expr> List<T> findsInOrder(Expr expr, OpType type) {
    Context<T> context = new Context<>(type);
    ExprFinder finder = new ExprFinder();
    try {
      finder.visit(context, new Stack<>(), expr);
    } catch (TajoException e) {
      throw new TajoInternalError(e);
    }
    return context.set;
  }

  public Object visit(Context ctx, Stack<Expr> stack, Expr expr) throws TajoException {
    if (expr instanceof UnaryOperator) {
      preHook(ctx, stack, expr);
      visitUnaryOperator(ctx, stack, (UnaryOperator) expr);
      postHook(ctx, stack, expr, null);
    } else if (expr instanceof BinaryOperator) {
      preHook(ctx, stack, expr);
      visitBinaryOperator(ctx, stack, (BinaryOperator) expr);
      postHook(ctx, stack, expr, null);
    } else {
      super.visit(ctx, stack, expr);
    }

    if (expr != null && ctx.targetType == expr.getType()) {
      ctx.set.add(expr);
    }

    return null;
  }
}
