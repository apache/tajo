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

import org.apache.tajo.algebra.*;

import java.util.Stack;

public abstract class BaseAlgebraVisitor<T1, T2> implements AlgebraVisitor<T1, T2> {

  /**
   * The prehook is called before each expression is visited.
   */
  public void preHook(T1 ctx, Stack<OpType> stack, Expr expr) throws PlanningException {
  }


  /**
   * The posthook is called before each expression is visited.
   */
  public T2 postHook(T1 ctx, Stack<OpType> stack, Expr expr, T2 current) throws PlanningException {
    return current;
  }

  /**
   * visitChild visits each relational operator expression recursively.
   *
   * @param stack The stack contains the upper operators' type.
   * @param expr The visiting relational operator
   */
  public T2 visitChild(T1 ctx, Stack<OpType> stack, Expr expr) throws PlanningException {
    preHook(ctx, stack, expr);

    T2 current;
    switch (expr.getType()) {
      case Projection:
        current = visitProjection(ctx, stack, (Projection) expr);
        break;
      case Limit:
        current = visitLimit(ctx, stack, (Limit) expr);
        break;
      case Sort:
        current = visitSort(ctx, stack, (Sort) expr);
        break;
      case Aggregation:
        current = visitGroupBy(ctx, stack, (Aggregation) expr);
        break;
      case Join:
        current = visitJoin(ctx, stack, (Join) expr);
        break;
      case Filter:
        current = visitFilter(ctx, stack, (Selection) expr);
        break;
      case Union:
        current = visitUnion(ctx, stack, (SetOperation) expr);
        break;
      case Except:
        current = visitExcept(ctx, stack, (SetOperation) expr);
        break;
      case Intersect:
        current = visitIntersect(ctx, stack, (SetOperation) expr);
        break;
      case RelationList:
        current = visitRelationList(ctx, stack, (RelationList) expr);
        break;
      case Relation:
        current = visitRelation(ctx, stack, (Relation) expr);
        break;
      case CreateTable:
        current = visitCreateTable(ctx, stack, (CreateTable) expr);
        break;
      case DropTable:
        current = visitDropTable(ctx, stack, (DropTable) expr);
        break;
      default:
        throw new PlanningException("Cannot support this type algebra \"" + expr.getType() + "\"");
    }

    postHook(ctx, stack, expr, current);

    return current;
  }

  @Override
  public T2 visitProjection(T1 ctx, Stack<OpType> stack, Projection expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public T2 visitLimit(T1 ctx, Stack<OpType> stack, Limit expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public T2 visitSort(T1 ctx, Stack<OpType> stack, Sort expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public T2 visitGroupBy(T1 ctx, Stack<OpType> stack, Aggregation expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public T2 visitJoin(T1 ctx, Stack<OpType> stack, Join expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = visitChild(ctx, stack, expr.getLeft());
    visitChild(ctx, stack, expr.getRight());
    stack.pop();
    return child;
  }

  @Override
  public T2 visitFilter(T1 ctx, Stack<OpType> stack, Selection expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public T2 visitUnion(T1 ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = visitChild(ctx, stack, expr.getLeft());
    visitChild(ctx, stack, expr.getRight());
    stack.pop();
    return child;
  }

  @Override
  public T2 visitExcept(T1 ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = visitChild(ctx, stack, expr.getLeft());
    visitChild(ctx, stack, expr.getRight());
    stack.pop();
    return child;
  }

  @Override
  public T2 visitIntersect(T1 ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = visitChild(ctx, stack, expr.getLeft());
    visitChild(ctx, stack, expr.getRight());
    stack.pop();
    return child;
  }

  @Override
  public T2 visitRelationList(T1 ctx, Stack<OpType> stack, RelationList expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = null;
    for (Expr e : expr.getRelations()) {
      child = visitChild(ctx, stack, e);
    }
    stack.pop();
    return child;
  }

  @Override
  public T2 visitRelation(T1 ctx, Stack<OpType> stack, Relation expr) throws PlanningException {
    return null;
  }

  @Override
  public T2 visitCreateTable(T1 ctx, Stack<OpType> stack, CreateTable expr) throws PlanningException {
    stack.push(expr.getType());
    T2 child = null;
    if (expr.hasSubQuery()) {
       child = visitChild(ctx, stack, expr.getSubQuery());
    }
    stack.pop();
    return child;
  }

  @Override
  public T2 visitDropTable(T1 ctx, Stack<OpType> stack, DropTable expr) throws PlanningException {
    return null;
  }
}
