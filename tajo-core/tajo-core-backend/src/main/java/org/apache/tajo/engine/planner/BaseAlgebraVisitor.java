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

public abstract class BaseAlgebraVisitor<CONTEXT, RESULT> implements AlgebraVisitor<CONTEXT, RESULT> {

  /**
   * The prehook is called before each expression is visited.
   */
  public void preHook(CONTEXT ctx, Stack<OpType> stack, Expr expr) throws PlanningException {
  }


  /**
   * The posthook is called before each expression is visited.
   */
  public RESULT postHook(CONTEXT ctx, Stack<OpType> stack, Expr expr, RESULT current) throws PlanningException {
    return current;
  }

  /**
   * visitChild visits each relational operator expression recursively.
   *
   * @param stack The stack contains the upper operators' type.
   * @param expr The visiting relational operator
   */
  public RESULT visitChild(CONTEXT ctx, Stack<OpType> stack, Expr expr) throws PlanningException {
    preHook(ctx, stack, expr);

    RESULT current;
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
      case Having:
        current = visitHaving(ctx, stack, (Having) expr);
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
      case TablePrimaryTableSubQuery:
        current = visitTableSubQuery(ctx, stack, (TablePrimarySubQuery) expr);
        break;
      case Relation:
        current = visitRelation(ctx, stack, (Relation) expr);
        break;
      case Insert:
        current = visitInsert(ctx, stack, (Insert) expr);
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

  protected RESULT visitInsert(CONTEXT ctx, Stack<OpType> stack, Insert expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getSubQuery());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitProjection(CONTEXT ctx, Stack<OpType> stack, Projection expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitLimit(CONTEXT ctx, Stack<OpType> stack, Limit expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitSort(CONTEXT ctx, Stack<OpType> stack, Sort expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitHaving(CONTEXT ctx, Stack<OpType> stack, Having expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitGroupBy(CONTEXT ctx, Stack<OpType> stack, Aggregation expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitJoin(CONTEXT ctx, Stack<OpType> stack, Join expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getLeft());
    visitChild(ctx, stack, expr.getRight());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitFilter(CONTEXT ctx, Stack<OpType> stack, Selection expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getChild());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitUnion(CONTEXT ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getLeft());
    visitChild(ctx, stack, expr.getRight());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitExcept(CONTEXT ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getLeft());
    visitChild(ctx, stack, expr.getRight());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitIntersect(CONTEXT ctx, Stack<OpType> stack, SetOperation expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getLeft());
    visitChild(ctx, stack, expr.getRight());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitRelationList(CONTEXT ctx, Stack<OpType> stack, RelationList expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = null;
    for (Expr e : expr.getRelations()) {
      child = visitChild(ctx, stack, e);
    }
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitTableSubQuery(CONTEXT ctx, Stack<OpType> stack, TablePrimarySubQuery expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = visitChild(ctx, stack, expr.getSubQuery());
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitRelation(CONTEXT ctx, Stack<OpType> stack, Relation expr) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitCreateTable(CONTEXT ctx, Stack<OpType> stack, CreateTable expr) throws PlanningException {
    stack.push(expr.getType());
    RESULT child = null;
    if (expr.hasSubQuery()) {
      child = visitChild(ctx, stack, expr.getSubQuery());
    }
    stack.pop();
    return child;
  }

  @Override
  public RESULT visitDropTable(CONTEXT ctx, Stack<OpType> stack, DropTable expr) throws PlanningException {
    return null;
  }
}
