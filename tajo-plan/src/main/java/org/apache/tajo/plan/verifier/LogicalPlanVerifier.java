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

package org.apache.tajo.plan.verifier;

import com.google.common.base.Preconditions;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.error.Errors;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.TUtil;

import java.util.Stack;

import static org.apache.tajo.plan.verifier.SyntaxErrorUtil.*;

public class LogicalPlanVerifier extends BasicLogicalPlanVisitor<LogicalPlanVerifier.Context, LogicalNode> {
  public LogicalPlanVerifier() {
  }

  public static class Context {
    VerificationState state;

    public Context(VerificationState state) {
      this.state = state;
    }
  }

  public VerificationState verify(VerificationState state, LogicalPlan plan)
      throws TajoException {
    Context context = new Context(state);
    visit(context, plan, plan.getRootBlock());
    return context.state;
  }

  /**
   * It checks if an output schema of a projectable node and target's output data types are equivalent to each other.
   */
  private static void verifyProjectableOutputSchema(Context context, Projectable node) throws TajoException {

    Schema outputSchema = node.getOutSchema();
    Schema targetSchema = PlannerUtil.targetToSchema(node.getTargets());

    if (outputSchema.size() != node.getTargets().length) {
      throw new TajoInternalError(String.format("Output schema and Target's schema are mismatched at Node (%d)",
          + node.getPID()));
    }

    for (int i = 0; i < outputSchema.size(); i++) {
      Column outputColumn = outputSchema.getColumn(i);

      if (outputColumn.getDataType().getType() == Type.RECORD) {
        context.state.addVerification(new UnsupportedException("record field in select list"));
      }

      if (!outputColumn.getDataType().equals(targetSchema.getColumn(i).getDataType())) {
        Column targetColumn = targetSchema.getColumn(i);
        Column insertColumn = outputColumn;
        throw new TajoInternalError(SyntaxErrorUtil.makeDataTypeMisMatch(insertColumn, targetColumn));
      }
    }
  }

  @Override
  public LogicalNode visitProjection(Context state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode node, Stack<LogicalNode> stack) throws TajoException {
    super.visitProjection(state, plan, block, node, stack);

    for (Target target : node.getTargets()) {
      ExprsVerifier.verify(state.state, node, target.getEvalTree());
    }

    verifyProjectableOutputSchema(state, node);

    return node;
  }

  @Override
  public LogicalNode visitLimit(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                LimitNode node, Stack<LogicalNode> stack) throws TajoException {
    super.visitLimit(context, plan, block, node, stack);

    if (node.getFetchFirstNum() < 0) {
      context.state.addVerification(makeSyntaxError("LIMIT must not be negative"));
    }

    return node;
  }

  @Override
  public LogicalNode visitGroupBy(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                  GroupbyNode node, Stack<LogicalNode> stack) throws TajoException {
    super.visitGroupBy(context, plan, block, node, stack);

    verifyProjectableOutputSchema(context, node);
    return node;
  }

  @Override
  public LogicalNode visitFilter(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode node, Stack<LogicalNode> stack) throws TajoException {
    visit(context, plan, block, node.getChild(), stack);
    ExprsVerifier.verify(context.state, node, node.getQual());
    return node;
  }

  @Override
  public LogicalNode visitJoin(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                               Stack<LogicalNode> stack) throws TajoException {
    visit(context, plan, block, node.getLeftChild(), stack);
    visit(context, plan, block, node.getRightChild(), stack);

    if (node.hasJoinQual()) {
      ExprsVerifier.verify(context.state, node, node.getJoinQual());
    }

    verifyProjectableOutputSchema(context, node);

    return node;
  }

  private void verifySetStatement(VerificationState state, BinaryNode setNode) {
    Preconditions.checkArgument(setNode.getType() == NodeType.UNION || setNode.getType() == NodeType.INTERSECT ||
      setNode.getType() == NodeType.EXCEPT);
    Schema left = setNode.getLeftChild().getOutSchema();
    Schema right = setNode.getRightChild().getOutSchema();
    NodeType type = setNode.getType();

    if (left.size() != right.size()) {
      state.addVerification(new TajoException(Errors.ResultCode.AMBIGUOUS_FUNCTION));
      return;
    }

    Column[] leftColumns = left.toArray();
    Column[] rightColumns = right.toArray();

    for (int i = 0; i < leftColumns.length; i++) {
      if (!leftColumns[i].getDataType().equals(rightColumns[i].getDataType())) {
        state.addVerification(
            makeSetOpDataTypeMisMatch(type, leftColumns[i].getDataType(), rightColumns[i].getDataType())
        );
      }
    }
  }

  @Override
  public LogicalNode visitUnion(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                UnionNode node, Stack<LogicalNode> stack) throws TajoException {
    super.visitUnion(context, plan, block, node, stack);
    verifySetStatement(context.state, node);
    return node;
  }

  @Override
  public LogicalNode visitExcept(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 ExceptNode node, Stack<LogicalNode> stack) throws TajoException {
    super.visitExcept(context, plan, block, node, stack);
    verifySetStatement(context.state, node);
    return node;
  }

  @Override
  public LogicalNode visitIntersect(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    IntersectNode node, Stack<LogicalNode> stack) throws TajoException {
    super.visitIntersect(context, plan, block, node, stack);
    verifySetStatement(context.state, node);
    return node;
  }

  @Override
  public LogicalNode visitTableSubQuery(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   TableSubQueryNode node, Stack<LogicalNode> stack) throws TajoException {
    super.visitTableSubQuery(context, plan, block, node, stack);
    if (node.hasTargets()) {
      for (Target target : node.getTargets()) {
        ExprsVerifier.verify(context.state, node, target.getEvalTree());
      }
    }

    verifyProjectableOutputSchema(context, node);
    return node;
  }

  @Override
  public LogicalNode visitScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                               Stack<LogicalNode> stack) throws TajoException {
    if (node.hasTargets()) {
      for (Target target : node.getTargets()) {
        ExprsVerifier.verify(context.state, node, target.getEvalTree());
      }
    }

    if (node.hasQual()) {
      ExprsVerifier.verify(context.state, node, node.getQual());
    }

    verifyProjectableOutputSchema(context, node);

    return node;
  }

  @Override
  public LogicalNode visitStoreTable(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     StoreTableNode node, Stack<LogicalNode> stack) throws TajoException {
    super.visitStoreTable(context, plan, block, node, stack);
    return node;
  }

  @Override
  public LogicalNode visitInsert(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 InsertNode node, Stack<LogicalNode> stack) throws TajoException {
    super.visitInsert(context, plan, block, node, stack);
    return node;
  }

  /**
   * This ensures that corresponding columns in both tables are equivalent to each other.
   */
  private static void ensureDomains(VerificationState state, Schema targetTableScheme, Schema schema)
      throws TajoException {
    for (int i = 0; i < schema.size(); i++) {

      // null can be used anywhere
      if (schema.getColumn(i).getDataType().getType() == Type.NULL_TYPE) {
        continue;
      }

      // checking castable between two data types
      if (!TUtil.containsInNestedMap(CatalogUtil.OPERATION_CASTING_MAP,
          schema.getColumn(i).getDataType().getType(), targetTableScheme.getColumn(i).getDataType().getType())) {
        Column targetColumn = targetTableScheme.getColumn(i);
        Column insertColumn = schema.getColumn(i);
        state.addVerification(makeDataTypeMisMatch(insertColumn, targetColumn));
      }
    }
  }

  @Override
  public LogicalNode visitCreateTable(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                      CreateTableNode node, Stack<LogicalNode> stack) throws TajoException {
    super.visitCreateTable(context, plan, block, node, stack);
    // here, we don't need check table existence because this check is performed in PreLogicalPlanVerifier.

    if (node.hasSubQuery()) {
      ensureDomains(context.state, node.getLogicalSchema(), node.getChild(0).getOutSchema());
    }
    return node;
  }

  @Override
  public LogicalNode visitDropTable(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    DropTableNode node, Stack<LogicalNode> stack) {
    // here, we don't need check table existence because this check is performed in PreLogicalPlanVerifier.
    return node;
  }
}
