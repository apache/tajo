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

package org.apache.tajo.engine.planner;

import com.google.common.base.Preconditions;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.logical.*;

import java.util.Stack;

public class LogicalPlanVerifier extends BasicLogicalPlanVisitor<VerificationState, LogicalNode> {
  private TajoConf conf;
  private CatalogService catalog;

  public LogicalPlanVerifier(TajoConf conf, CatalogService catalog) {
    this.conf = conf;
    this.catalog = catalog;
  }

  /**
   * It checks if an output schema of a projectable node and target's output data types are equivalent to each other.
   */
  private static void verifyProjectableOutputSchema(Projectable node) throws PlanningException {

    Schema outputSchema = node.getOutSchema();
    Schema targetSchema = PlannerUtil.targetToSchema(node.getTargets());

    if (outputSchema.size() != node.getTargets().length) {
      throw new PlanningException(String.format("Output schema and Target's schema are mismatched at Node (%d)",
          + node.getPID()));
    }

    for (int i = 0; i < outputSchema.size(); i++) {
      if (!outputSchema.getColumn(i).getDataType().equals(targetSchema.getColumn(i).getDataType())) {
        Column targetColumn = targetSchema.getColumn(i);
        Column insertColumn = outputSchema.getColumn(i);
        throw new PlanningException("ERROR: " +
            insertColumn.getSimpleName() + " is of type " + insertColumn.getDataType().getType().name() +
            ", but target column '" + targetColumn.getSimpleName() + "' is of type " +
            targetColumn.getDataType().getType().name());
      }
    }
  }

  @Override
  public LogicalNode visitProjection(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitProjection(state, plan, block, node, stack);

    for (Target target : node.getTargets()) {
      ExprsVerifier.verify(state, node, target.getEvalTree());
    }

    verifyProjectableOutputSchema(node);

    return node;
  }

  @Override
  public LogicalNode visitLimit(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                LimitNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitLimit(state, plan, block, node, stack);

    if (node.getFetchFirstNum() < 0) {
      state.addVerification("LIMIT must not be negative");
    }

    return node;
  }

  @Override
  public LogicalNode visitGroupBy(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                  GroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitGroupBy(state, plan, block, node, stack);

    verifyProjectableOutputSchema(node);
    return node;
  }

  @Override
  public LogicalNode visitFilter(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    visit(state, plan, block, node.getChild(), stack);
    ExprsVerifier.verify(state, node, node.getQual());
    return node;
  }

  @Override
  public LogicalNode visitJoin(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                               Stack<LogicalNode> stack) throws PlanningException {
    visit(state, plan, block, node.getLeftChild(), stack);
    visit(state, plan, block, node.getRightChild(), stack);

    if (node.hasJoinQual()) {
      ExprsVerifier.verify(state, node, node.getJoinQual());
    }

    verifyProjectableOutputSchema(node);

    return node;
  }

  private void verifySetStatement(VerificationState state, BinaryNode setNode) {
    Preconditions.checkArgument(setNode.getType() == NodeType.UNION || setNode.getType() == NodeType.INTERSECT ||
      setNode.getType() == NodeType.EXCEPT);
    Schema left = setNode.getLeftChild().getOutSchema();
    Schema right = setNode.getRightChild().getOutSchema();
    NodeType type = setNode.getType();

    if (left.size() != right.size()) {
      state.addVerification("each " + type.name() + " query must have the same number of columns");
      return;
    }

    Column[] leftColumns = left.toArray();
    Column[] rightColumns = right.toArray();

    for (int i = 0; i < leftColumns.length; i++) {
      if (!leftColumns[i].getDataType().equals(rightColumns[i].getDataType())) {
        state.addVerification(type + " types " + leftColumns[i].getDataType().getType() + " and "
            + rightColumns[i].getDataType().getType() + " cannot be matched");
      }
    }
  }

  @Override
  public LogicalNode visitUnion(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                UnionNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitUnion(state, plan, block, node, stack);
    verifySetStatement(state, node);
    return node;
  }

  @Override
  public LogicalNode visitExcept(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 ExceptNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitExcept(state, plan, block, node, stack);
    verifySetStatement(state, node);
    return node;
  }

  @Override
  public LogicalNode visitIntersect(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    IntersectNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitIntersect(state, plan, block, node, stack);
    verifySetStatement(state, node);
    return node;
  }

  @Override
  public LogicalNode visitScan(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                               Stack<LogicalNode> stack) throws PlanningException {
    if (node.hasTargets()) {
      for (Target target : node.getTargets()) {
        ExprsVerifier.verify(state, node, target.getEvalTree());
      }
    }

    if (node.hasQual()) {
      ExprsVerifier.verify(state, node, node.getQual());
    }

    verifyProjectableOutputSchema(node);

    return node;
  }

  @Override
  public LogicalNode visitStoreTable(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     StoreTableNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitStoreTable(state, plan, block, node, stack);
    return node;
  }

  @Override
  public LogicalNode visitInsert(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 InsertNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitInsert(state, plan, block, node, stack);
    return node;
  }

  /**
   * This ensures that corresponding columns in both tables are equivalent to each other.
   */
  private static void ensureDomains(VerificationState state, Schema targetTableScheme, Schema schema)
      throws PlanningException {
    for (int i = 0; i < schema.size(); i++) {
      if (!schema.getColumn(i).getDataType().equals(targetTableScheme.getColumn(i).getDataType())) {
        Column targetColumn = targetTableScheme.getColumn(i);
        Column insertColumn = schema.getColumn(i);
        state.addVerification("ERROR: " +
            insertColumn.getSimpleName() + " is of type " + insertColumn.getDataType().getType().name() +
            ", but target column '" + targetColumn.getSimpleName() + "' is of type " +
            targetColumn.getDataType().getType().name());
      }
    }
  }

  @Override
  public LogicalNode visitCreateTable(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                      CreateTableNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitCreateTable(state, plan, block, node, stack);

    if (catalog.existsTable(node.getTableName())) {
      state.addVerification("relation \"" + node.getTableName() + "\" already exists");
    }

    return node;
  }

  @Override
  public LogicalNode visitDropTable(VerificationState state, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    DropTableNode node, Stack<LogicalNode> stack) {
    if (!catalog.existsTable(node.getTableName())) {
      state.addVerification("table \"" + node.getTableName() + "\" does not exist");
    }

    return node;
  }
}
