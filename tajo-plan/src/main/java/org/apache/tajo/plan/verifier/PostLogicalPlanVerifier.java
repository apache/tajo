/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.verifier;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TooLargeInputForCrossJoinException;
import org.apache.tajo.exception.TooLargeResultForCrossJoinException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.joinorder.GreedyHeuristicJoinOrderAlgorithm;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlanVerifierUtil;
import org.apache.tajo.plan.verifier.PostLogicalPlanVerifier.InputContext;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.TUtil;

import java.util.Set;
import java.util.Stack;

public class PostLogicalPlanVerifier extends BasicLogicalPlanVisitor<InputContext, Object> {

  public static class VerifyContext {
    private long broadcastThresholdForNonCrossJoin;
    private long broadcastThresholdForCrossJoin;
    private long crossJoinResultThreshold;

    public VerifyContext(long broadcastThresholdForNonCrossJoin,
                         long broadcastThresholdForCrossJoin,
                         long crossJoinResultThreshold) {
      this.broadcastThresholdForNonCrossJoin = broadcastThresholdForNonCrossJoin;
      this.broadcastThresholdForCrossJoin = broadcastThresholdForCrossJoin;
      this.crossJoinResultThreshold = crossJoinResultThreshold;
    }
  }

  static class InputContext {
    VerifyContext verifyContext;
    VerificationState state;
    double estimatedResultSize;
    double estimatedRowNum;
    Set<String> nonBroadcastableRelations = TUtil.newHashSet();

    public InputContext(VerificationState state) {
      this.state = state;
    }
  }

  public VerificationState verify(VerifyContext verifyContext, VerificationState state, LogicalPlan plan)
      throws TajoException {
    InputContext context = new InputContext(state);
    context.verifyContext = verifyContext;
    visit(context, plan, plan.getRootBlock());
    return context.state;
  }

  @Override
  public Object visitJoin(InputContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                          Stack<LogicalNode> stack) throws TajoException {
    stack.push(node);
    visit(context, plan, block, node.getLeftChild(), stack);
    double estimatedLeftResultSize = context.estimatedResultSize;
    double estimatedLeftRowNum = context.estimatedRowNum;
    visit(context, plan, block, node.getRightChild(), stack);
    stack.pop();

    context.estimatedRowNum = PlanVerifierUtil.estimateOutputRowNumForJoin(node.getJoinSpec(),
        estimatedLeftRowNum, context.estimatedRowNum);
    context.estimatedResultSize = context.estimatedRowNum *
        SchemaUtil.estimateRowByteSizeWithSchema(node.getOutSchema());

    if (node.getJoinType() == JoinType.CROSS) {
      /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      // Cross join is one of the most heavy operations. To avoid the trouble caused by exhausting resources to perform
      // cross join, we allow it only when it does not burden cluster too much.
      //
      // If the join type is cross, the following two restrictions are checked.
      // 1) The expected result size does not exceed the predefined threshold.
      // 2) Cross join must be executed with broadcast join.
      //
      // For the second restriction, the following two conditions must be satisfied.
      // 1) There is at most a single relation which size is greater than the broadcast join threshold for non-cross
      // join.
      // 2) At least one of the cross join's inputs must not exceed the broadcast join threshold for cross join.
      /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      if (estimatedLeftResultSize > context.verifyContext.broadcastThresholdForCrossJoin &&
          context.estimatedResultSize > context.verifyContext.broadcastThresholdForCrossJoin) {
        context.state.addVerification(new TooLargeInputForCrossJoinException(
            context.nonBroadcastableRelations.toArray(new String[context.nonBroadcastableRelations.size()]),
            context.verifyContext.broadcastThresholdForCrossJoin
        ));
      }

      if (context.nonBroadcastableRelations.size() > 1) {
        context.state.addVerification(new TooLargeInputForCrossJoinException(
            context.nonBroadcastableRelations.toArray(new String[context.nonBroadcastableRelations.size()]),
            context.verifyContext.broadcastThresholdForCrossJoin
        ));
      }

      if (context.verifyContext.crossJoinResultThreshold < context.estimatedResultSize) {
        context.state.addVerification(
            new TooLargeResultForCrossJoinException(context.verifyContext.crossJoinResultThreshold));
      }

    }
    return null;
  }

  @Override
  public Object visitProjection(InputContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, ProjectionNode node,
                                Stack<LogicalNode> stack) throws TajoException {
    super.visitProjection(context, plan, block, node, stack);
    context.estimatedResultSize *= (double) SchemaUtil.estimateRowByteSizeWithSchema(node.getOutSchema()) /
        (double) SchemaUtil.estimateRowByteSizeWithSchema(node.getInSchema());
    return null;
  }

  @Override
  public Object visitLimit(InputContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, LimitNode node,
                           Stack<LogicalNode> stack) throws TajoException {
    super.visitLimit(context, plan, block, node, stack);
    context.estimatedRowNum = node.getFetchFirstNum();
    context.estimatedResultSize = node.getFetchFirstNum() *
        SchemaUtil.estimateRowByteSizeWithSchema(node.getOutSchema());
    return null;
  }

  @Override
  public Object visitFilter(InputContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, SelectionNode node,
                            Stack<LogicalNode> stack) throws TajoException {
    super.visitFilter(context, plan, block, node, stack);
    context.estimatedResultSize *= GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR;
    context.estimatedRowNum *= GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR;
    return null;
  }

  @Override
  public Object visitUnion(InputContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, UnionNode node,
                           Stack<LogicalNode> stack) throws TajoException {
    stack.push(node);
    if (plan != null) {
      LogicalPlan.QueryBlock leftBlock = plan.getBlock(node.getLeftChild());
      visit(context, plan, leftBlock, leftBlock.getRoot(), stack);
      double estimatedLeftResultSize = context.estimatedResultSize;
      double estimatedLeftRowNum = context.estimatedRowNum;
      LogicalPlan.QueryBlock rightBlock = plan.getBlock(node.getRightChild());
      visit(context, plan, rightBlock, rightBlock.getRoot(), stack);
      context.estimatedResultSize += estimatedLeftResultSize;
      context.estimatedRowNum += estimatedLeftRowNum;
    } else {
      visit(context, null, null, node.getLeftChild(), stack);
      double estimatedLeftResultSize = context.estimatedResultSize;
      double estimatedLeftRowNum = context.estimatedRowNum;
      visit(context, null, null, node.getRightChild(), stack);
      context.estimatedResultSize += estimatedLeftResultSize;
      context.estimatedRowNum += estimatedLeftRowNum;
    }

    stack.pop();
    return null;
  }

  @Override
  public Object visitScan(InputContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                          Stack<LogicalNode> stack) throws TajoException {
    context.estimatedResultSize = PlanVerifierUtil.getTableVolume(node);
    context.estimatedRowNum = context.estimatedResultSize /
        (double) SchemaUtil.estimateRowByteSizeWithSchema(node.getTableDesc().getLogicalSchema());
    if (context.estimatedResultSize > context.verifyContext.broadcastThresholdForNonCrossJoin) {
      context.nonBroadcastableRelations.add(node.getTableDesc().getName());
    }
    return null;
  }

  @Override
  public Object visitPartitionedTableScan(InputContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                          PartitionedTableScanNode node, Stack<LogicalNode> stack)
      throws TajoException {
    context.estimatedResultSize = PlanVerifierUtil.getTableVolume(node);
    context.estimatedRowNum = context.estimatedResultSize /
        (double) SchemaUtil.estimateRowByteSizeWithSchema(node.getTableDesc().getLogicalSchema());
    if (context.estimatedResultSize > context.verifyContext.broadcastThresholdForNonCrossJoin) {
      context.nonBroadcastableRelations.add(node.getTableDesc().getName());
    }
    return null;
  }
}
