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

import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;

import java.util.Stack;

public class BroadcastJoinPlanVisitor extends BasicLogicalPlanVisitor<GlobalPlanner.GlobalPlanContext, LogicalNode> {

  private boolean leftBase;

  public BroadcastJoinPlanVisitor(boolean leftBase) {
    this.leftBase = leftBase;
  }

  @Override
  public LogicalNode visitJoin(GlobalPlanner.GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
    if (leftBase) {
      return findLeftBaseBroadcastJoin(context, plan, block, node, stack);
    } else {
      return findRightBaseBroadcastJoin(context, plan, block, node, stack);
    }
  }

  private LogicalNode findRightBaseBroadcastJoin(GlobalPlanner.GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                 JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode leftChild = node.getLeftChild();
    LogicalNode rightChild = node.getRightChild();

    if (leftChild.getType() == NodeType.JOIN  && isScanNode(rightChild)) {
      node.getRightBaseBroadcastCandidateTargets().add(node);
    }
    LogicalNode parentNode = stack.peek();
    if (parentNode != null && parentNode.getType() == NodeType.JOIN) {
      node.getRightBaseBroadcastCandidateTargets().addAll(((JoinNode)parentNode).getRightBaseBroadcastCandidateTargets());
    }

    Stack<LogicalNode> currentStack = new Stack<LogicalNode>();
    currentStack.push(node);
    if(!isScanNode(leftChild)) {
      visit(context, plan, block, leftChild, currentStack);
    }

    if(!isScanNode(rightChild)) {
      visit(context, plan, block, rightChild, currentStack);
    }
    currentStack.pop();

    return node;
  }

  private LogicalNode findLeftBaseBroadcastJoin(GlobalPlanner.GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode leftChild = node.getLeftChild();
    LogicalNode rightChild = node.getRightChild();

    if (isScanNode(leftChild) && isScanNode(rightChild)) {
      node.setCandidateBroadcast(true);
      node.getLeftBaseBroadcastCandidateTargets().add(leftChild);
      node.getLeftBaseBroadcastCandidateTargets().add(rightChild);
      return node;
    }

    if(!isScanNode(leftChild)) {
      visit(context, plan, block, leftChild, stack);
    }

    if(!isScanNode(rightChild)) {
      visit(context, plan, block, rightChild, stack);
    }

    if(isBroadcastCandidateNode(leftChild) && isBroadcastCandidateNode(rightChild)) {
      node.setCandidateBroadcast(true);
      if(leftChild.getType() == NodeType.JOIN) {
        node.getLeftBaseBroadcastCandidateTargets().addAll(((JoinNode)leftChild).getLeftBaseBroadcastCandidateTargets());
      } else {
        node.getLeftBaseBroadcastCandidateTargets().add(leftChild);
      }

      if(rightChild.getType() == NodeType.JOIN) {
        node.getLeftBaseBroadcastCandidateTargets().addAll(((JoinNode)rightChild).getLeftBaseBroadcastCandidateTargets());
      } else {
        node.getLeftBaseBroadcastCandidateTargets().add(rightChild);
      }
    }

    return node;
  }

  public static boolean isBroadcastCandidateNode(LogicalNode node) {
    if(node.getType() == NodeType.SCAN ||
        node.getType() == NodeType.PARTITIONS_SCAN) {
      return true;
    }

    if(node.getType() == NodeType.JOIN && ((JoinNode)node).isCandidateBroadcast()) {
      return true;
    }

    return false;
  }

  private static boolean isScanNode(LogicalNode node) {
    return node.getType() == NodeType.SCAN ||
        node.getType() == NodeType.PARTITIONS_SCAN;
  }
}
