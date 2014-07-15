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
import org.apache.tajo.engine.planner.logical.ScanNode;

import java.util.Stack;

public class BroadcastJoinPlanVisitor extends BasicLogicalPlanVisitor<GlobalPlanner.GlobalPlanContext, LogicalNode> {
  @Override
  public LogicalNode visitJoin(GlobalPlanner.GlobalPlanContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode leftChild = node.getLeftChild();
    LogicalNode rightChild = node.getRightChild();

    if (leftChild.getType() == NodeType.JOIN  && ScanNode.isScanNode(rightChild)) {
      node.getBroadcastCandidateTargets().add(node);
    }
    LogicalNode parentNode = stack.peek();
    if (parentNode != null && parentNode.getType() == NodeType.JOIN) {
      node.getBroadcastCandidateTargets().addAll(((JoinNode)parentNode).getBroadcastCandidateTargets());
    }

    Stack<LogicalNode> currentStack = new Stack<LogicalNode>();
    currentStack.push(node);
    if(!ScanNode.isScanNode(leftChild)) {
      visit(context, plan, block, leftChild, currentStack);
    }

    if(!ScanNode.isScanNode(rightChild)) {
      visit(context, plan, block, rightChild, currentStack);
    }
    currentStack.pop();

    return node;
  }
}
