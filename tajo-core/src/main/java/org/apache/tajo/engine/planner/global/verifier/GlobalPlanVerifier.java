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

package org.apache.tajo.engine.planner.global.verifier;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.exception.TooLargeInputForCrossJoinException;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.verifier.VerificationState;
import org.apache.tajo.util.graph.DirectedGraphVisitor;

import java.util.Stack;

public class GlobalPlanVerifier implements DirectedGraphVisitor<ExecutionBlockId> {
  private MasterPlan plan;
  private VerificationState state;

  public VerificationState verify(MasterPlan plan) {
    this.plan = plan;
    this.state = new VerificationState();
    plan.accept(plan.getRoot().getId(), this);
    return state;
  }

  @Override
  public void visit(Stack<ExecutionBlockId> stack, ExecutionBlockId executionBlockId) {
    ExecutionBlock block = plan.getExecBlock(executionBlockId);
    if (block.hasJoin()) {
      LogicalNode[] joinNodes = PlannerUtil.findAllNodes(block.getPlan(), NodeType.JOIN);
      boolean containCrossJoin = false;
      for (LogicalNode eachNode : joinNodes) {
        if (((JoinNode)eachNode).getJoinType() == JoinType.CROSS) {
          containCrossJoin = true;
          break;
        }
      }
      if (containCrossJoin) {
        // In the case of cross join, this execution block must be executed with broadcast join.
        // 
        if (block.getBroadcastRelations().size() == 0 || block.getNonBroadcastRelNum() > 1) {
          String[] relNames = new String[block.getScanNodes().length];
          for (int i = 0; i < relNames.length; i++) {
            relNames[i] = block.getScanNodes()[i].getCanonicalName();
          }
          state.addVerification(new TooLargeInputForCrossJoinException(new String[]{}));
        }
      }
    }
  }
}
