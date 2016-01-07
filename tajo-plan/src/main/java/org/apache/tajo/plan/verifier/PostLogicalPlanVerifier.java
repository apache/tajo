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
import org.apache.tajo.exception.InvalidInputsForCrossJoin;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TooLargeInputForCrossJoinException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.verifier.PostLogicalPlanVerifier.Context;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.unit.StorageUnit;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 *
 * PostLogicalPlanVerifier verifies the logical plan with some physical information.
 */
public class PostLogicalPlanVerifier extends BasicLogicalPlanVisitor<Context, Object> {

  static class Context {
    long bcastLimitForCrossJoin;
    VerificationState state;

    public Context(VerificationState state, long bcastLimitForCrossJoin) {
      this.state = state;
      this.bcastLimitForCrossJoin = bcastLimitForCrossJoin;
    }
  }

  public VerificationState verify(long broadcastThresholdForCrossJoin, VerificationState state, LogicalPlan plan)
      throws TajoException {
    Context context = new Context(state, broadcastThresholdForCrossJoin);
    visit(context, plan, plan.getRootBlock());
    return context.state;
  }

  @Override
  public Object visitJoin(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                          Stack<LogicalNode> stack) throws TajoException {
    super.visitJoin(context, plan, block, node, stack);

    if (node.getJoinType() == JoinType.CROSS) {

      /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      //
      // Cross join is one of the most heavy operations. To avoid the trouble caused by exhausting resources to perform
      // cross join, we allow it only when it does not burden cluster too much.
      //
      // Currently, we simply allow the cross join only when it has at least one of inputs is a broadcastable relation.
      // However, we can lose a lot of possible opportunities because this rule is too simple.
      // This rule must be improved as follows.
      //
      // If the join type is cross, the following two restrictions are checked.
      // 1) The expected result size does not exceed the predefined threshold.
      // 2) Cross join must be executed with broadcast join.
      //
      // For the second restriction, the following two conditions must be satisfied.
      // 1) There is at most a single relation which size is greater than the broadcast join threshold for non-cross
      // join.
      // 2) At least one of the cross join's inputs must not exceed the broadcast join threshold for cross join.
      //
      /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      if (!isSimpleRelationNode(node.getLeftChild()) && !isSimpleRelationNode(node.getRightChild())) {
        context.state.addVerification(new InvalidInputsForCrossJoin());
      } else {

        boolean crossJoinAllowed = false;
        List<String> largeRelationNames = new ArrayList<>();

        if (isSimpleRelationNode(node.getLeftChild())) {
          if (getTableVolume((ScanNode) node.getLeftChild()) <= context.bcastLimitForCrossJoin * StorageUnit.KB) {
            crossJoinAllowed = true;
          } else {
            largeRelationNames.add(((ScanNode) node.getLeftChild()).getCanonicalName());
          }
        }

        if (isSimpleRelationNode(node.getRightChild())) {
          if (getTableVolume((ScanNode) node.getRightChild()) <= context.bcastLimitForCrossJoin * StorageUnit.KB) {
            crossJoinAllowed = true;
          } else {
            largeRelationNames.add(((ScanNode) node.getRightChild()).getCanonicalName());
          }

          if (!crossJoinAllowed) {
            context.state.addVerification(new TooLargeInputForCrossJoinException(
                largeRelationNames.toArray(new String[largeRelationNames.size()]),
                context.bcastLimitForCrossJoin));
          }
        }
      }

    }
    return null;
  }

  private static boolean isSimpleRelationNode(LogicalNode node) {
    if (node instanceof ScanNode) {
      // PartitionedTableScanNode and IndexScanNode extends ScanNode.
      // TableSubqueryNode is not the simple relation node.
      return true;
    } else {
      return false;
    }
  }

  /**
   * Get a volume of a table of a partitioned table
   * @param scanNode ScanNode corresponding to a table
   * @return table volume (bytes)
   */
  private static long getTableVolume(ScanNode scanNode) {
    if (scanNode.getTableDesc().hasStats()) {
      long scanBytes = scanNode.getTableDesc().getStats().getNumBytes();
      if (scanNode.getType() == NodeType.PARTITIONS_SCAN) {
        PartitionedTableScanNode pScanNode = (PartitionedTableScanNode) scanNode;
        if (pScanNode.getInputPaths() == null || pScanNode.getInputPaths().length == 0) {
          scanBytes = 0L;
        }
      }

      return scanBytes;
    } else {
      return -1;
    }
  }
}
