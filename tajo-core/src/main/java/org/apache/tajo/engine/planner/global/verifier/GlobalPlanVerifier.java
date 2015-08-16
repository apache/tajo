package org.apache.tajo.engine.planner.global.verifier;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
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
        assert block.getBroadcastRelations().size() > 0;
        assert block.getNonBroadcastRelNum() < 2;
      }
    }
  }
}
