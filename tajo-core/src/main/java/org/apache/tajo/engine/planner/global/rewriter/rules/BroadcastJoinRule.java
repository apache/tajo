package org.apache.tajo.engine.planner.global.rewriter.rules;

import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.global.rewriter.GlobalPlanRewriteRule;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.NodeType;

public class BroadcastJoinRule implements GlobalPlanRewriteRule {

  @Override
  public String getName() {
    return "BroadcastJoinRule";
  }

  @Override
  public boolean isEligible(MasterPlan plan) {
    for (LogicalPlan.QueryBlock block : plan.getLogicalPlan().getQueryBlocks()) {
      if (block.hasNode(NodeType.JOIN)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public MasterPlan rewrite(MasterPlan plan) {
    rewrite(plan, plan.getTerminalBlock());
    return plan;
  }

  private void rewrite(MasterPlan plan, ExecutionBlock current) {
    if (plan.isLeaf(current)) {
      // compute input size
    } else {
      for (ExecutionBlock child : plan.getChilds(current)) {
        rewrite(plan, child);
      }
      if (current.hasJoin()) {
        for (ExecutionBlock child : plan.getChilds(current)) {
          if (child.hasBroadcastTable()) {
            merge(plan, child, current);
          }
        }
      }
    }
  }

  private ExecutionBlock merge(MasterPlan plan, ExecutionBlock child, ExecutionBlock parent) {
    

    return parent;
  }
}
