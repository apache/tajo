package org.apache.tajo.engine.planner.global.rewriter.rules;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.global.rewriter.GlobalPlanRewriteRule;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.util.graph.DirectedGraphVisitor;

import java.util.Stack;

public class UnionReduceRule implements GlobalPlanRewriteRule {

  @Override
  public String getName() {
    return "UnionReduceRule";
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, MasterPlan plan) {
    for (LogicalPlan.QueryBlock block : plan.getLogicalPlan().getQueryBlocks()) {
      if (block.hasNode(NodeType.UNION)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public MasterPlan rewrite(MasterPlan plan) throws PlanningException {
    Rewriter.rewrite(plan);
    return plan;
  }

  static class Rewriter implements DirectedGraphVisitor<ExecutionBlockId> {
    private static Rewriter instance;
    private static MasterPlan plan;

    private Rewriter() {}

    public static void rewrite(MasterPlan plan) {
      if (instance == null) {
        instance = new Rewriter();
      }
      instance.plan = plan;
      instance.visit(new Stack<ExecutionBlockId>(), plan.getTerminalBlock().getId());
    }

    @Override
    public void visit(Stack<ExecutionBlockId> stack, ExecutionBlockId executionBlockId) {
      // must have the form of
      /*
            operator
               |
             union
             /   \
            op   op
       */
    }
  }
}
