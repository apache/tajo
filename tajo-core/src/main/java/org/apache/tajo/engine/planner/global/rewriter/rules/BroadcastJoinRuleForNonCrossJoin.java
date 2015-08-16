package org.apache.tajo.engine.planner.global.rewriter.rules;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.NodeType;

public class BroadcastJoinRuleForNonCrossJoin extends AbstractBroadcastJoinRule {

  @Override
  public String getName() {
    return "Broadcast join rule for non-cross join";
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, MasterPlan plan) {
    if (queryContext.getBool(SessionVars.TEST_BROADCAST_JOIN_ENABLED)) {
      for (LogicalPlan.QueryBlock block : plan.getLogicalPlan().getQueryBlocks()) {
        if (block.hasNode(NodeType.JOIN)) {
          long broadcastSizeThreshold = queryContext.getLong(SessionVars.BROADCAST_NON_CROSS_JOIN_THRESHOLD);
          if (broadcastSizeThreshold > 0) {
            init(RewriteScope.NON_CROSS, plan, broadcastSizeThreshold);
            return true;
          }
        }
      }
    }
    return false;
  }
}
