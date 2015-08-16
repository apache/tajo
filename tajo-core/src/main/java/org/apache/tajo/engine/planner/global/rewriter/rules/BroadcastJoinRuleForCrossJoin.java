package org.apache.tajo.engine.planner.global.rewriter.rules;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.plan.LogicalPlan;

public class BroadcastJoinRuleForCrossJoin extends AbstractBroadcastJoinRule {

  @Override
  public String getName() {
    return "Broadcast join rule for cross join";
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, MasterPlan plan) {
    for (LogicalPlan.QueryBlock block : plan.getLogicalPlan().getQueryBlocks()) {
      // Broadcast join is enforced whether the SessionVars.TEST_BROADCAST_JOIN_ENABLED is set or not.

      // TODO: check containsJoinType()
      if (block.containsJoinType(JoinType.CROSS)) {
        // The broadcast threshold for cross join is always expected to be smaller than or equal to
        // that for non-cross join.
        long broadcastThreshold = queryContext.getLong(SessionVars.BROADCAST_CROSS_JOIN_THRESHOLD);
        broadcastThreshold = queryContext.getLong(SessionVars.BROADCAST_NON_CROSS_JOIN_THRESHOLD) < broadcastThreshold ?
            queryContext.getLong(SessionVars.BROADCAST_NON_CROSS_JOIN_THRESHOLD) : broadcastThreshold;

        if (broadcastThreshold > 0) {
          init(RewriteScope.CROSS, plan, broadcastThreshold);
          return true;
        }
      }
    }
    return false;
  }
}
