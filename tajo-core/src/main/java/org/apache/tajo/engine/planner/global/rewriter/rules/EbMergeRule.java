package org.apache.tajo.engine.planner.global.rewriter.rules;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.global.rewriter.GlobalPlanRewriteRule;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.logical.*;

public class EbMergeRule implements GlobalPlanRewriteRule {
  private GlobalPlanRewriteUtil.ParentFinder parentFinder = new GlobalPlanRewriteUtil.ParentFinder();

  @Override
  public String getName() {
    return "EbMergeRule";
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, MasterPlan plan) {
    return true;
  }

  @Override
  public MasterPlan rewrite(MasterPlan plan) throws PlanningException {
    return null;
  }

  private ExecutionBlock mergeTwoPhaseNonJoin(MasterPlan plan, ExecutionBlock child, ExecutionBlock parent)
      throws PlanningException {

    ScanNode scanForChild = GlobalPlanRewriteUtil.findScanForChildEb(child, parent);
    if (scanForChild == null) {
      throw new PlanningException("Cannot find any scan nodes for " + child.getId() + " in " + parent.getId());
    }

    parentFinder.set(scanForChild);
    parentFinder.find(parent.getPlan());
    LogicalNode parentOfScanForChild = parentFinder.getFound();
    if (parentOfScanForChild == null) {
      throw new PlanningException("Cannot find the parent of " + scanForChild.getCanonicalName());
    }

    LogicalNode rootOfChild = child.getPlan();
    if (rootOfChild.getType() == NodeType.STORE) {
      rootOfChild = ((StoreTableNode)rootOfChild).getChild();
    }
    LogicalNode mergedPlan;
    if (rootOfChild.getType() == parentOfScanForChild.getType()) {
      // merge two-phase plan into one-phase plan.
      // remove the second-phase plan.
      LogicalNode firstPhaseNode = rootOfChild;
      LogicalNode secondPhaseNode = parentOfScanForChild;

      parentFinder.set(parentOfScanForChild);
      parentFinder.find(parent.getPlan());
      parentOfScanForChild = parentFinder.getFound();

      if (parentOfScanForChild == null) {
        // assume that the node which will be merged is the root node of the plan of the parent eb.
        mergedPlan = firstPhaseNode;
      } else {
        GlobalPlanRewriteUtil.replaceChild(firstPhaseNode, scanForChild, parentOfScanForChild);
        mergedPlan = parent.getPlan();
      }

      if (firstPhaseNode.getType() == NodeType.GROUP_BY) {
        GroupbyNode firstPhaseGroupby = (GroupbyNode) firstPhaseNode;
        GroupbyNode secondPhaseGroupby = (GroupbyNode) secondPhaseNode;
        for (AggregationFunctionCallEval aggFunc : firstPhaseGroupby.getAggFunctions()) {
          aggFunc.setFirstAndLastPhase();
        }
        firstPhaseGroupby.setTargets(secondPhaseGroupby.getTargets());
        firstPhaseGroupby.setOutSchema(secondPhaseGroupby.getOutSchema());
      }
    } else {
      mergedPlan = parent.getPlan();
    }

    parent = GlobalPlanRewriteUtil.mergeExecutionBlocks(plan, child, parent);

    if (parent.getEnforcer().hasEnforceProperty(TajoWorkerProtocol.EnforceProperty.EnforceType.SORTED_INPUT)) {
      parent.getEnforcer().removeSortedInput(scanForChild.getTableName());
    }

    parent.setPlan(mergedPlan);

    return parent;
  }

}
