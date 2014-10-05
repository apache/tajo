package org.apache.tajo.engine.planner.rewrite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.IndexDesc;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.AccessPathInfo.ScanTypeControl;
import org.apache.tajo.engine.planner.logical.IndexScanNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.RelationNode;
import org.apache.tajo.engine.planner.logical.ScanNode;

import java.util.List;
import java.util.Map.Entry;
import java.util.Stack;

public class AccessPathRewriter implements RewriteRule {
  private static final Log LOG = LogFactory.getLog(AccessPathRewriter.class);

  private static final String NAME = "Access Path Rewriter";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      for (Entry<RelationNode, List<AccessPathInfo>> relationAccessInfo : block.getRelationAccessInfos().entrySet()) {
        // If there are any alternative access paths
        if (relationAccessInfo.getValue().size() > 1) {
          for (AccessPathInfo accessPathInfo : relationAccessInfo.getValue()) {
            if (accessPathInfo.getScanType() == ScanTypeControl.INDEX_SCAN) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    return null;
  }

  private final class ReWriter extends BasicLogicalPlanVisitor<Object, Object> {
    @Override
    public Object visitScan(Object object, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode scanNode,
                            Stack<LogicalNode> stack) throws PlanningException {
      List<AccessPathInfo> accessPaths = block.getAccessInfos(scanNode);
      // find the optimal path
      IndexScanInfo optimalPath = null;
      for (AccessPathInfo accessPath : accessPaths) {
        if (accessPath.getScanType() == ScanTypeControl.INDEX_SCAN) {

        }
      }
      //
      plan.addHistory("AccessPathRewriter chooses " + optimalPath.getIndexDesc().getName() + " for "
          + scanNode.getTableName() + " scan");
      IndexDesc indexDesc = optimalPath.getIndexDesc();
      IndexScanNode indexScanNode = new IndexScanNode(plan.newPID(), scanNode,
          )
    }
  }
}
