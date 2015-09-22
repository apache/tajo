/*
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

package org.apache.tajo.plan.rewrite.rules;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.IndexScanNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.RelationNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.List;
import java.util.Stack;

public class AccessPathRewriter implements LogicalPlanRewriteRule {
  private static final Log LOG = LogFactory.getLog(AccessPathRewriter.class);

  private static final String NAME = "Access Path Rewriter";
  private Rewriter rewriter = new Rewriter();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlanRewriteRuleContext context) {
    if (context.getQueryContext().getBool(SessionVars.INDEX_ENABLED)) {
      for (LogicalPlan.QueryBlock block : context.getPlan().getQueryBlocks()) {
        for (RelationNode relationNode : block.getRelations()) {
          List<AccessPathInfo> accessPathInfos = block.getAccessInfos(relationNode);
          // If there are any alternative access paths
          if (accessPathInfos.size() > 1) {
            for (AccessPathInfo accessPathInfo : accessPathInfos) {
              if (accessPathInfo.getScanType() == AccessPathInfo.ScanTypeControl.INDEX_SCAN) {
                return true;
              }
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlanRewriteRuleContext context) throws TajoException {
    LogicalPlan plan = context.getPlan();
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();
    rewriter.init(context.getQueryContext());
    rewriter.visit(rootBlock, plan, rootBlock, rootBlock.getRoot(), new Stack<LogicalNode>());
    return plan;
  }

  private final class Rewriter extends BasicLogicalPlanVisitor<Object, Object> {

    private OverridableConf conf;

    public void init(OverridableConf conf) {
      this.conf = conf;
    }

    @Override
    public Object visitScan(Object object, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode scanNode,
                            Stack<LogicalNode> stack) throws TajoException {
      List<AccessPathInfo> accessPaths = block.getAccessInfos(scanNode);
      AccessPathInfo optimalPath = null;
      // initialize
      for (AccessPathInfo accessPath : accessPaths) {
        if (accessPath.getScanType() == AccessPathInfo.ScanTypeControl.SEQ_SCAN) {
          optimalPath = accessPath;
          break;
        }
      }
      // find the optimal path
      for (AccessPathInfo accessPath : accessPaths) {
        if (accessPath.getScanType() == AccessPathInfo.ScanTypeControl.INDEX_SCAN) {
          // estimation selectivity and choose the better path
          // TODO: improve the selectivity estimation
          double estimateSelectivity = 0.001;
          double selectivityThreshold = conf.getFloat(SessionVars.INDEX_SELECTIVITY_THRESHOLD);
          LOG.info("Selectivity threshold: " + selectivityThreshold);
          LOG.info("Estimated selectivity: " + estimateSelectivity);
          if (estimateSelectivity < selectivityThreshold) {
            // if the estimated selectivity is greater than threshold, use the index scan
            optimalPath = accessPath;
          }
        }
      }

      if (optimalPath != null && optimalPath.getScanType() == AccessPathInfo.ScanTypeControl.INDEX_SCAN) {
        IndexScanInfo indexScanInfo = (IndexScanInfo) optimalPath;
        plan.addHistory("AccessPathRewriter chooses index scan for " + scanNode.getTableName());
        IndexScanNode indexScanNode = new IndexScanNode(plan.newPID(), scanNode, indexScanInfo.getKeySchema(),
            indexScanInfo.getPredicates(), indexScanInfo.getIndexPath());
        if (stack.empty() || block.getRoot().equals(scanNode)) {
          block.setRoot(indexScanNode);
        } else {
          PlannerUtil.replaceNode(plan, stack.peek(), scanNode, indexScanNode);
        }
        block.registerNode(indexScanNode);
      }
      return null;
    }
  }
}
