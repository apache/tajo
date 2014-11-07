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
import org.apache.tajo.catalog.IndexDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.IndexScanNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.RelationNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.rewrite.RewriteRule;
import org.apache.tajo.plan.rewrite.rules.IndexScanInfo.SimplePredicate;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.List;
import java.util.Stack;

public class AccessPathRewriter implements RewriteRule {
  private static final Log LOG = LogFactory.getLog(AccessPathRewriter.class);

  private static final String NAME = "Access Path Rewriter";
  private final Rewriter rewriter = new Rewriter();
  private final AccessPathRewriterContext context;

  public static class AccessPathRewriterContext {
    boolean enableIndex;
    float selectivityThreshold;

    public AccessPathRewriterContext(boolean enableIndex) {
      this(enableIndex, ConfVars.$INDEX_SELECTIVITY_THRESHOLD.defaultFloatVal);
    }

    public AccessPathRewriterContext(boolean enableIndex, float selectivityThreshold) {
      this.enableIndex = enableIndex;
      this.selectivityThreshold = selectivityThreshold;
    }
  }

  public AccessPathRewriter(AccessPathRewriterContext context) {
    this.context = context;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    if (context.enableIndex) {
      for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
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

  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();
    rewriter.visit(rootBlock, plan, rootBlock, rootBlock.getRoot(), new Stack<LogicalNode>());
    return plan;
  }

  private final class Rewriter extends BasicLogicalPlanVisitor<Object, Object> {

    @Override
    public Object visitScan(Object object, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode scanNode,
                            Stack<LogicalNode> stack) throws PlanningException {
      List<AccessPathInfo> accessPaths = block.getAccessInfos(scanNode);
      AccessPathInfo optimalPath = null;
      // initialize
      for (AccessPathInfo accessPath : accessPaths) {
        if (accessPath.getScanType() == AccessPathInfo.ScanTypeControl.SEQ_SCAN) {
          optimalPath = accessPath;
        }
      }
      // find the optimal path
      for (AccessPathInfo accessPath : accessPaths) {
        if (accessPath.getScanType() == AccessPathInfo.ScanTypeControl.INDEX_SCAN) {
          // estimation selectivity and choose the better path
          // TODO: improve the selectivity estimation
          double estimateSelectivity = 0.001;
          LOG.info("Selectivity threshold: " + context.selectivityThreshold);
          LOG.info("Estimated selectivity: " + estimateSelectivity);
          if (estimateSelectivity < context.selectivityThreshold) {
            // if the estimated selectivity is greater than threshold, use the index scan
            optimalPath = accessPath;
          }
        }
      }

      if (optimalPath != null && optimalPath.getScanType() == AccessPathInfo.ScanTypeControl.INDEX_SCAN) {
        IndexScanInfo indexScanInfo = (IndexScanInfo) optimalPath;
        plan.addHistory("AccessPathRewriter chooses the index scan for " + scanNode.getTableName());
        IndexScanNode indexScanNode = new IndexScanNode(plan.newPID(), scanNode, indexScanInfo.getKeySchema(),
            indexScanInfo.getPredicates(), indexScanInfo.getIndexPath());
        if (stack.empty() || block.getRoot().equals(scanNode)) {
          block.setRoot(indexScanNode);
        } else {
          PlannerUtil.replaceNode(plan, stack.peek(), scanNode, indexScanNode);
        }
      }
      return null;
    }
  }
}
