/**
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
import org.apache.tajo.catalog.*;
import org.apache.tajo.exception.*;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.util.FilteredPartitionInfo;
import org.apache.tajo.plan.util.PartitionedTableUtil;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.io.IOException;
import java.util.*;

public class PartitionedTableRewriter implements LogicalPlanRewriteRule {
  private static final Log LOG = LogFactory.getLog(PartitionedTableRewriter.class);
  private CatalogService catalog;
  private static final String NAME = "Partitioned Table Rewriter";
  private final Rewriter rewriter = new Rewriter();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlanRewriteRuleContext context) {
    for (LogicalPlan.QueryBlock block : context.getPlan().getQueryBlocks()) {
      for (RelationNode relation : block.getRelations()) {
        if (relation.getType() == NodeType.SCAN) {
          TableDesc table = ((ScanNode)relation).getTableDesc();
          if (table.hasPartition()) {
            return true;
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
    this.catalog = context.getCatalog();
    rewriter.visit(context.getQueryContext(), plan, rootBlock, rootBlock.getRoot(), new Stack<>());
    return plan;
  }

  public FilteredPartitionInfo findFilteredPartitionInfo(OverridableConf conf,
    PartitionedTableScanNode partitionedTableScanNode) throws IOException, UndefinedDatabaseException,
    UndefinedTableException, UndefinedPartitionMethodException, UndefinedOperatorException, UnsupportedException {
    return PartitionedTableUtil.findFilteredPartitionInfo(catalog, conf.getConf(), partitionedTableScanNode);
  }

  private final class Rewriter extends BasicLogicalPlanVisitor<OverridableConf, Object> {
    @Override
    public Object visitScan(OverridableConf queryContext, LogicalPlan plan, LogicalPlan.QueryBlock block,
                            ScanNode scanNode, Stack<LogicalNode> stack) throws TajoException {

      TableDesc table = scanNode.getTableDesc();
      if (!table.hasPartition()) {
        return null;
      }

      PartitionedTableScanNode rewrittenScanNode = plan.createNode(PartitionedTableScanNode.class);
      rewrittenScanNode.init(scanNode);

      try {
        // If PartitionedTableScanNode doesn't have correct table volume, broadcast join might not run occasionally. 
        FilteredPartitionInfo filteredPartitionInfo = findFilteredPartitionInfo(queryContext, rewrittenScanNode);
        rewrittenScanNode.getTableDesc().getStats().setNumBytes(filteredPartitionInfo.getTotalVolume());
      } catch (IOException e) {
        throw new TajoInternalError("Partitioned Table Rewrite Failed: \n" + e.getMessage());
      }

      // if it is topmost node, set it as the rootnode of this block.
      if (stack.empty() || block.getRoot().equals(scanNode)) {
        block.setRoot(rewrittenScanNode);
      } else {
        PlannerUtil.replaceNode(plan, stack.peek(), scanNode, rewrittenScanNode);
      }
      block.registerNode(rewrittenScanNode);

      return null;
    }
  }
}
