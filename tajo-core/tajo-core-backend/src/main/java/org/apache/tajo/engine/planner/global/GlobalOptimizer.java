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

package org.apache.tajo.engine.planner.global;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.logical.ExprType;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.engine.planner.logical.UnaryNode;
import org.apache.tajo.master.ExecutionBlock;
import org.apache.tajo.master.ExecutionBlock.PartitionType;

public class GlobalOptimizer {

  public GlobalOptimizer() {
    
  }
  
  public MasterPlan optimize(MasterPlan plan) {
    ExecutionBlock reducedStep = reduceSchedules(plan.getRoot());

    MasterPlan optimized = new MasterPlan(reducedStep);
    optimized.setOutputTableName(plan.getOutputTable());

    return optimized;
  }
  
  @VisibleForTesting
  private ExecutionBlock reduceSchedules(ExecutionBlock logicalUnit) {
    reduceLogicalQueryUnitStep_(logicalUnit);
    return logicalUnit;
  }

  private void reduceLogicalQueryUnitStep_(ExecutionBlock cur) {
    if (cur.hasChildBlock()) {
      for (ExecutionBlock childBlock: cur.getChildBlocks())
        reduceLogicalQueryUnitStep_(childBlock);
    }

    for (ExecutionBlock childBlock: cur.getChildBlocks()) {
      if (childBlock.getStoreTableNode().getSubNode().getType() != ExprType.UNION &&
          childBlock.getPartitionType() == PartitionType.LIST) {
        mergeLogicalUnits(cur, childBlock);
      }
    }
  }
  
  private ExecutionBlock mergeLogicalUnits(ExecutionBlock parent, ExecutionBlock child) {
    LogicalNode p = PlannerUtil.findTopParentNode(parent.getPlan(), ExprType.SCAN);

    if (p instanceof UnaryNode) {
      UnaryNode u = (UnaryNode) p;
      ScanNode scan = (ScanNode) u.getSubNode();
      LogicalNode c = child.getStoreTableNode().getSubNode();

      parent.removeChildBlock(scan);
      u.setSubNode(c);
      parent.setPlan(parent.getPlan());
      parent.addChildBlocks(child.getChildBlockMap());
    }
    return parent;
  }
}
