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

package tajo.engine.planner.global;

import com.google.common.annotations.VisibleForTesting;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.logical.ScanNode;
import tajo.engine.planner.logical.UnaryNode;
import tajo.master.SubQuery;
import tajo.master.SubQuery.PARTITION_TYPE;

import java.util.Collection;
import java.util.Iterator;

public class GlobalOptimizer {

  public GlobalOptimizer() {
    
  }
  
  public MasterPlan optimize(MasterPlan plan) {
    SubQuery reducedStep = reduceSchedules(plan.getRoot());
    SubQuery joinChosen = chooseJoinAlgorithm(reducedStep);

    MasterPlan optimized = new MasterPlan(joinChosen);
    optimized.setOutputTableName(plan.getOutputTable());

    return optimized;
  }
  
  @VisibleForTesting
  private SubQuery chooseJoinAlgorithm(SubQuery logicalUnit) {
    
    return logicalUnit;
  }
  
  @VisibleForTesting
  private SubQuery reduceSchedules(SubQuery logicalUnit) {
    reduceLogicalQueryUnitStep_(logicalUnit);
    return logicalUnit;
  }
  
  private void reduceLogicalQueryUnitStep_(SubQuery cur) {
    if (cur.hasChildQuery()) {
      Iterator<SubQuery> it = cur.getChildIterator();
      SubQuery prev;
      while (it.hasNext()) {
        prev = it.next();
        reduceLogicalQueryUnitStep_(prev);
      }
      
      Collection<SubQuery> prevs = cur.getChildQueries();
      it = prevs.iterator();
      while (it.hasNext()) {
        prev = it.next();
        if (prev.getStoreTableNode().getSubNode().getType() != ExprType.UNION &&
            prev.getOutputType() == PARTITION_TYPE.LIST) {
          mergeLogicalUnits(cur, prev);
        }
      }
    }
  }
  
  private SubQuery mergeLogicalUnits(SubQuery parent,
      SubQuery child) {
    LogicalNode p = PlannerUtil.findTopParentNode(parent.getLogicalPlan(), 
        ExprType.SCAN);
//    Preconditions.checkArgument(p instanceof UnaryNode);
    if (p instanceof UnaryNode) {
      UnaryNode u = (UnaryNode) p;
      ScanNode scan = (ScanNode) u.getSubNode();
      LogicalNode c = child.getStoreTableNode().getSubNode();

      parent.removeChildQuery(scan);
      u.setSubNode(c);
      parent.setLogicalPlan(parent.getLogicalPlan());
      parent.addChildQueries(child.getChildMaps());
    }
    return parent;
  }
}
