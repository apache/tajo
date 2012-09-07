/**
 * 
 */
package tajo.engine.planner.global;

import com.google.common.annotations.VisibleForTesting;
import tajo.engine.planner.PlannerUtil;
import tajo.master.SubQuery;
import tajo.master.SubQuery.PARTITION_TYPE;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.logical.ScanNode;
import tajo.engine.planner.logical.UnaryNode;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author jihoon
 *
 */
public class GlobalOptimizer {

  public GlobalOptimizer() {
    
  }
  
  public MasterPlan optimize(SubQuery logicalUnit) {
    SubQuery reducedStep = reduceSchedules(logicalUnit);
    SubQuery joinChosen = chooseJoinAlgorithm(reducedStep);
    return new MasterPlan(joinChosen);
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
