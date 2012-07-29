/**
 * 
 */
package nta.engine.planner.global;

import java.util.Collection;
import java.util.Iterator;

import nta.engine.planner.PlannerUtil;
import nta.engine.planner.global.ScheduleUnit.PARTITION_TYPE;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.UnaryNode;

import com.google.common.annotations.VisibleForTesting;

/**
 * @author jihoon
 *
 */
public class GlobalOptimizer {

  public GlobalOptimizer() {
    
  }
  
  public MasterPlan optimize(ScheduleUnit logicalUnit) {
    ScheduleUnit reducedStep = reduceSchedules(logicalUnit);
    ScheduleUnit joinChosen = chooseJoinAlgorithm(reducedStep);
    return new MasterPlan(joinChosen);
  }
  
  @VisibleForTesting
  private ScheduleUnit chooseJoinAlgorithm(ScheduleUnit logicalUnit) {
    
    return logicalUnit;
  }
  
  @VisibleForTesting
  private ScheduleUnit reduceSchedules(ScheduleUnit logicalUnit) {
    reduceLogicalQueryUnitStep_(logicalUnit);
    return logicalUnit;
  }
  
  private void reduceLogicalQueryUnitStep_(ScheduleUnit cur) {
    if (cur.hasChildQuery()) {
      Iterator<ScheduleUnit> it = cur.getChildIterator();
      ScheduleUnit prev;
      while (it.hasNext()) {
        prev = it.next();
        reduceLogicalQueryUnitStep_(prev);
      }
      
      Collection<ScheduleUnit> prevs = cur.getChildQueries();
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
  
  private ScheduleUnit mergeLogicalUnits(ScheduleUnit parent, 
      ScheduleUnit child) {
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
