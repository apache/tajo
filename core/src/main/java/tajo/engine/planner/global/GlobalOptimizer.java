/**
 * 
 */
package tajo.engine.planner.global;

import com.google.common.annotations.VisibleForTesting;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.global.ScheduleUnit.PARTITION_TYPE;
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
