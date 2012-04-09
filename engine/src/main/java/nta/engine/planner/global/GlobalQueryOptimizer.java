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
import com.google.common.base.Preconditions;

/**
 * @author jihoon
 *
 */
public class GlobalQueryOptimizer {

  public GlobalQueryOptimizer() {
    
  }
  
  public LogicalQueryUnitGraph optimize(ScheduleUnit logicalUnit) {
    ScheduleUnit reducedStep = reduceLogicalQueryUnitStep(logicalUnit);
    ScheduleUnit joinChosen = chooseJoinAlgorithm(reducedStep);
    return new LogicalQueryUnitGraph(joinChosen);
  }
  
  @VisibleForTesting
  private ScheduleUnit chooseJoinAlgorithm(ScheduleUnit logicalUnit) {
    
    return logicalUnit;
  }
  
  @VisibleForTesting
  private ScheduleUnit reduceLogicalQueryUnitStep(ScheduleUnit logicalUnit) {
    reduceLogicalQueryUnitStep_(logicalUnit);
    return logicalUnit;
  }
  
  private void reduceLogicalQueryUnitStep_(ScheduleUnit cur) {
    if (cur.hasPrevQuery()) {
      Iterator<ScheduleUnit> it = cur.getPrevIterator();
      ScheduleUnit prev;
      while (it.hasNext()) {
        prev = it.next();
        reduceLogicalQueryUnitStep_(prev);
      }
      
      Collection<ScheduleUnit> prevs = cur.getPrevQueries();
      it = prevs.iterator();
      while (it.hasNext()) {
        prev = it.next();
        if (prev.getOutputType() == PARTITION_TYPE.LIST) {
          mergeLogicalUnits(cur, prev);
        }
      }
    }
  }
  
  private ScheduleUnit mergeLogicalUnits(ScheduleUnit parent, 
      ScheduleUnit child) {
    LogicalNode p = PlannerUtil.findTopParentNode(parent.getLogicalPlan(), 
        ExprType.SCAN);
    Preconditions.checkArgument(p instanceof UnaryNode);
    ScanNode scan = (ScanNode) ((UnaryNode)p).getSubNode();
    LogicalNode c = child.getStoreTableNode().getSubNode();
    UnaryNode u = (UnaryNode) p;
    u.setSubNode(c);

    parent.removePrevQuery(scan);
    parent.setLogicalPlan(parent.getLogicalPlan());
    parent.addPrevQueries(child.getPrevMaps());
    return parent;
  }
}
