/**
 * 
 */
package nta.engine.planner.global;

import java.util.Collection;
import java.util.Iterator;

import nta.engine.planner.PlannerUtil;
import nta.engine.planner.global.LogicalQueryUnit.PARTITION_TYPE;
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
  
  public LogicalQueryUnitGraph optimize(LogicalQueryUnit logicalUnit) {
    LogicalQueryUnit reducedStep = reduceLogicalQueryUnitStep(logicalUnit);
    LogicalQueryUnit joinChosen = chooseJoinAlgorithm(reducedStep);
    return new LogicalQueryUnitGraph(joinChosen);
  }
  
  @VisibleForTesting
  private LogicalQueryUnit chooseJoinAlgorithm(LogicalQueryUnit logicalUnit) {
    
    return logicalUnit;
  }
  
  @VisibleForTesting
  private LogicalQueryUnit reduceLogicalQueryUnitStep(LogicalQueryUnit logicalUnit) {
    reduceLogicalQueryUnitStep_(logicalUnit);
    return logicalUnit;
  }
  
  private void reduceLogicalQueryUnitStep_(LogicalQueryUnit cur) {
    if (cur.hasPrevQuery()) {
      Iterator<LogicalQueryUnit> it = cur.getPrevIterator();
      LogicalQueryUnit prev;
      while (it.hasNext()) {
        prev = it.next();
        reduceLogicalQueryUnitStep_(prev);
      }
      
      Collection<LogicalQueryUnit> prevs = cur.getPrevQueries();
      it = prevs.iterator();
      while (it.hasNext()) {
        prev = it.next();
        if (prev.getOutputType() == PARTITION_TYPE.LIST) {
          mergeLogicalUnits(cur, prev);
        }
      }
    }
  }
  
  private LogicalQueryUnit mergeLogicalUnits(LogicalQueryUnit parent, 
      LogicalQueryUnit child) {
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
