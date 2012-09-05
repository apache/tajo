/**
 * 
 */
package tajo.engine.planner;

import tajo.SubqueryContext;
import tajo.engine.exception.InternalException;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.physical.PhysicalExec;

/**
 * This class generates a physical execution plan.
 * 
 * @author Hyunsik Choi
 * 
 */
public interface PhysicalPlanner {
  public PhysicalExec createPlan(SubqueryContext context,
                                 LogicalNode logicalPlan)
      throws InternalException;
}
