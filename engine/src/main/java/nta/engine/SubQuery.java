/**
 * 
 */
package nta.engine;

import nta.engine.planner.global.LogicalQueryUnitGraph;

/**
 * @author jihoon
 *
 */
public class SubQuery {

  private final SubQueryId id;
  private final LogicalQueryUnitGraph plan;
  
  public SubQuery(SubQueryId id, LogicalQueryUnitGraph plan) {
    this.id = id;
    this.plan = plan;
  }
  
  public SubQueryId getId() {
    return this.id;
  }
  
  public LogicalQueryUnitGraph getPlan() {
    return this.plan;
  }
}
