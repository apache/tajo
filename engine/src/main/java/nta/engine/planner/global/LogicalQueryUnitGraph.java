/**
 * 
 */
package nta.engine.planner.global;

/**
 * @author jihoon
 *
 */
public class LogicalQueryUnitGraph {

  private ScheduleUnit root;
  
  public LogicalQueryUnitGraph() {
    root = null;
  }
  
  public LogicalQueryUnitGraph(ScheduleUnit root) {
    setRoot(root);
  }
  
  public void setRoot(ScheduleUnit root) {
    this.root = root;
  }
  
  public ScheduleUnit getRoot() {
    return this.root;
  }
}
