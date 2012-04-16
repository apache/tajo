/**
 * 
 */
package nta.engine.planner.global;

/**
 * @author jihoon
 *
 */
public class MasterPlan {

  private ScheduleUnit root;
  
  public MasterPlan() {
    root = null;
  }
  
  public MasterPlan(ScheduleUnit root) {
    setRoot(root);
  }
  
  public void setRoot(ScheduleUnit root) {
    this.root = root;
  }
  
  public ScheduleUnit getRoot() {
    return this.root;
  }
}
