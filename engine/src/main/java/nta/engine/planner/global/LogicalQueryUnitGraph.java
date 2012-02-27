/**
 * 
 */
package nta.engine.planner.global;

/**
 * @author jihoon
 *
 */
public class LogicalQueryUnitGraph {

  private LogicalQueryUnit root;
  
  public LogicalQueryUnitGraph() {
    root = null;
  }
  
  public LogicalQueryUnitGraph(LogicalQueryUnit root) {
    setRoot(root);
  }
  
  public void setRoot(LogicalQueryUnit root) {
    this.root = root;
  }
  
  public LogicalQueryUnit getRoot() {
    return this.root;
  }
}
