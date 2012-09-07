/**
 * 
 */
package tajo.engine.planner.global;

import tajo.master.SubQuery;

/**
 * @author jihoon
 *
 */
public class MasterPlan {

  private SubQuery root;
  
  public MasterPlan() {
    root = null;
  }
  
  public MasterPlan(SubQuery root) {
    setRoot(root);
  }
  
  public void setRoot(SubQuery root) {
    this.root = root;
  }
  
  public SubQuery getRoot() {
    return this.root;
  }
}
