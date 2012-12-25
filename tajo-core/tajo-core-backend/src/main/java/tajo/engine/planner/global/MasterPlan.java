/**
 * 
 */
package tajo.engine.planner.global;

import tajo.master.SubQuery;

public class MasterPlan {
  private SubQuery root;
  private String outputTableName;
  
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

  public void setOutputTableName(String tableName) {
    this.outputTableName = tableName;
  }

  public String getOutputTable() {
    return outputTableName;
  }
}
