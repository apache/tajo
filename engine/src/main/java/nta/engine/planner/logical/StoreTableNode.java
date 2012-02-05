package nta.engine.planner.logical;

import com.google.gson.annotations.Expose;

import nta.engine.json.GsonCreator;

/**
 * @author Hyunsik Choi
 * 
 */
public class StoreTableNode extends UnaryNode {
  @Expose
  private String tableName;

  public StoreTableNode(String tableName) {
    super(ExprType.STORE);
    this.tableName = tableName;
  }

  public String getTableName() {
    return this.tableName;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"Store\": {\"table\": \""+tableName+"\",")
    .append("\n  \"out schema\": ").append(getOutputSchema()).append(",")
    .append("\n  \"in schema\": ").append(getInputSchema())
    .append("}");
    
    return sb.toString() + "\n"
        + getSubNode().toString();
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
