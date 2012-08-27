package tajo.engine.planner.logical;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import tajo.catalog.Schema;
import tajo.datum.Datum;
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock.SortSpec;

public class IndexScanNode extends ScanNode {
  
  @Expose private SortSpec[]sortKeys;
  @Expose private Schema keySchema = null;
  @Expose private Datum[] datum = null;
  //TODO- @Expose private IndexType type;
  
  public IndexScanNode(ScanNode scanNode , 
      Schema keySchema , Datum[] datum, SortSpec[] sortKeys ) {
    super();
    setQual(scanNode.getQual());
    setFromTable(scanNode.getFromTable());
    setInputSchema(scanNode.getInputSchema());
    setOutputSchema(scanNode.getOutputSchema());
    setLocal(scanNode.isLocal());
    setTargets(scanNode.getTargets());
    setType(ExprType.BST_INDEX_SCAN);
    this.sortKeys = sortKeys;
    this.keySchema = keySchema;
    this.datum = datum;
  }
  
  public SortSpec[] getSortKeys() {
    return this.sortKeys;
  }
  
  public Schema getKeySchema() {
    return this.keySchema;
  }
  
  public Datum[] getDatum() {
    return this.datum;
  }
  
  public void setSortKeys(SortSpec[] sortKeys) {
    this.sortKeys = sortKeys;
  }
  
  public void setKeySchema( Schema keySchema ) {
    this.keySchema = keySchema;
  }
  
  @Override
  public String toJSON() {
    GsonCreator.getInstance().toJson(this, LogicalNode.class);
    return null;
  }
  @Override
  public String toString() {
    Gson gson = GsonCreator.getInstance();
    StringBuilder builder = new StringBuilder();
    builder.append("IndexScanNode : {\n");
    builder.append("  \"keySchema\" : \"" + gson.toJson(this.keySchema) + "\"\n");
    builder.append("  \"sortKeys\" : \"" + gson.toJson(this.sortKeys) + " \"\n");
    builder.append("  \"datums\" : \"" + gson.toJson(this.datum) + "\"\n");
    builder.append("      <<\"superClass\" : " + super.toString());
    builder.append(">>}");
    builder.append("}");
    return builder.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IndexScanNode) {
      IndexScanNode other = (IndexScanNode) obj;
      
      boolean eq = super.equals(other);
      eq = eq && this.sortKeys.length == other.sortKeys.length;
      if(eq) {
        for(int i = 0 ; i < this.sortKeys.length ; i ++) {
          eq = eq && this.sortKeys[i].getSortKey().equals(
              other.sortKeys[i].getSortKey());
          eq = eq && this.sortKeys[i].isAscending()
              == other.sortKeys[i].isAscending();
          eq = eq && this.sortKeys[i].isNullFirst()
              == other.sortKeys[i].isNullFirst();
        }
      }
      if(eq) {
        for(int i = 0 ; i < this.datum.length ; i ++ ) {
          eq = eq && this.datum[i].equals(other.datum[i]);
        }
      }
     return eq;
    }   
    return false;
  } 
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    IndexScanNode indexNode = (IndexScanNode) super.clone();
    indexNode.keySchema = (Schema) this.keySchema.clone();
    indexNode.sortKeys = new SortSpec[this.sortKeys.length];
    for(int i = 0 ; i < sortKeys.length ; i ++ )
      indexNode.sortKeys[i] = (SortSpec) this.sortKeys[i].clone();
    indexNode.datum = new Datum[this.datum.length];
    for(int i = 0 ; i < datum.length ; i ++ ) {
      indexNode.datum[i] = this.datum[i];
    }
    return indexNode;
  }
}


