/**
 * 
 */
package nta.engine.planner.logical;

import nta.catalog.Options;
import nta.catalog.proto.CatalogProtos.IndexMethod;
import nta.engine.json.GsonCreator;
import nta.engine.parser.CreateIndexStmt;
import nta.engine.parser.QueryBlock.SortSpec;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 */
public class IndexWriteNode extends UnaryNode {
  @Expose private String indexName;
  @Expose private boolean unique = false;
  @Expose private String tableName;
  @Expose private IndexMethod method = IndexMethod.TWO_LEVEL_BIN_TREE;
  @Expose private SortSpec [] sortSpecs;
  @Expose private Options params = null;

  public IndexWriteNode(CreateIndexStmt stmt) {
    super(ExprType.CREATE_INDEX);
    this.indexName = stmt.getIndexName();
    this.unique = stmt.isUnique();
    this.tableName = stmt.getTableName();
    this.method = stmt.getMethod();
    this.sortSpecs = stmt.getSortSpecs();
    this.params = stmt.hasParams() ? stmt.getParams() : null;
  }
  
  public String getIndexName() {
    return this.indexName;
  }
  
  public boolean isUnique() {
    return this.unique;
  }
  
  public void setUnique() {
    this.unique = true;
  }
  
  public String getTableName() {
    return this.tableName;
  }
  
  public IndexMethod getMethod() {
    return this.method;
  }
  
  public SortSpec [] getSortSpecs() {
    return this.sortSpecs;
  }
  
  public boolean hasParams() {
    return this.params != null;
  }
  
  public Options getParams() {
    return this.params;
  }

  public String toJSON() {
    for( int i = 0 ; i < this.sortSpecs.length ; i ++ ) {
      sortSpecs[i].getSortKey().initFromProto();
    }
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
  
  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }
}
