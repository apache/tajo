/**
 * 
 */
package nta.catalog;

import nta.catalog.proto.CatalogProtos.IndexDescProto;
import nta.catalog.proto.CatalogProtos.IndexDescProtoOrBuilder;
import nta.catalog.proto.CatalogProtos.IndexMethod;
import nta.common.ProtoObject;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 */
public class IndexDesc implements ProtoObject<IndexDescProto>, Cloneable {
  private IndexDescProto proto;
  private IndexDescProto.Builder builder;
  private boolean viaProto;
  
  @Expose private String name;
  @Expose private String tableId;
  @Expose private Column column;
  @Expose private IndexMethod indexMethod;
  @Expose private boolean isUnique = false;
  @Expose private boolean isClustered = false;
  @Expose private boolean isAscending = false;
  
  public IndexDesc() {
    this.builder = IndexDescProto.newBuilder();
  }
  
  public IndexDesc(String name, String tableId, Column column, IndexMethod type,
      boolean isUnique, boolean isClustered, boolean isAscending) {
    this();
    this.name = name;
    this.tableId = tableId;
    this.column = column;
    this.indexMethod = type;
    this.isUnique = isUnique;
    this.isClustered = isClustered;
    this.isAscending = isAscending;
  }
  
  public IndexDesc(IndexDescProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public String getName() {
    IndexDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.name != null) {
      return name;
    }
    if (!p.hasName()) {
      return null;
    }
    this.name = p.getName();
    
    return name;
  }
  
  public String getTableId() {
    IndexDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.tableId != null) {
      return tableId;
    }
    if (!p.hasTableId()) {
      return null;
    }
    this.tableId = p.getTableId();
    
    return tableId;
  }
  
  public Column getColumn() {
    IndexDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.column != null) {
      return column;
    }
    if (!p.hasColumn()) {
      return null;
    }
    this.column = new Column(p.getColumn());
    
    return column;
  }
  
  public IndexMethod getIndexMethod() {
    IndexDescProtoOrBuilder p = viaProto ? proto : builder;    
    if (this.indexMethod != null) {
      return this.indexMethod;
    }
    if (!p.hasIndexMethod()) { // if isCluster == false and proto has no set
      return null;
    }
    this.indexMethod = p.getIndexMethod();
    
    return this.indexMethod;
  }
  
  public boolean isClustered() {
    IndexDescProtoOrBuilder p = viaProto ? proto : builder;
    if (isClustered) {
      return true;
    }
    if (!p.hasIsClustered()) { // if isCluster == false and proto has no set
      return false;
    }
    this.isClustered = p.getIsClustered();
    
    return this.isClustered;
  }
  
  public boolean isUnique() {
    IndexDescProtoOrBuilder p = viaProto ? proto : builder;    
    if (isUnique) {
      return true;
    }
    if (!p.hasIsUnique()) { // if isCluster == false and proto has no set
      return false;
    }
    this.isUnique = p.getIsUnique();
    
    return this.isUnique;
  }
  
  public boolean isAscending() {
    IndexDescProtoOrBuilder p = viaProto ? proto : builder;    
    if (isAscending) {
      return true;
    }
    if (!p.hasIsAscending()) { // if isCluster == false and proto has no set
      return false;
    }
    this.isAscending = p.getIsAscending();
    
    return this.isAscending;
  }

  @Override
  public IndexDescProto getProto() {
    if(!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    
    return proto;
  }
  
  @SuppressWarnings("unused")
  private void setModified() {
    if (viaProto && builder == null) {
      builder = IndexDescProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  private void mergeLocalToBuilder() {
    if (builder == null) {
      builder = IndexDescProto.newBuilder(proto);
    }
    if (this.name != null) {
      builder.setName(this.name);
    }
    if (this.tableId != null) {
      builder.setTableId(this.tableId);
    }
    if (this.column != null) {
      builder.setColumn(this.column.getProto());
    }
    if (this.indexMethod != null) {
      builder.setIndexMethod(indexMethod);
    }
    if (this.isUnique) {
      builder.setIsUnique(this.isUnique);
    }
    if (this.isClustered) {
      builder.setIsClustered(this.isClustered);
    }
    if (this.isAscending) {
      builder.setIsAscending(this.isAscending);
    }
  }
  
  @Override
  public void initFromProto() {
    IndexDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.name == null && p.hasName()) {
      this.name = p.getName();
    }
    if (this.tableId == null && p.hasTableId()) {
      this.tableId = p.getTableId();
    }
    if (this.column == null && p.hasColumn()) {
      this.column = new Column(p.getColumn());
    }
    if (this.indexMethod == null && p.hasIndexMethod()) {
      this.indexMethod = p.getIndexMethod();
    }
    if (this.isUnique == false && p.hasIsUnique()) {
      this.isUnique = p.getIsUnique();
    }
    if (this.isClustered == false && p.hasIsClustered()) {
      this.isUnique = p.getIsUnique();
    }
    if (this.isAscending == false && p.hasIsAscending()) {
      this.isAscending = p.getIsAscending();
    }
    viaProto = false;
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof IndexDesc) {
      IndexDesc other = (IndexDesc) obj;
      return getName().equals(other.getName())
          && getTableId().equals(other.getTableId())
          && getColumn().equals(other.getColumn())
          && getIndexMethod().equals(other.getIndexMethod())
          && isUnique() == other.isUnique()
          && isClustered() == other.isClustered()
          && isAscending() == other.isAscending();
    } else {
      return false;
    }
  }
  
  public int hashCode() {
    return Objects.hashCode(getName(), getTableId(), getColumn(), 
        getIndexMethod(), isUnique(), isClustered(), isAscending());
  }
  
  public Object clone() throws CloneNotSupportedException {
    IndexDesc desc = (IndexDesc) super.clone();
    initFromProto();
    desc.name = name;
    desc.tableId = tableId;
    desc.column = (Column) column.clone();
    desc.indexMethod = indexMethod;
    desc.isUnique = isUnique;
    desc.isClustered = isClustered;
    desc.isAscending = isAscending;
    return desc; 
  }
  
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }
}