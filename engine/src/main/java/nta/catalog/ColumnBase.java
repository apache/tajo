package nta.catalog;

import nta.catalog.proto.CatalogProtos.DataType;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class ColumnBase {
	protected String name;
	protected DataType dataType;
	
	public ColumnBase(String columnName, DataType dataType) {
		this.name = columnName;
		this.dataType = dataType;
	}
	
	public String getName() {
		return this.name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public boolean isQualifiedName() {
	  return this.name.split(".").length == 2;
	}
	
	public String getTableName() {
	  return this.name.split(".")[0];
	}
	
	public String getColumnName() {
	  if(isQualifiedName())
	    return this.name.split(".")[1];
	  else
	    return name;
  }
	
	public DataType getDataType() {
		return this.dataType;
	}
	
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}
	
	public String toString() {
		return name+" "+dataType.toString();
	}
	
	public int hashCode() {
	  return name.hashCode() ^ (dataType.hashCode() * 17);
	}
	
	public boolean equals(Object obj) {
	  if(obj instanceof ColumnBase) {
	    ColumnBase cb = (ColumnBase) obj;
	    return this.name.equals(cb.name) && this.dataType.equals(cb.dataType);
	  }
	  
	  return false;
	}
}