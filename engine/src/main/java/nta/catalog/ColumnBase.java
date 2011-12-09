package nta.catalog;

import nta.catalog.proto.TableProtos.AttrType;
import nta.catalog.proto.TableProtos.DataType;

public class ColumnBase {
	protected String name;
	protected DataType dataType;
	protected AttrType attrType;
	
	public ColumnBase() {}
	
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
	
	public DataType getDataType() {
		return this.dataType;
	}
	
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}
	
	public AttrType getAttrType() {
		return this.attrType;
	}
	
	public void setAttrType(AttrType attrType) {
		this.attrType = attrType;
	}
	
	public String toString() {
		return name+" ("+dataType.toString()+")";
	}
}
