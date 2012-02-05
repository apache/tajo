package nta.catalog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

import nta.catalog.proto.CatalogProtos.ColumnProto;
import nta.catalog.proto.CatalogProtos.ColumnProtoOrBuilder;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.common.ProtoObject;
import nta.engine.json.GsonCreator;

/**
 * @author Hyunsik Choi
 */
public class Column implements ProtoObject<ColumnProto> {
	private ColumnProto proto = ColumnProto.getDefaultInstance();
	private ColumnProto.Builder builder = null;
	private boolean viaProto = false;
	
	@Expose
  protected String name;
	@Expose
  protected DataType dataType;
	
	public Column() {
		this.builder = ColumnProto.newBuilder();
	}
	  
	public Column(String columnName, DataType dataType) {
		this.name = columnName;
		this.dataType = dataType;
		this.builder = ColumnProto.newBuilder();
	}
	
	public Column(ColumnProto proto) {
		this.proto = proto;
		this.viaProto = true;
	}
	

	public String getName() {
		ColumnProtoOrBuilder p = viaProto ? proto : builder;
		if(name != null) {
			return this.name;
		}
		if(!p.hasColumnName()) {
			return null;			
		}		
		this.name = p.getColumnName();
		
		return this.name;
	}
	
  public boolean isQualifiedName() {
    return getName().split("\\.").length == 2;
  }

  public String getTableName() {
    return getName().split("\\.")[0];
  }

  public String getColumnName() {
    if (isQualifiedName())
      return this.name.split("\\.")[1];
    else
      return name;
  }
	
	public void setName(String name) {
		maybeInitBuilder();
		this.name = name;
	}
	
	public DataType getDataType() {
		ColumnProtoOrBuilder p = viaProto ? proto : builder;
		if(dataType != null) {
			return this.dataType;
		}
		if(!p.hasDataType()) {
			return null;
		}
		this.dataType = p.getDataType();
		
		return this.dataType;
	}
	
	public void setDataType(DataType dataType) {
		maybeInitBuilder();
		this.dataType = dataType;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Column) {
			Column cd = (Column)o;
			if (this.getName().equals(cd.getName()) &&
					this.getDataType() == cd.getDataType()
					) {
				return true;
			}
		}
		return false;
	}
	
  public int hashCode() {
    return getName().hashCode() ^ (getDataType().hashCode() * 17);
  }

	@Override
	public ColumnProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}
	
	private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = ColumnProto.newBuilder(proto);
		}
		viaProto = false;
	}
	
	private void mergeLocalToBuilder() {
		if (this.name != null) {
			builder.setColumnName(this.name);			
		}
		if (this.dataType != null) {
			builder.setDataType(this.dataType);
		}
	}
	
	private void mergeLocalToProto() {
		if(viaProto) {
			maybeInitBuilder();
		}
		mergeLocalToBuilder();
		proto = builder.build();
		viaProto = true;
	}
	
	public String toString() {
	  return getName() +" " + getDataType();
	}
	
	public String toJSON() {
		initFromProto();
		return GsonCreator.getInstance().toJson(this);
	}

	@Override
	public void initFromProto() {
		ColumnProtoOrBuilder p = viaProto ? proto : builder;
		if (this.name == null && p.hasColumnName()) {
			this.name = p.getColumnName();
		}
		if (this.dataType == null && p.hasDataType()) {
			this.dataType = p.getDataType();
		}
	}
}