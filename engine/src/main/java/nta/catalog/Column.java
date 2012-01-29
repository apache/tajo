package nta.catalog;

import nta.catalog.proto.CatalogProtos.ColumnProto;
import nta.catalog.proto.CatalogProtos.ColumnProtoOrBuilder;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.common.ProtoObject;

/**
 * @author Hyunsik Choi
 */
public class Column extends ColumnBase implements ProtoObject<ColumnProto> {
	private ColumnProto proto = ColumnProto.getDefaultInstance();
	private ColumnProto.Builder builder = null;
	private boolean viaProto = false;
	
	// volatile variable
	private Integer id;
	  
	public Column(int columnId, String columnName, DataType dataType) {		
	  super(columnName, dataType);
		setId(columnId);
		this.builder = ColumnProto.newBuilder();
	}
	
	public Column(ColumnProto proto) {
	  super(proto.getColumnName(), proto.getDataType());
		this.proto = proto;
		this.viaProto = true;
	}
	
	public Integer getId() {
		ColumnProtoOrBuilder p = viaProto ? proto : builder;
		if(id != null) {
			return this.id;
		}
		if(!p.hasColumnId()) {
			return null;
		}
		this.id = p.getColumnId();
		
		return this.id;
	}
	
	public void setId(int columnId) {
		maybeInitBuilder();
		this.id = columnId;
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
	
	@Override
	public int hashCode() {
	  return this.getName().hashCode();
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
		if (this.id  != null) {			
			builder.setColumnId(this.id);
		}
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
}