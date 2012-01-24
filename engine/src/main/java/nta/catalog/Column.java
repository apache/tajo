package nta.catalog;

import nta.catalog.proto.TableProtos.ColumnProto;
import nta.catalog.proto.TableProtos.ColumnProtoOrBuilder;
import nta.catalog.proto.TableProtos.DataType;
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
	
	public Column() {
		this.builder = ColumnProto.newBuilder();
	}
	
	public Column(int columnId, String columnName, DataType dataType) {		
		this();
		setId(columnId);
		setName(columnName);
		setDataType(dataType);
	}
	
	public Column(ColumnProto proto) {
		this.proto = proto;
		this.viaProto = true;
	}
	
	public Integer getId() {
		ColumnProtoOrBuilder p = viaProto ? proto : builder;
		if(id != null) {
			return this.id;
		}
		if(!proto.hasColumnId()) {
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
		if(!proto.hasColumnName()) {
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
		if(!proto.hasDataType()) {
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
			if (this.name.equals(cd.getName()) &&
					this.getId() == cd.getId() &&
					this.getDataType() == cd.getDataType()
					) {
				return true;
			}
		}
		return false;
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