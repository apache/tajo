/**
 * 
 */
package nta.catalog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableProto;
import nta.catalog.proto.TableProtos.TableProtoOrBuilder;
import nta.catalog.proto.TableProtos.TableType;
import nta.storage.Store;

/**
 * @author Hyunsik Choi
 *
 */
public class TableInfo implements ProtoObject<TableProto>, Writable, Comparable<TableInfo> {
	protected TableProto proto = TableProto.getDefaultInstance();
	protected TableProto.Builder builder = null;
	protected boolean viaProto = false;	
	
	// TODO - visiability와 protocol buffer의 optional 정리 필요
	// volatile variables
	protected int tableId;
	protected String name;
	protected Store store; 
	
	// materialized variables
	protected StoreType storeType;
	protected TableType tableType;
	protected Schema schema;
	protected Options options;
	protected Long startKey;
	protected Long endKey;
	protected Long duration;
	
	public TableInfo() {
		builder = TableProto.newBuilder();
	}
	
	public TableInfo(TableProto proto) {
		this.proto = proto;
		this.viaProto = true;
	}
	
	public int getId() {
		return this.tableId;
	}
	
	public String getName() {
		return name;
	}
	
	public Store getStore() {
		return this.store;
	}
	
	public StoreType getStoreType() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		
		if(storeType != null) {
			return this.storeType;
		}
		if(!p.hasStoreType()) {
			return null;
		}
		this.storeType = p.getStoreType();
		
		return this.storeType;		
	}
	
	public TableType getTableType() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		
		if(tableType != null) {
			return this.tableType;
		}
		if(!p.hasTableType()) {
			return null;
		}
		this.tableType = p.getTableType();
		
		return this.tableType;
	}
	
	public Schema getSchema() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		
		if(schema != null) {
			return this.schema;
		}
		this.schema = new Schema(p.getSchema());
		
		return this.schema;
	}
	
	public Long getStartKey() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		
		if(startKey != null) {
			return this.startKey;
		}
		if(!p.hasStartKey()) {
			return null;
		}
		this.startKey = p.getStartKey();
		return this.startKey;
	}
	
	public Long getEndKey() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		
		if(endKey != null) {
			return this.endKey;
		}
		if(!p.hasEndKey()) {
			return null;
		}
		this.endKey = p.getEndKey();
		return this.endKey;
	}
	
	public Long getDuration() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		
		if(duration != null) {
			return this.duration;
		}
		if(!p.hasUnit()) {
			return null;
		}
		this.duration = p.getUnit();
		return this.duration;
	}
	
	public Options getOptions() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		if(this.options != null) {
			return this.options;
		}
		if(!p.hasOptions()) {
			return null;
		}
		this.options = new Options(p.getOptions());
		
		return this.options;
	}
	
	public String getOption(String key) {
		initOptions();
		return this.options.get(key);		
	}
	
	protected void initOptions() {
		if(this.options != null) {
			return;
		}
		TableProtoOrBuilder p = viaProto ? proto : builder;
		this.options = new Options(p.getOptions());
	}
	
	// TODO - to be completed
	public boolean equals(Object object) {
		if(object instanceof TableInfo) {
			TableInfo other = (TableInfo) object;
			
			return this.getTableType() == other.getTableType() &&
					this.getStoreType() == ((TableInfo)other).getStoreType();
		} else {
			return false;
		}
	}
	
	////////////////////////////////////////////////////////////////////////
	// ProtoObject
	////////////////////////////////////////////////////////////////////////
	@Override
	public TableProto getProto() {
		mergeLocalToProto();
		
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}
	
	protected void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = TableProto.newBuilder(proto);
		}
		viaProto = false;
	}
	
	protected void mergeLocalToBuilder() {
		if (this.storeType  != null) {			
			builder.setStoreType(this.storeType);
		}
		if (this.tableType != null) {
			builder.setTableType(this.tableType);			
		}
		if (this.schema != null) {
			builder.setSchema(this.schema.getProto());
		}
		if (this.startKey != null) {
			builder.setStartKey(this.startKey);
		}
		if (this.endKey != null) {
			builder.setEndKey(this.endKey);
		}
		if (this.duration != null) {
			builder.setUnit(this.duration);
		}
		if (this.options != null) {
			builder.setOptions(this.options.getProto());
		}		
	}
	
	protected void mergeLocalToProto() {
		if(viaProto) {
			maybeInitBuilder();
		}
		mergeLocalToBuilder();
		proto = builder.build();
		viaProto = true;
	}

	@Override
	public void write(DataOutput out) throws IOException {		
		if(getName() != null) {
			out.writeBoolean(true);
			Text.writeString(out, name);
		} else {
			out.writeBoolean(false);
		}
		
		byte [] bytes = getProto().toByteArray();
		out.writeInt(bytes.length);
		out.write(bytes);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if(in.readBoolean()) {
			this.name = Text.readString(in);
		}
		
		int len = in.readInt();
		byte [] bytes = new byte[len];
		in.readFully(bytes);
		builder.mergeFrom(bytes);
		proto = builder.build();
		viaProto = true;
	}

	@Override
	public int compareTo(TableInfo o) {
		if(getStartKey() < o.getStartKey()) {
			return -1;
		} else if (getStartKey() > o.getStartKey()) {
			return 1;
		} else {
			return 0;	
		}
	}
	
	public static class TableComparator implements Comparator<TableInfo> {

		@Override
		public int compare(TableInfo o1, TableInfo o2) {
			if(o1.getStartKey() < o2.getStartKey()) {
				return -1;
			} else if (o1.getStartKey() > o2.getStartKey()) {
				return 1;
			} else {
				return 0;	
			}
		}		
	}
}
