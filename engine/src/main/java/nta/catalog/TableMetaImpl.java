/**
 * 
 */
package nta.catalog;

import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableProto;
import nta.catalog.proto.TableProtos.TableProtoOrBuilder;

/**
 * @author Hyunsik Choi
 *
 */
public class TableMetaImpl implements TableMeta {
	protected TableProto proto = TableProto.getDefaultInstance();
	protected TableProto.Builder builder = null;
	protected boolean viaProto = false;	
	
	// materialized variables
	protected Schema schema;
	protected StoreType storeType;
	protected Options options;
	
	public TableMetaImpl() {
		builder = TableProto.newBuilder();
	}
	
	public TableMetaImpl(Schema schema, StoreType type) {
	  this();
	  setSchema(schema);
	  setStorageType(type);
	}
	
	public TableMetaImpl(TableProto proto) {
		this.proto = proto;
		this.viaProto = true;
	}
	
	public void setStorageType(StoreType storeType) {
    maybeInitBuilder();
    this.storeType = storeType;
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
	
  public void setSchema(Schema schema) {
    maybeInitBuilder();
    this.schema = schema;
  }
	
	public Schema getSchema() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		
		if(schema != null) {
			return this.schema;
		}
		if(!proto.hasSchema()) {
		  return null;
		}
		this.schema = new Schema(p.getSchema());
		
		return this.schema;
	}
  public void setOptions(Options options) {
    initOptions();
    this.options = options;
  }
  
  public Options getOptions() {
    TableProtoOrBuilder p = viaProto ? proto : builder;
    if(this.options != null) {
      return this.options;
    }
    if(!p.hasParams()) {
      return null;
    }
    this.options = new Options(p.getParams());
    
    return this.options;
  }

  public void putOption(String key, String val) {
    initOptions();
    this.options.put(key, val);
  }
    
  public String deleteOption(String key) {
    initOptions();
    return this.options.delete(key);
  }
	
	public String getOption(String key) {
		initOptions();
		return this.options.get(key);		
	}
	
	private void initOptions() {
		if(this.options != null) {
			return;
		}
		TableProtoOrBuilder p = viaProto ? proto : builder;
		this.options = new Options(p.getParams());
	}	
	
	public boolean equals(Object object) {
		if(object instanceof TableMetaImpl) {
			TableMetaImpl other = (TableMetaImpl) object;			
			
			return this.getProto().equals(other.getProto());	     
		}
		
		return false;		
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
	
	private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = TableProto.newBuilder(proto);
		}
		viaProto = false;
	}
	
	private void mergeLocalToBuilder() {
	  if (this.schema != null) {
	    builder.setSchema(this.schema.getProto());
	  }

	  if (this.storeType != null) {
      builder.setStoreType(storeType);
    }

		if (this.options != null) {
			builder.setParams(options.getProto());
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
	
	public Object clone() {
	  return new TableMetaImpl(this.getProto());
	}
	
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  if(viaProto) {
	    return proto.toString();
	  }
	  
	  sb.append("{");
	  if(schema != null) {
	    sb.append("schema {")
	    .append(schema.toString())
	    .append("}");
	  }
	  
	  if(storeType != null) {
	    sb.append("storeType: {")
	    .append(storeType)
	    .append("}");
	  }
	  
	  if(options != null) {
	    sb.append("options: {")
	    .append(options)
	    .append("}");
	  }
	  
	  return sb.toString();
	}
}
