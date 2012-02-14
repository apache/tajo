/**
 * 
 */
package nta.catalog;

import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.proto.CatalogProtos.TableProto;
import nta.catalog.proto.CatalogProtos.TableProtoOrBuilder;
import nta.engine.json.GsonCreator;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 *
 */
public class TableMetaImpl implements TableMeta {
	protected TableProto proto = TableProto.getDefaultInstance();
	protected TableProto.Builder builder = null;
	protected boolean viaProto = false;	
	
	// materialized variables
	@Expose
	protected Schema schema;
	@Expose
	protected StoreType storeType;
	@Expose
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
      this.options = new Options();
      return this.options;
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
	
	@Override
	public Object clone() throws CloneNotSupportedException {    
	  TableMetaImpl meta = (TableMetaImpl) super.clone();
	  initFromProto();
	  meta.proto = null;
    meta.viaProto = false;
    meta.builder = TableProto.newBuilder();
    meta.schema = (Schema) schema.clone();
    meta.storeType = storeType;
    meta.options = (Options) (options != null ? options.clone() : null);
    
    return meta;
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
	
	private void mergeProtoToLocal() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		if (schema == null && p.hasSchema()) {
			schema = new Schema(p.getSchema());
		}
		if (storeType == null && p.hasStoreType()) {
			storeType = p.getStoreType();
		}
		if (options == null && p.hasParams()) {
			options = new Options(p.getParams());
		} else {
		  options = new Options();
		}
	}
	
	public void initFromProto() {
		mergeProtoToLocal();
    schema.initFromProto();
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
	
	public String toJSON() {
		initFromProto();
		Gson gson = GsonCreator.getInstance();
		return gson.toJson(this, TableMeta.class);
	}
}
