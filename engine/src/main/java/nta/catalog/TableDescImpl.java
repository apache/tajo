package nta.catalog;

import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.catalog.proto.CatalogProtos.TableDescProtoOrBuilder;
import nta.catalog.proto.CatalogProtos.TableProto;
import nta.common.ProtoObject;
import nta.engine.json.GsonCreator;

import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 *
 */
public class TableDescImpl implements TableDesc, ProtoObject<TableDescProto>,
    Cloneable {
  protected TableDescProto proto = TableDescProto.getDefaultInstance();
  protected TableDescProto.Builder builder = null;
  protected boolean viaProto = false;
  
	@Expose
  protected String tableId;
	@Expose
  protected Path uri;
	@Expose
  protected TableMeta meta;
  
	public TableDescImpl() {
		builder = TableDescProto.newBuilder();
	}
	
	public TableDescImpl(String tableId, TableMeta info, Path path) {
		this();
	  this.tableId = tableId;
	  this.meta = info;
	  this.uri = path;	   
	}
	
	public TableDescImpl(String tableId, Schema schema, StoreType type, 
	    Options options, Path path) {
	  this(tableId, new TableMetaImpl(schema, type, options), path);
	}
	
	public TableDescImpl(TableDescProto proto) {
	  this.proto = proto;
	  viaProto = true;
	}
	
	public void setId(String tableId) {
	  setModified();
		this.tableId = tableId;
	}
	
  public String getId() {
    TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (tableId != null) {
      return this.tableId;
    }
    if (!proto.hasId()) {
      return null;
    }
    this.tableId = p.getId();
    
    return this.tableId;
  }
	
	public void setPath(Path uri) {
	  setModified();
		this.uri = uri;
	}
	
  public Path getPath() {
    TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (uri != null) {
      return this.uri;
    }
    if (!proto.hasPath()) {
      return null;
    }
    this.uri = new Path(p.getPath());
    
    return this.uri;
  }
  
  @Override
  public void setMeta(TableMeta info) {
    setModified();
    this.meta = info;
  }
	
	public TableMeta getMeta() {
	  TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (meta != null) {
      return this.meta;
    }
    if (!p.hasMeta()) {
      return null;
    }
    this.meta = new TableMetaImpl(p.getMeta());
	  return this.meta;
	}
	
  public Schema getSchema() {
    return getMeta().getSchema();
  }
	
	public boolean equals(Object object) {
    if(object instanceof TableDescImpl) {
      TableDescImpl other = (TableDescImpl) object;
      
      return this.getProto().equals(other.getProto());
    }
    
    return false;   
  }
	
	public Object clone() throws CloneNotSupportedException {	  
	  TableDescImpl desc = (TableDescImpl) super.clone();
	  initFromProto();
	  desc.proto = null;
	  desc.builder = TableDescProto.newBuilder();
	  desc.viaProto = false;
	  desc.tableId = tableId;
	  desc.uri = uri;
	  desc.meta = (TableMeta) meta.clone();
	  
	  return desc;
	}
	
	public String toString() {
	  StringBuilder str = new StringBuilder();
	  str.append("\"table\": {")
	  .append("\"id\": \""+getId()).append("\",")
	  .append("\"path\": \""+getPath()).append("\",")
	  .append("\"meta\": \""+getMeta().toString()).append("\"}");
	  
	  return str.toString();
	}
	
	public String toJSON() {
		initFromProto();
		Gson gson = GsonCreator.getInstance();
		
		return gson.toJson(this, TableDesc.class);
	}

  public TableDescProto getProto() {
    if(!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    
    return proto;
  }
  
  private void setModified() {
    if (viaProto && builder == null) {
      builder = TableDescProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  protected void mergeLocalToBuilder() {
    if (builder == null) {
      builder = TableDescProto.newBuilder(proto);
    }
    
    if (this.tableId != null) {
      builder.setId(this.tableId);
    }
    
    if (this.uri != null) {
      builder.setPath(this.uri.toString());
    }
    
    if (this.meta != null) {
      builder.setMeta((TableProto) meta.getProto());
    }
  }
  
  private void mergeProtoToLocal() {
	  TableDescProtoOrBuilder p = viaProto ? proto : builder;
	  if (tableId == null && p.hasId()) {
		  tableId = p.getId();
	  }
	  if (uri == null && p.hasPath()) {
		  uri = new Path(p.getPath());
	  }
	  if (meta == null && p.hasMeta()) {
		  meta = new TableMetaImpl(p.getMeta());
	  }
  }

  @Override
  public void initFromProto() {
	  mergeProtoToLocal();
    meta.initFromProto();
  }
}
