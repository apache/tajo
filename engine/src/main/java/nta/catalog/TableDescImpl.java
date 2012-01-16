package nta.catalog;

import java.net.URI;

import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableDescProto;
import nta.catalog.proto.TableProtos.TableDescProtoOrBuilder;
import nta.catalog.proto.TableProtos.TableProto;

import org.apache.hadoop.fs.Path;

/**
 * @author Hyunsik Choi
 *
 */
public class TableDescImpl implements TableDesc {
  protected TableDescProto proto = TableDescProto.getDefaultInstance();
  protected TableDescProto.Builder builder = null;
  protected boolean viaProto = false;
  
  protected int id;
  protected String name;
  protected URI uri;
  protected TableMeta info;
  
	public TableDescImpl() {
		builder = TableDescProto.newBuilder();
	}
	
	public TableDescImpl(String name) {
		this();
		setName(name);
	}
	
	public TableDescImpl(String name, TableMeta info) {
		this();
	  setName(name);
	  setMeta(info);		
	}
	
	public TableDescImpl(String name, Schema schema, StoreType type) {
	  this(name, new TableMetaImpl(schema, type));
	}
	
	public TableDescImpl(TableDescProto proto) {
	  this.proto = proto;
	  viaProto = true;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public int getId() {
	  TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (id > -1) {
      return this.id;
    }
    if (!proto.hasId()) {
      return id;
    }
    this.id = p.getId();
    
    return this.id;
  }
	
	public void setName(String name) {
		this.name = name;
	}
	
  public String getName() {
    TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (name != null) {
      return this.name;
    }
    if (!proto.hasName()) {
      return null;
    }
    this.name = p.getName();
    
    return this.name;
  }
	
	public void setURI(URI uri) {
		this.uri = uri;
	}
	
	public void setURI(Path path) {
	  this.uri = path.toUri();
	}
	
  public URI getURI() {
    TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (uri != null) {
      return this.uri;
    }
    if (!proto.hasPath()) {
      return null;
    }
    this.uri = URI.create(p.getPath());
    
    return this.uri;
  }
  
  @Override
  public void setMeta(TableMeta info) {
    maybeInitBuilder();
    this.info = info;
  }
	
	public TableMeta getMeta() {
	  TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (info != null) {
      return this.info;
    }
    if (!proto.hasDesc()) {
      return null;
    }
    this.info = new TableMetaImpl(p.getDesc());
	  return this.info;
	}
	
	public boolean equals(Object object) {
    if(object instanceof TableDescImpl) {
      TableDescImpl other = (TableDescImpl) object;
      
      return this.getProto().equals(other.getProto());
    }
    
    return false;   
  }
	
	public Object clone() {
	  return new TableDescImpl(this.getProto());
	}
	
	public String toString() {
	  StringBuilder str = new StringBuilder();
	  str.append("{")
	  .append("tableId: "+this.id).append("\n")
	  .append("name: "+this.name).append("\n")
	  .append("uri: "+this.uri).append("\n")
	  .append("proto: "+this.proto.toString()).append("\n}");
	  
	  return str.toString();
	}

  public TableDescProto getProto() {
    mergeLocalToProto();
    
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = TableDescProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  protected void mergeLocalToBuilder() {    
    if (this.id > 0) {
      builder.setId(this.id);
    }
    
    if (this.name != null) {
      builder.setName(this.name);
    }
    
    if (this.uri != null) {
      builder.setPath(this.uri.toString());
    }
    
    if (this.info != null) {
      builder.setDesc((TableProto) info.getProto());
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
