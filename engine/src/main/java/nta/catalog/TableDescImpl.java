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
  
  protected String tableId;
  protected Path uri;
  protected TableMeta meta;
  
	public TableDescImpl() {
		builder = TableDescProto.newBuilder();
	}
	
	public TableDescImpl(String tableId) {
		this();
		setId(tableId);
	}
	
	public TableDescImpl(String tableId, TableMeta info) {
		this();
	  setId(tableId);
	  setMeta(info);		
	}
	
	public TableDescImpl(String tableId, Schema schema, StoreType type) {
	  this(tableId, new TableMetaImpl(schema, type));
	}
	
	public TableDescImpl(TableDescProto proto) {
	  this.proto = proto;
	  viaProto = true;
	}
	
	public void setId(String tableId) {
	  maybeInitBuilder();
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
    maybeInitBuilder();
    this.meta = info;
  }
	
	public TableMeta getMeta() {
	  TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (meta != null) {
      return this.meta;
    }
    if (!proto.hasMeta()) {
      return null;
    }
    this.meta = new TableMetaImpl(p.getMeta());
	  return this.meta;
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
	  .append("id: "+this.tableId).append("\n")
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
  
  private void mergeLocalToProto() {
    if(viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
}
