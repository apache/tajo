package nta.engine.ipc.protocolrecords;

import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.TabletProto;
import nta.catalog.proto.TableProtos.TabletProtoOrBuilder;

import org.apache.hadoop.fs.Path;

/**
 * @author jihoon
 * @author Hyunsik Choi
 */
public class Tablet implements Comparable<Tablet> {
  
  protected TabletProto proto = TabletProto.getDefaultInstance();
  protected TabletProto.Builder builder = null;
  protected boolean viaProto = false;
  
  private String tabletId;
  private Path path;
  private TableMeta meta;
  private long startOffset;
  private long length;
  
  public Tablet() {
    builder = TabletProto.newBuilder();
    startOffset = length = -1;
  }
  
  public Tablet(String tabletId, Path path, TableMeta meta, long start, long length) {
    this();
    this.set(tabletId, path, meta, start, length);
  }
  
  public Tablet(TabletProto proto) {
    startOffset = length = -1;
    this.proto = proto;
    this.viaProto = true;
  }
  
  public void set(String tabletId, Path path, TableMeta meta, long start, long length) {
    maybeInitBuilder();
    this.tabletId = tabletId;
    this.path = path;
    this.meta = meta;
    this.startOffset = start;
    this.length = length;
  }
  
  public String getId() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;
    
    if(this.tabletId != null) {
      return this.tabletId;
    }
    
    if(!proto.hasId()) {
      return null;
    }    
    this.tabletId = p.getId();
    
    return this.tabletId;
  }
  
  public Path getPath() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;
    
    if (this.path != null) {
      return this.path; 
    }
    if (!proto.hasPath()) {
      return null;
    }
    this.path = new Path(p.getPath());
    return this.path;
  }
  
  public TableMeta getMeta() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;
    
    if (this.meta != null) {
      return this.meta;
    }
    if (!proto.hasMeta()) {
      return null;
    }
    this.meta = new TableMetaImpl(p.getMeta());
    return this.meta;    
  }
  
  public long getStartOffset() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;
    
    if (this.startOffset > -1) {
      return this.startOffset;
    }
    if (!proto.hasStartOffset()) {
      return -1;
    }    
    this.startOffset = p.getStartOffset();
    return this.startOffset;
  }
  
  public long getLength() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;
    
    if (this.length > -1) {
      return this.length;
    } 
    if (!proto.hasLength()) {
      return -1;
    }
    this.length = p.getLength();
    return this.length;
  }
  
  /**
   * 
   * The offset range of tablets <b>MUST NOT</b> be overlapped.
   * @param t
   * @return If the table paths are not same, return -1. 
   */
  @Override
  public int compareTo(Tablet t) {
    if (getPath().equals(t.getPath())) {
      return (int)(this.getStartOffset() - t.getStartOffset());
    } else {
      return -1;
    }
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Tablet) {
      Tablet t = (Tablet)o;
      if (getPath().equals(t.getPath()) 
          && t.getStartOffset() == this.getStartOffset()
          && t.getLength() == this.getLength()) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return (int) (getPath().hashCode() << 16 | getStartOffset() >> 16);
  }
  
  @Override
  public String toString() {
    return getPath() + "(start=" + this.getStartOffset() + ",length=" + getLength() + ")";
  }
  
  public TabletProto getProto() {
      mergeLocalToProto();
      
      proto = viaProto ? proto : builder.build();
      viaProto = true;
      return proto;
    }
    
    private void maybeInitBuilder() {
      if (viaProto || builder == null) {
        builder = TabletProto.newBuilder(proto);
      }
      viaProto = false;
    }
    
    protected void mergeLocalToBuilder() {
      if (this.tabletId != null) {
        builder.setId(this.tabletId);
      }
      
      if (this.startOffset > -1) {
        builder.setStartOffset(this.startOffset);
      }
      
      if (this.meta != null) {
        builder.setMeta(meta.getProto());
      }
      
      if (this.length > -1) {
        builder.setLength(this.length);
      }
      
      if (this.path != null) {
        builder.setPath(this.path.toString());
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
