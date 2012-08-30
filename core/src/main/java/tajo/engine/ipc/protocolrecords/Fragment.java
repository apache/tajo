package tajo.engine.ipc.protocolrecords;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.SchemaProto;
import tajo.catalog.proto.CatalogProtos.TabletProto;
import tajo.catalog.proto.CatalogProtos.TabletProtoOrBuilder;
import tajo.SchemaObject;
import tajo.engine.json.GsonCreator;
import tajo.engine.utils.TUtil;

/**
 * @author jihoon
 * @author Hyunsik Choi
 */
public class Fragment implements TableDesc, Comparable<Fragment>, SchemaObject {

  protected TabletProto proto = TabletProto.getDefaultInstance();
  protected TabletProto.Builder builder = null;
  protected boolean viaProto = false;

  @Expose
  private String fragmentId;
  @Expose
  private Path path;
  @Expose
  private TableMeta meta;
  @Expose
  private Long startOffset;
  @Expose
  private Long length;

  @Expose
  private Boolean distCached;

  public Fragment() {
    builder = TabletProto.newBuilder();
  }

  public Fragment(String fragmentId, Path path, TableMeta meta, long start,
      long length) {
    this();
    TableMeta newMeta = new TableMetaImpl(meta.getProto());
    SchemaProto newSchemaProto = TCatUtil.getQualfiedSchema(fragmentId, meta
        .getSchema().getProto());
    newMeta.setSchema(new Schema(newSchemaProto));
    this.set(fragmentId, path, newMeta, start, length);
  }

  public Fragment(TabletProto proto) {
    this();
    TableMeta newMeta = new TableMetaImpl(proto.getMeta());
    this.set(proto.getId(), new Path(proto.getPath()), newMeta,
        proto.getStartOffset(), proto.getLength());
    if (proto.hasDistCached() && proto.getDistCached()) {
      distCached = true;
    }
  }

  private void set(String fragmentId, Path path, TableMeta meta, long start,
      long length) {
    this.fragmentId = fragmentId;
    this.path = path;
    this.meta = meta;
    this.startOffset = start;
    this.length = length;
  }

  public String getId() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;

    if (this.fragmentId != null) {
      return this.fragmentId;
    }

    if (!p.hasId()) {
      return null;
    }
    this.fragmentId = p.getId();

    return this.fragmentId;
  }

  @Override
  public void setId(String fragmentId) {
    setModified();
    this.fragmentId = fragmentId;
  }
  
  @Override
  public Path getPath() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;

    if (this.path != null) {
      return this.path;
    }
    if (!p.hasPath()) {
      return null;
    }
    this.path = new Path(p.getPath());
    return this.path;
  }

  @Override
  public void setPath(Path path) {
    setModified();
    this.path = path;
  }
  
  public Schema getSchema() {
    return getMeta().getSchema();
  }

  public TableMeta getMeta() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;

    if (this.meta != null) {
      return this.meta;
    }
    if (!p.hasMeta()) {
      return null;
    }
    this.meta = new TableMetaImpl(p.getMeta());
    return this.meta;
  }

  @Override
  public void setMeta(TableMeta meta) {
    setModified();
    this.meta = meta;
  }

  public Long getStartOffset() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;

    if (this.startOffset != null) {
      return this.startOffset;
    }
    if (!p.hasStartOffset()) {
      return null;
    }
    this.startOffset = p.getStartOffset();
    return this.startOffset;
  }

  public Long getLength() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;

    if (this.length != null) {
      return this.length;
    }
    if (!p.hasLength()) {
      return null;
    }
    this.length = p.getLength();
    return this.length;
  }

  public Boolean isDistCached() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;

    if (this.distCached != null) {
      return distCached;
    }
    if (!p.hasDistCached()) {
      return false;
    }
    this.distCached = p.getDistCached();
    return this.distCached;
  }

  public void setDistCached() {
    setModified();
    this.distCached = true;
  }

  /**
   * 
   * The offset range of tablets <b>MUST NOT</b> be overlapped.
   * 
   * @param t
   * @return If the table paths are not same, return -1.
   */
  @Override
  public int compareTo(Fragment t) {
    if (getPath().equals(t.getPath())) {
      long diff = this.getStartOffset() - t.getStartOffset();
      if (diff < 0) {
        return -1;
      } else if (diff > 0) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Fragment) {
      Fragment t = (Fragment) o;
      if (getPath().equals(t.getPath())
          && TUtil.checkEquals(t.getStartOffset(), this.getStartOffset())
          && TUtil.checkEquals(t.getLength(), this.getLength())
          && TUtil.checkEquals(t.isDistCached(), this.isDistCached())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getPath(), getStartOffset(), getLength(),
        isDistCached());
  }
  
  public Object clone() throws CloneNotSupportedException {
    Fragment frag = (Fragment) super.clone();
    initFromProto();
    frag.proto = null;
    frag.viaProto = false;
    frag.builder = TabletProto.newBuilder();
    frag.fragmentId = fragmentId;
    frag.path = path;
    frag.meta = (TableMeta) (meta != null ? meta.clone() : null);
    frag.distCached = distCached;
    
    return frag;
  }

  @Override
  public String toString() {
    return "\"fragment\": {\"id\": \""+fragmentId+"\", \"path\": "
    		+getPath() + "\", \"start\": " + this.getStartOffset() + ",\"length\": "
        + getLength() + ", \"distCached\": " + distCached + "}" ;
  }

  @Override
  public TabletProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    
    return proto;
  }

  private void setModified() {
    if (viaProto || builder == null) {
      builder = TabletProto.newBuilder(proto);
    }
    viaProto = false;
  }

  protected void mergeLocalToBuilder() {
    if (builder == null) {
      this.builder = TabletProto.newBuilder(proto);
    }
    
    if (this.fragmentId != null) {
      builder.setId(this.fragmentId);
    }

    if (this.startOffset != null) {
      builder.setStartOffset(this.startOffset);
    }

    if (this.meta != null) {
      builder.setMeta(meta.getProto());
    }

    if (this.length!= null) {
      builder.setLength(this.length);
    }

    if (this.path != null) {
      builder.setPath(this.path.toString());
    }

    if (this.distCached != null) {
      builder.setDistCached(this.distCached);
    }
  }
  
  private void mergeProtoToLocal() {
	  TabletProtoOrBuilder p = viaProto ? proto : builder;
	  if (fragmentId == null && p.hasId()) {
	    fragmentId = p.getId();
	  }
	  if (path == null && p.hasPath()) {
		  path = new Path(p.getPath());
	  }
	  if (meta == null && p.hasMeta()) {
		  meta = new TableMetaImpl(p.getMeta());
	  }
	  if (startOffset == null && p.hasStartOffset()) {
		  startOffset = p.getStartOffset();
	  }
	  if (length == null && p.hasLength()) {
		  length = p.getLength();
	  }
    if (distCached == null && p.hasDistCached()) {
      distCached = p.getDistCached();
    }
  }

  @Override
  public String toJSON() {
	  initFromProto();
	  Gson gson = GsonCreator.getInstance();
	  return gson.toJson(this, TableDesc.class);
  }

  @Override
  public void initFromProto() {
	  mergeProtoToLocal();
    meta.initFromProto();
  }
}
