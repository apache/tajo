package nta.engine.ipc.protocolrecords;

import nta.annotation.Optional;
import nta.annotation.Required;
import nta.catalog.TCatUtil;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.SchemaProto;
import nta.catalog.proto.CatalogProtos.TabletProto;
import nta.catalog.proto.CatalogProtos.TabletProtoOrBuilder;
import nta.engine.SchemaObject;
import nta.engine.json.GsonCreator;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

/**
 * @author jihoon
 * @author Hyunsik Choi
 */
public class Fragment implements TableDesc, Comparable<Fragment>, SchemaObject {

  protected TabletProto proto = TabletProto.getDefaultInstance();
  protected TabletProto.Builder builder = null;
  protected boolean viaProto = false;

  @Expose @Required
  private String fragmentId;
  @Expose @Required
  private Path path;
  @Expose @Required
  private TableMeta meta;
  @Expose @Required
  private long startOffset;
  @Expose @Required
  private long length;
  @Expose @Optional
  private Boolean distCached;

  public Fragment() {
    builder = TabletProto.newBuilder();
    startOffset = length = -1;
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
    this(proto.getId(), new Path(proto.getPath()), new TableMetaImpl(
        proto.getMeta()), proto.getStartOffset(), proto.getLength());
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

    if (!proto.hasId()) {
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
    if (!proto.hasPath()) {
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
    if (!proto.hasMeta()) {
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

  public long getStartOffset() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;

    if (this.startOffset > -1) {
      return this.startOffset;
    }
    if (!p.hasStartOffset()) {
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
    if (!p.hasLength()) {
      return -1;
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
      return null;
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
          && t.getStartOffset() == this.getStartOffset()
          && t.getLength() == this.getLength()
          && t.isDistCached() == this.isDistCached()) {
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
	  if (startOffset == -1 && p.hasStartOffset()) {
		  startOffset = p.getStartOffset();
	  }
	  if (length == -1 && p.hasLength()) {
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
