package nta.engine.ipc.protocolrecords;

import nta.catalog.CatalogUtil;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.SchemaProto;
import nta.catalog.proto.CatalogProtos.TabletProto;
import nta.catalog.proto.CatalogProtos.TabletProtoOrBuilder;

import org.apache.hadoop.fs.Path;

/**
 * @author jihoon
 * @author Hyunsik Choi
 */
public class Fragment implements TableDesc, Comparable<Fragment> {

  protected TabletProto proto = TabletProto.getDefaultInstance();
  protected TabletProto.Builder builder = null;
  protected boolean viaProto = false;

  private String fragmentId;
  private Path path;
  private TableMeta meta;
  private long startOffset;
  private long length;

  public Fragment() {
    builder = TabletProto.newBuilder();
    startOffset = length = -1;
  }

  public Fragment(String fragmentId, Path path, TableMeta meta, long start,
      long length) {
    this();
    TableMeta newMeta = new TableMetaImpl(meta.getProto());
    SchemaProto newSchemaProto = CatalogUtil.getQualfiedSchema(fragmentId, meta
        .getSchema().getProto());
    newMeta.setSchema(new Schema(newSchemaProto));
    this.set(fragmentId, path, newMeta, start, length);
  }

  public Fragment(TabletProto proto) {
    this(proto.getId(), new Path(proto.getPath()), new TableMetaImpl(
        proto.getMeta()), proto.getStartOffset(), proto.getLength());
  }

  public void set(String fragmentId, Path path, TableMeta meta, long start,
      long length) {
    maybeInitBuilder();
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
    maybeInitBuilder();
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
    maybeInitBuilder();
    this.path = path;
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
    maybeInitBuilder();
    this.meta = meta;
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
   * 
   * @param t
   * @return If the table paths are not same, return -1.
   */
  @Override
  public int compareTo(Fragment t) {
    if (getPath().equals(t.getPath())) {
      return (int) (this.getStartOffset() - t.getStartOffset());
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
    return "\"fragment\": {\"id\": \""+fragmentId+"\", \"path\": "
    		+getPath() + "\", \"start\": " + this.getStartOffset() + ",\"length\": "
        + getLength() + "}";
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
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
  
  public Object clone() {
    return new Fragment(this.proto);
  }
}
