package nta.engine.ipc.protocolrecords;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.TabletProto;
import nta.catalog.proto.CatalogProtos.TabletProtoOrBuilder;
import nta.common.ProtoObject;

import org.apache.hadoop.fs.Path;

/**
 * @author jihoon
 * @author Hyunsik Choi
 */
public class Fragment implements TableDesc, Comparable<Fragment> {

  protected TabletProto proto = TabletProto.getDefaultInstance();
  protected TabletProto.Builder builder = null;
  protected boolean viaProto = false;

  private String tabletId;
  private Path path;
  private TableMeta meta;
  private long startOffset;
  private long length;

  public Fragment() {
    builder = TabletProto.newBuilder();
    startOffset = length = -1;
  }

  public Fragment(String tabletId, Path path, TableMeta meta, long start,
      long length) {
    this();
    TableMeta newMeta = new TableMetaImpl(meta.getProto());
    Schema newSchema = new Schema();
    Column newColumn = null;
    for(Column col : meta.getSchema().getColumns()) {
      if(col.isQualifiedName() == false) {
        newColumn = new Column(col.getProto());
        newColumn.setName(tabletId+"."+col.getName());
        newSchema.addColumn(newColumn);
      }
    }
    newMeta.setSchema(newSchema);
    this.set(tabletId, path, newMeta, start, length);
  }

  public Fragment(TabletProto proto) {
    startOffset = length = -1;
    this.proto = proto;
    this.viaProto = true;
  }

  public void set(String tabletId, Path path, TableMeta meta, long start,
      long length) {
    maybeInitBuilder();
    this.tabletId = tabletId;
    this.path = path;
    this.meta = meta;
    this.startOffset = start;
    this.length = length;
  }

  public String getId() {
    TabletProtoOrBuilder p = viaProto ? proto : builder;

    if (this.tabletId != null) {
      return this.tabletId;
    }

    if (!proto.hasId()) {
      return null;
    }
    this.tabletId = p.getId();

    return this.tabletId;
  }

  @Override
  public void setId(String tabletId) {
    maybeInitBuilder();
    this.tabletId = tabletId;
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
    return "\"tablet\": {\"id\": \""+tabletId+"\", \"path\": "
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
