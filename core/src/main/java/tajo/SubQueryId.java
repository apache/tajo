package tajo;

import tajo.common.ProtoObject;
import tajo.engine.TCommonProtos.SubQueryIdProto;
import tajo.engine.TCommonProtos.SubQueryIdProtoOrBuilder;
import tajo.impl.pb.QueryIdPBImpl;
import tajo.util.TajoIdUtils;

import java.text.NumberFormat;

/**
 * @author Hyunsik Choi
 */
public class SubQueryId implements Comparable<SubQueryId>, 
  ProtoObject<SubQueryIdProto> {
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(3);
  }

  private QueryId queryId = null;
  private int id = -1;
  private String finalId = null;
  
  private SubQueryIdProto proto = SubQueryIdProto.getDefaultInstance();
  private SubQueryIdProto.Builder builder = null;
  private boolean viaProto = false;
  
  public SubQueryId() {
    builder = SubQueryIdProto.newBuilder();
  }
  
  public SubQueryId(final QueryId queryId, final int id) {
    this();
    this.queryId = queryId;
    this.id = id;
  }
  
  public SubQueryId(String finalId) {
    this();
    int i = finalId.lastIndexOf(QueryId.SEPARATOR);
    
    this.queryId = TajoIdUtils.createQueryId(finalId.substring(0, i));
    this.id = Integer.valueOf(finalId.substring(i+1));
  }
  
  public SubQueryId(SubQueryIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public int getId() {
    SubQueryIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id != -1) {
      return this.id;
    }
    if (!p.hasId()) {
      return -1;
    }
    this.id = p.getId();
    return this.id;
  }
  
  public final String toString() {
    if (finalId == null) {
      finalId = getQueryId().toString() + QueryId.SEPARATOR
          + idFormat.format(getId());
    }
    return finalId;
  }
  
  public QueryId getQueryId() {
    SubQueryIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.queryId != null) {
      return this.queryId;
    }
    if (!p.hasQueryId()) {
      return null;
    }
    this.queryId = new QueryIdPBImpl(p.getQueryId());
    return this.queryId;
  }
  
  @Override
  public final boolean equals(final Object o) {
    if (o instanceof SubQueryId) {
      SubQueryId other = (SubQueryId) o;
      return this.getQueryId().equals(other.getQueryId()) &&
          this.getId()== other.getId();
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  @Override
  public final int compareTo(final SubQueryId o) {
    return this.toString().compareTo(o.toString());
  }
  
  private void mergeProtoToLocal() {
    SubQueryIdProtoOrBuilder p = viaProto ? proto : builder;
    if (queryId == null) {
      queryId = new QueryIdPBImpl(p.getQueryId());
    }
    if (id == -1) {
      id = p.getId();
    }
  }

  @Override
  public void initFromProto() {
    mergeProtoToLocal();
  }
  
  private void mergeLocalToBuilder() {
    if (this.builder == null) {
      this.builder = SubQueryIdProto.newBuilder(proto);
    }
    if (this.queryId != null) {
      builder.setQueryId(((QueryIdPBImpl)queryId).getProto());
    }
    if (this.id != -1) {
      builder.setId(id);
    }
  }

  @Override
  public SubQueryIdProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    return proto;
  }
}
