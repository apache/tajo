/**
 * 
 */
package tajo;

import tajo.common.ProtoObject;
import tajo.engine.TCommonProtos.QueryIdProto;
import tajo.engine.TCommonProtos.QueryIdProtoOrBuilder;

import java.text.NumberFormat;

/**
 * @author Hyunsik Choi
 */
public class QueryId implements Comparable<QueryId>, ProtoObject<QueryIdProto> {
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(3);
  }

  public static final String PREFIX = "query";
  public static final String SEPERATOR = "_";
  private String timeId = null;
  private int id = -1;
  private String finalId = null;
  
  private QueryIdProto proto = QueryIdProto.getDefaultInstance();
  private QueryIdProto.Builder builder = null;
  private boolean viaProto = false;
  
  public QueryId() {
    builder = QueryIdProto.newBuilder();
  }
  
  public QueryId(final String timeId, final int id) {
    this();
    this.timeId = timeId;
    this.id = id;
  }
  
  public QueryId(final String finalId) {
    this();
    this.finalId = finalId;
    String[] split = finalId.split(QueryId.SEPERATOR);
    this.timeId = split[1];
    this.id = Integer.valueOf(split[2]);
  }
  
  public QueryId(QueryIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public int getId() {
    QueryIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id != -1) {
      return this.id;
    }
    if (!p.hasId()) {
      return -1;
    }
    this.id = p.getId();
    return this.id;
  }
  
  public String getTimeId() {
    QueryIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.timeId != null) {
      return this.timeId;
    }
    if (!p.hasTimeId()) {
      return null;
    }
    this.timeId = p.getTimeId();
    return this.timeId;
  }
  
  public final String toString() {
    if (finalId == null) {
      finalId = PREFIX + SEPERATOR + getTimeId() + 
          SEPERATOR + idFormat.format(getId());
    }
    return finalId;
  }
  
  @Override
  public final boolean equals(final Object o) {
    if (o instanceof QueryId) {
      QueryId other = (QueryId) o;
      return this.getTimeId().equals(other.getTimeId()) && 
          (this.getId()==other.getId());
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  @Override
  public final int compareTo(final QueryId o) {
    return this.toString().compareTo(o.toString());
  }
  
  private void mergeProtoToLocal() {
    QueryIdProtoOrBuilder p = viaProto ? proto : builder;
    if (timeId == null) {
      timeId = p.getTimeId();
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
      this.builder = QueryIdProto.newBuilder(proto);
    }
    if (this.timeId != null) {
      builder.setTimeId(timeId);
    }
    if (this.id != -1) {
      builder.setId(id);
    }
  }

  @Override
  public QueryIdProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    return proto;
  }
}