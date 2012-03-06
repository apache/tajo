/**
 * 
 */
package nta.engine;

import java.text.NumberFormat;

import nta.common.ProtoObject;
import nta.engine.TCommonProtos.LogicalQueryUnitIdProto;
import nta.engine.TCommonProtos.LogicalQueryUnitIdProtoOrBuilder;

/**
 * @author jihoon
 *
 */
public class LogicalQueryUnitId implements Comparable<LogicalQueryUnitId>,
  ProtoObject<LogicalQueryUnitIdProto> {
  
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(3);
  }

  public static final String SEPERATOR = "_";
  private SubQueryId subQueryId = null;
  private int id = -1;
  private String finalId = null;
  
  private LogicalQueryUnitIdProto proto = 
      LogicalQueryUnitIdProto.getDefaultInstance();
  private LogicalQueryUnitIdProto.Builder builder = null;
  private boolean viaProto = false;
  
  public LogicalQueryUnitId() {
    builder = LogicalQueryUnitIdProto.newBuilder();
  }
  
  public LogicalQueryUnitId(SubQueryId subQueryId, final int id) {
    this();
    this.subQueryId = subQueryId;
    this.id = id;
  }
  
  public LogicalQueryUnitId(LogicalQueryUnitIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public LogicalQueryUnitId(String finalId) {
    this.finalId = finalId;
    int i = finalId.lastIndexOf(QueryId.SEPERATOR);
    this.subQueryId = new SubQueryId(finalId.substring(0, i));
    this.id = Integer.valueOf(finalId.substring(i+1));
  }

  @Override
  public int compareTo(LogicalQueryUnitId o) {
    return this.toString().compareTo(o.toString());
  }

  @Override
  public final String toString() {
    if (finalId == null) {
      finalId = subQueryId +  SEPERATOR + idFormat.format(id);
    }
    return finalId;
  }
  
  @Override
  public final boolean equals(final Object o) {
    if (o instanceof LogicalQueryUnitId) {
      LogicalQueryUnitId oid = (LogicalQueryUnitId) o;
      return this.subQueryId.equals(oid.subQueryId) &&
          this.id == oid.id;
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }
  
  public SubQueryId getSubQueryId() {
    return this.subQueryId;
  }
  
  private void mergeProtoToLocal() {
    LogicalQueryUnitIdProtoOrBuilder p = viaProto ? proto : builder;
    if (subQueryId == null) {
      subQueryId = new SubQueryId(p.getSubQueryId());
    }
    if (id == -1) {
      id = p.getId();
    }
  }

  @Override
  public void initFromProto() {
    mergeProtoToLocal();
    subQueryId.initFromProto();
  }
  
  private void mergeLocalToBuilder() {
    if (this.builder == null) {
      builder = LogicalQueryUnitIdProto.newBuilder(proto);
    }
    if (this.subQueryId != null) {
      builder.setSubQueryId(subQueryId.getProto());
    }
    if (this.id != -1) {
      builder.setId(id);
    }
  }

  @Override
  public LogicalQueryUnitIdProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    return proto;
  }
 
  public int getId() {
    return this.id;
  }
}
