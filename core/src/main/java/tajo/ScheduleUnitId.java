/**
 * 
 */
package tajo;

import tajo.common.ProtoObject;
import tajo.engine.TCommonProtos.ScheduleUnitIdProto;
import tajo.engine.TCommonProtos.ScheduleUnitIdProtoOrBuilder;

import java.text.NumberFormat;

/**
 * @author jihoon
 *
 */
public class ScheduleUnitId implements Comparable<ScheduleUnitId>,
  ProtoObject<ScheduleUnitIdProto> {
  
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(3);
  }

  public static final String SEPERATOR = "_";
  private SubQueryId subQueryId = null;
  private int id = -1;
  private String finalId = null;
  
  private ScheduleUnitIdProto proto = 
      ScheduleUnitIdProto.getDefaultInstance();
  private ScheduleUnitIdProto.Builder builder = null;
  private boolean viaProto = false;
  
  public ScheduleUnitId() {
    builder = ScheduleUnitIdProto.newBuilder();
  }
  
  public ScheduleUnitId(SubQueryId subQueryId, final int id) {
    this();
    this.subQueryId = subQueryId;
    this.id = id;
  }
  
  public ScheduleUnitId(ScheduleUnitIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ScheduleUnitId(String finalId) {
    this.finalId = finalId;
    int i = finalId.lastIndexOf(QueryId.SEPERATOR);
    this.subQueryId = new SubQueryId(finalId.substring(0, i));
    this.id = Integer.valueOf(finalId.substring(i+1));
  }

  @Override
  public int compareTo(ScheduleUnitId o) {
    return this.toString().compareTo(o.toString());
  }

  @Override
  public final String toString() {
    if (finalId == null) {
      finalId = getSubQueryId() +  SEPERATOR + 
          idFormat.format(getId());
    }
    return finalId;
  }
  
  @Override
  public final boolean equals(final Object o) {
    if (o instanceof ScheduleUnitId) {
      ScheduleUnitId oid = (ScheduleUnitId) o;
      return this.getSubQueryId().equals(oid.getSubQueryId()) &&
          this.getId()== oid.getId();
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }
  
  public SubQueryId getSubQueryId() {
    ScheduleUnitIdProtoOrBuilder p = viaProto ? proto : builder;
    if (subQueryId != null) {
      return this.subQueryId;
    }
    if (!p.hasSubQueryId()) {
      return null;
    }
    this.subQueryId = new SubQueryId(p.getSubQueryId());
    return this.subQueryId;
  }
  
  public QueryId getQueryId() {
    return this.getSubQueryId().getQueryId();
  }
  
  private void mergeProtoToLocal() {
    ScheduleUnitIdProtoOrBuilder p = viaProto ? proto : builder;
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
      builder = ScheduleUnitIdProto.newBuilder(proto);
    }
    if (this.subQueryId != null) {
      builder.setSubQueryId(subQueryId.getProto());
    }
    if (this.id != -1) {
      builder.setId(id);
    }
  }

  @Override
  public ScheduleUnitIdProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    return proto;
  }
 
  public int getId() {
    ScheduleUnitIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id != -1) {
      return this.id;
    }
    if (!p.hasId()) {
      return -1;
    }
    this.id = p.getId();
    return this.id;
  }
}
