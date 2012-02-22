package nta.engine;

import java.text.NumberFormat;

import nta.common.ProtoObject;
import nta.engine.query.QueryIdProtos.QueryUnitIdProto;
import nta.engine.query.QueryIdProtos.QueryUnitIdProtoOrBuilder;

/**
 * @author Hyunsik Choi
 */
public class QueryUnitId implements Comparable<QueryUnitId>, 
ProtoObject<QueryUnitIdProto> {
  private static final NumberFormat format = NumberFormat.getInstance();
  static {
    format.setGroupingUsed(false);
    format.setMinimumIntegerDigits(6);
  }
  
  private QueryStepId stepId = null;
  private int id = -1;
  private String finalId = null;
  
  private QueryUnitIdProto proto = QueryUnitIdProto.getDefaultInstance();
  private QueryUnitIdProto.Builder builder = null;
  private boolean viaProto = false;
  
  public QueryUnitId() {
    builder = QueryUnitIdProto.newBuilder();
  }
  
  public QueryUnitId(final QueryStepId stepId,
      final int id) {
    this.stepId = stepId;
    this.id = id;
  }
  
  public QueryUnitId(QueryUnitIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public QueryUnitId(final String finalId) {
    this.finalId = finalId;
    int i = finalId.lastIndexOf(QueryId.SEPERATOR);
    this.stepId = new QueryStepId(finalId.substring(0, i));
    this.id = Integer.valueOf(finalId.substring(i+1));
  }
  
  public QueryStepId getQueryStepId() {
    return this.stepId;
  }
  
  @Override
  public final String toString() {
    if (finalId == null) {
      finalId = this.stepId + 
          QueryId.SEPERATOR + format.format(id);
    }
    return this.finalId;
  }
  
  @Override
  public final boolean equals(final Object o) {
    if (o instanceof QueryUnitId) {
      QueryUnitId other = (QueryUnitId) o;
      return this.toString().equals(other.toString());
    }    
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  @Override
  public final int compareTo(final QueryUnitId o) {
    return this.toString().compareTo(o.toString());
  }
  
  private void mergeProtoToLocal() {
    QueryUnitIdProtoOrBuilder p = viaProto ? proto : builder;
    if (stepId == null) {
      stepId = new QueryStepId(p.getQueryStepId());
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
    if (builder == null) {
      builder = QueryUnitIdProto.newBuilder(proto);
    }
    if (this.stepId != null) {
      builder.setQueryStepId(stepId.toString());
    }
    if (this.id != -1) {
      builder.setId(id);
    }
  }

  @Override
  public QueryUnitIdProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    return proto;
  }
}
