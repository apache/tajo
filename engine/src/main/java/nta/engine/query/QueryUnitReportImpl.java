/**
 * 
 */
package nta.engine.query;

import nta.engine.QueryUnitId;
import nta.engine.QueryUnitProtos.QueryUnitReportProto;
import nta.engine.QueryUnitProtos.QueryUnitReportProtoOrBuilder;
import nta.engine.ipc.QueryUnitReport;

/**
 * @author jihoon
 *
 */
public class QueryUnitReportImpl implements QueryUnitReport {
  
  private QueryUnitId id;
  private float progress;
  
  private QueryUnitReportProto proto;
  private QueryUnitReportProto.Builder builder;
  private boolean viaProto;
  
  public QueryUnitReportImpl() {
    builder = QueryUnitReportProto.newBuilder();
    viaProto = false;
    progress = -1.f;
  }
  
  public QueryUnitReportImpl(QueryUnitId id, float progress) {
    this();
    this.set(id, progress);
  }
  
  public QueryUnitReportImpl(QueryUnitReportProto proto) {
    this.proto = proto;
    viaProto = true;
    progress = -1.f;
  }
  
  public void set(QueryUnitId id, float progress) {
    this.id = id;
    this.progress = progress;
  }

  @Override
  public QueryUnitId getId() {
    QueryUnitReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id != null) {
      return this.id;
    }
    if (!p.hasId()) {
      return null;
    }
    this.id = new QueryUnitId(p.getId());
    return this.id;
  }

  @Override
  public float getProgress() {
    QueryUnitReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.progress != -1) {
      return this.progress;
    }
    if (!p.hasProgress()) {
      return -1.f;
    }
    this.progress = p.getProgress();
    return this.progress;
  }

  @Override
  public void initFromProto() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public QueryUnitReportProto getProto() {
    mergeLocalToProto();
    
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = QueryUnitReportProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
  
  private void mergeLocalToBuilder() {
    if (this.id != null) {
      builder.setId(this.id.toString());
    }
    if (this.progress != -1.f) {
      builder.setProgress(this.progress);
    }
  }
}
