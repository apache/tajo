package nta.engine.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nta.engine.QueryUnitProtos.InProgressStatus;
import nta.engine.QueryUnitProtos.QueryUnitReportProto;
import nta.engine.QueryUnitProtos.QueryUnitReportProtoOrBuilder;
import nta.engine.ipc.QueryUnitReport;

/**
 * @author jihoon
 * @author Hyunsik Choi
 * 
 */
public class QueryUnitReportImpl implements QueryUnitReport {
  private QueryUnitReportProto proto;
  private QueryUnitReportProto.Builder builder;
  private boolean viaProto;
  private List<InProgressStatus> inProgressQueries;
  
  public QueryUnitReportImpl() {
    builder = QueryUnitReportProto.newBuilder();
  }
  
  public QueryUnitReportImpl(List<InProgressStatus> inProgress) {
    this();
    this.inProgressQueries = 
        new ArrayList<InProgressStatus>(inProgress);
  }
  
  public QueryUnitReportImpl(QueryUnitReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  private void initProgress() {
    if (this.inProgressQueries != null) {
      return;
    }
    QueryUnitReportProtoOrBuilder p = viaProto ? proto : builder;
    this.inProgressQueries = p.getStatusList();
  }

  @Override
  public Collection<InProgressStatus> getProgressList() {
    initProgress();
    return inProgressQueries;
  }

  @Override
  public void initFromProto() {    
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
    if (this.inProgressQueries != null) {
      builder.clearStatus();
      builder.addAllStatus(this.inProgressQueries);
    }
  }
}
