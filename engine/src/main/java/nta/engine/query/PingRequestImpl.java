package nta.engine.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.MasterInterfaceProtos.PingRequestProto;
import nta.engine.MasterInterfaceProtos.PingRequestProtoOrBuilder;
import nta.engine.ipc.PingRequest;

/**
 * @author jihoon
 * @author Hyunsik Choi
 * 
 */
public class PingRequestImpl implements PingRequest {
  private PingRequestProto proto;
  private PingRequestProto.Builder builder;
  private boolean viaProto;
  private Long timestamp;
  private String serverName;
  private List<InProgressStatus> inProgressQueries;
  
  public PingRequestImpl() {
    builder = PingRequestProto.newBuilder();
  }
  
  public PingRequestImpl(long timestamp, String serverName, 
      List<InProgressStatus> inProgress) {
    this();
    this.timestamp = timestamp;
    this.serverName = serverName;
    this.inProgressQueries = 
        new ArrayList<InProgressStatus>(inProgress);
  }
  
  public PingRequestImpl(PingRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  private void initProgress() {
    if (this.inProgressQueries != null) {
      return;
    }
    PingRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.inProgressQueries = p.getStatusList();
  }
  
  public Long timestamp() {
    PingRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (timestamp != null) {
      return this.timestamp;
    }
    if (!p.hasTimestamp()) {
      return null;
    }
    timestamp = p.getTimestamp();
    
    return timestamp;
  }
  
  public String getServerName() {
    PingRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (serverName != null) {
      return this.serverName;
    }
    if (!p.hasServerName()) {
      return null;
    }
    serverName = p.getServerName();
    
    return serverName;
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
  public PingRequestProto getProto() {
    mergeLocalToProto();
    
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = PingRequestProto.newBuilder(proto);
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
    if (this.timestamp != null) {
      builder.setTimestamp(timestamp);
    }
    if (this.serverName != null) {
      builder.setServerName(serverName);
    }
    if (this.inProgressQueries != null) {
      builder.clearStatus();
      builder.addAllStatus(this.inProgressQueries);
    }    
  }
}
