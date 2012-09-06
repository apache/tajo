/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.query;

import tajo.engine.MasterWorkerProtos.InProgressStatusProto;
import tajo.engine.MasterWorkerProtos.PingRequestProto;
import tajo.engine.MasterWorkerProtos.PingRequestProtoOrBuilder;
import tajo.engine.ipc.PingRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PingRequestImpl implements PingRequest {
  private PingRequestProto proto;
  private PingRequestProto.Builder builder;
  private boolean viaProto;
  private Long timestamp;
  private String serverName;
  private List<InProgressStatusProto> inProgressQueries;
  
  public PingRequestImpl() {
    builder = PingRequestProto.newBuilder();
  }
  
  public PingRequestImpl(long timestamp, String serverName, 
      List<InProgressStatusProto> inProgress) {
    this();
    this.timestamp = timestamp;
    this.serverName = serverName;
    this.inProgressQueries = 
        new ArrayList<InProgressStatusProto>(inProgress);
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
  public Collection<InProgressStatusProto> getProgressList() {
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
