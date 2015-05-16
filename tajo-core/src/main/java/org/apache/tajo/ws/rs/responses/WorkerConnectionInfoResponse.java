/**
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

package org.apache.tajo.ws.rs.responses;

import org.apache.tajo.master.cluster.WorkerConnectionInfo;

import com.google.gson.annotations.Expose;

public class WorkerConnectionInfoResponse {

  @Expose private int id;

  @Expose private String host;

  @Expose private int peerRpcPort;

  @Expose private int pullServerPort;

  @Expose private int queryMasterPort;

  @Expose private int clientPort;

  @Expose private int httpInfoPort;
  
  public WorkerConnectionInfoResponse(WorkerConnectionInfo connectionInfo) {
    this.id = connectionInfo.getId();
    this.host = connectionInfo.getHost();
    this.peerRpcPort = connectionInfo.getPeerRpcPort();
    this.pullServerPort = connectionInfo.getPullServerPort();
    this.clientPort = connectionInfo.getClientPort();
    this.queryMasterPort = connectionInfo.getQueryMasterPort();
    this.httpInfoPort = connectionInfo.getHttpInfoPort();
  }

  public int getId() {
    return id;
  }

  public String getHost() {
    return host;
  }

  public int getPeerRpcPort() {
    return peerRpcPort;
  }

  public int getPullServerPort() {
    return pullServerPort;
  }

  public int getQueryMasterPort() {
    return queryMasterPort;
  }

  public int getClientPort() {
    return clientPort;
  }

  public int getHttpInfoPort() {
    return httpInfoPort;
  }
}
