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

package org.apache.tajo.master.cluster;

import org.apache.tajo.common.ProtoObject;

import static org.apache.tajo.TajoProtos.WorkerConnectionInfoProto;

public class WorkerConnectionInfo implements ProtoObject<WorkerConnectionInfoProto>, Comparable<WorkerConnectionInfo> {
  private WorkerConnectionInfoProto.Builder builder;
  /**
  * unique worker id
  */
  private int id;
  /**
   * Hostname
   */
  private String host;
  /**
   * Peer rpc port
   */
  private int peerRpcPort;
  /**
   * pull server port
   */
  private int pullServerPort;
  /**
   * QueryMaster rpc port
   */
  private int queryMasterPort;
  /**
   * the port of client rpc which provides an client API
   */
  private int clientPort;
  /**
   * http info port
   */
  private int httpInfoPort;

  public WorkerConnectionInfo() {
    builder = WorkerConnectionInfoProto.newBuilder();
  }

  public WorkerConnectionInfo(WorkerConnectionInfoProto proto) {
    this();
    this.id = proto.getId();
    this.host = proto.getHost();
    this.peerRpcPort = proto.getPeerRpcPort();
    this.pullServerPort = proto.getPullServerPort();
    this.clientPort = proto.getClientPort();
    this.httpInfoPort = proto.getHttpInfoPort();
    this.queryMasterPort = proto.getQueryMasterPort();
  }

  public WorkerConnectionInfo(String host, int peerRpcPort, int pullServerPort, int clientPort,
                              int queryMasterPort, int httpInfoPort) {
    this();
    this.host = host;
    this.peerRpcPort = peerRpcPort;
    this.pullServerPort = pullServerPort;
    this.clientPort = clientPort;
    this.queryMasterPort = queryMasterPort;
    this.httpInfoPort = httpInfoPort;
    this.id = hashCode();
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

  public int getId() {
    return id;
  }

  public String getHostAndPeerRpcPort() {
    return this.getHost() + ":" + this.getPeerRpcPort();
  }

  @Override
  public WorkerConnectionInfoProto getProto() {
    builder.setId(id)
        .setHost(host)
        .setPeerRpcPort(peerRpcPort)
        .setPullServerPort(pullServerPort)
        .setClientPort(clientPort)
        .setHttpInfoPort(httpInfoPort)
        .setQueryMasterPort(queryMasterPort);
    return builder.build();
  }

  @Override
  public int hashCode() {
    final int prime = 493217;
    int result = 8501;
    result = prime * result + this.getHost().hashCode();
    result = prime * result + this.getPeerRpcPort();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    WorkerConnectionInfo other = (WorkerConnectionInfo) obj;
    if (!this.getHost().equals(other.getHost()))
      return false;
    if (this.getPeerRpcPort() != other.getPeerRpcPort())
      return false;
    return true;
  }

  @Override
  public int compareTo(WorkerConnectionInfo other) {
    int hostCompare = this.getHost().compareTo(other.getHost());
    if (hostCompare == 0) {
      if (this.getPeerRpcPort() > other.getPeerRpcPort()) {
        return 1;
      } else if (this.getPeerRpcPort() < other.getPeerRpcPort()) {
        return -1;
      }
      return 0;
    }
    return hostCompare;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("id:").append(id).append(", ")
        .append("host:").append(host).append(", ")
        .append("PeerRpcPort:").append(peerRpcPort).append(", ")
        .append("PullServerPort:").append(pullServerPort).append(", ")
        .append("ClientPort:").append(clientPort).append(", ")
        .append("QueryMasterPort:").append(queryMasterPort).append(", ")
        .append("HttpInfoPort:").append(httpInfoPort);
    return builder.toString();
  }
}