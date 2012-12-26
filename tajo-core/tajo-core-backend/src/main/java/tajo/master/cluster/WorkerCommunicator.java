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

package tajo.master.cluster;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import tajo.engine.MasterWorkerProtos.CommandRequestProto;
import tajo.engine.MasterWorkerProtos.CommandResponseProto;
import tajo.engine.MasterWorkerProtos.QueryUnitRequestProto;
import tajo.engine.MasterWorkerProtos.ServerStatusProto;
import tajo.engine.exception.UnknownWorkerException;
import tajo.ipc.AsyncWorkerCBProtocol;
import tajo.ipc.AsyncWorkerProtocol;
import tajo.master.cluster.event.WorkerEvent;
import tajo.master.cluster.event.WorkerEventType;
import tajo.rpc.Callback;
import tajo.rpc.NettyRpc;
import tajo.rpc.RemoteException;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerCommunicator extends AbstractService
    implements EventHandler<WorkerEvent> {
  private final Log LOG = LogFactory.getLog(WorkerCommunicator.class);

  private final WorkerTracker tracker;
  private final Map<String, AsyncWorkerCBProtocol> proxies = new ConcurrentHashMap<>();
  private final EventHandler eventHandler;

  public WorkerCommunicator(WorkerTracker tracker, EventHandler eventHandler)
      throws Exception {
    super(WorkerCommunicator.class.getName());
    this.tracker = tracker;
    this.eventHandler = eventHandler;
  }

  public void init(Configuration conf) {
    super.init(conf);
  }

  public void start() {
    Collection<String> workerAddrs = this.tracker.getMembers();

    for (String workerAddr : workerAddrs) {
      connect(workerAddr);
    }

    super.start();
  }

  public void stop() {
    proxies.clear();
    super.stop();
  }

  private void connect(String nodeName) {
    AsyncWorkerCBProtocol proxy;
    if (!proxies.containsKey(nodeName)) {
      proxy = (AsyncWorkerCBProtocol) NettyRpc
          .getProtoParamAsyncRpcProxy(AsyncWorkerProtocol.class,
              AsyncWorkerCBProtocol.class, new InetSocketAddress(
              extractHost(nodeName), extractPort(nodeName)));
      if (proxy != null) {
        proxies.put(nodeName, proxy);
      }
    }
  }

  private void disconnect(String workerAddr) {
    proxies.remove(workerAddr);
  }

  @Override
  public void handle(WorkerEvent event) {
    if (event.getType() == WorkerEventType.JOIN) {
      for (String workerAddr : event.getWorkerNames()) {
        connect(workerAddr);
      }
    } else if (event.getType() == WorkerEventType.LEAVE) {
      for (String workerAddr : event.getWorkerNames()) {
        disconnect(workerAddr);
      }
    }
  }

  private String extractHost(String servername) {
    return servername.substring(0, servername.indexOf(":"));
  }

  private int extractPort(String servername) {
    return Integer.parseInt(servername.substring(servername.indexOf(":") + 1));
  }

  public Callback<BoolProto> requestQueryUnit(String serverName,
                                              QueryUnitRequestProto requestProto) throws Exception {
    Callback<BoolProto> cb = new Callback<BoolProto>();
//    AsyncWorkerCBProtocol leaf = proxies.get(serverName);
//    if (leaf == null) {
//      throw new UnknownWorkerException(serverName);
//    }
//    leaf.requestQueryUnit(cb, requestProto);
    return cb;
  }

  public Callback<ServerStatusProto> getServerStatus(String serverName)
      throws RemoteException, InterruptedException, UnknownWorkerException {
    Callback<ServerStatusProto> cb = new Callback<ServerStatusProto>();
    AsyncWorkerCBProtocol leaf = proxies.get(serverName);
    if (leaf == null) {
      throw new UnknownWorkerException(serverName);
    }
    leaf.getServerStatus(cb, NullProto.newBuilder().build());
    return cb;
  }

  public Callback<CommandResponseProto> requestCommand(String workerAddr,
      CommandRequestProto request) throws UnknownWorkerException {

    Preconditions.checkArgument(workerAddr != null);
    Callback<CommandResponseProto> cb = new Callback<CommandResponseProto>();
    AsyncWorkerCBProtocol leaf = proxies.get(workerAddr);
    if (leaf == null) {
      throw new UnknownWorkerException(workerAddr);
    }
    leaf.requestCommand(cb, request);

    return cb;
  }

  public Callback<CommandResponseProto> requestCommand(String serverName,
      int port, CommandRequestProto request) throws UnknownWorkerException {

    return this.requestCommand(serverName + ":" + port, request);
  }

  public Map<String, AsyncWorkerCBProtocol> getProxyMap() {
    return proxies;
  }
}