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

package tajo.engine.cluster;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import tajo.NConstants;
import tajo.engine.MasterWorkerProtos.*;
import tajo.engine.exception.UnknownWorkerException;
import tajo.ipc.AsyncWorkerCBProtocol;
import tajo.ipc.AsyncWorkerProtocol;
import tajo.rpc.Callback;
import tajo.rpc.NettyRpc;
import tajo.rpc.RemoteException;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkListener;
import tajo.zookeeper.ZkUtil;

import java.net.InetSocketAddress;
import java.util.*;

public class WorkerCommunicator extends ZkListener {
  private final Log LOG = LogFactory.getLog(LeafServerTracker.class);

  private ZkClient zkClient;
  private LeafServerTracker tracker;

  private Map<String, AsyncWorkerCBProtocol> hm =
      new MapMaker().concurrencyLevel(4).makeMap();

  public WorkerCommunicator(ZkClient zkClient, LeafServerTracker tracker)
      throws Exception {

    this.zkClient = zkClient;
    this.tracker = tracker;
  }

  public void start() throws Exception {
    this.zkClient.subscribe(this);
    ZkUtil.listChildrenAndWatchThem(zkClient, NConstants.ZNODE_LEAFSERVERS);
    update(this.tracker.getMembers());
  }
  
  private void update(final List<String> servers) {
    if (hm.size() > servers.size()) {
      HashSet<String> serverset = new HashSet<String>();
      serverset.addAll(servers);
      removeUpdate(serverset);
    } else if (hm.size() < servers.size()) {
      addUpdate(servers);
    }
  }

  private void removeUpdate(final Set<String> servers) {
    Iterator<String> iterator = hm.keySet().iterator();
    while (iterator.hasNext()) {
      String key = (String) iterator.next();
      if (!servers.contains(key)) {
        hm.remove(key);
      }
    }
  }

  private void addUpdate(final List<String> servers) {
    for (String servername : servers) {
      if (!hm.containsKey(servername)) {
        AsyncWorkerCBProtocol proxy = null;
        try {
        proxy = (AsyncWorkerCBProtocol) NettyRpc
            .getProtoParamAsyncRpcProxy(AsyncWorkerProtocol.class,
                AsyncWorkerCBProtocol.class, new InetSocketAddress(
                    extractHost(servername), extractPort(servername)));
        } catch (Exception e) {
          LOG.error("cannot connect to the worker (" + servername + ")");
          try {
            zkClient.delete(ZkUtil.concat(NConstants.ZNODE_LEAFSERVERS, servername));
          } catch (InterruptedException ie) {
            ie.printStackTrace();
          } catch (KeeperException ke) {
            ke.printStackTrace();
          }
        }
        if (proxy != null) {
          hm.put(servername, proxy);
        }
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
    AsyncWorkerCBProtocol leaf = hm.get(serverName);
    if (leaf == null) {
      throw new UnknownWorkerException(serverName);
    }
    leaf.requestQueryUnit(cb, requestProto);
    return cb;
  }

  public Callback<BoolProto> requestQueryUnit(String serverName,
      int port, QueryUnitRequestProto requestProto) throws Exception {
    return this.requestQueryUnit(serverName + ":" + port, requestProto);
  }

  public Callback<ServerStatusProto> getServerStatus(String serverName)
      throws RemoteException, InterruptedException, UnknownWorkerException {
    Callback<ServerStatusProto> cb = new Callback<ServerStatusProto>();
    AsyncWorkerCBProtocol leaf = hm.get(serverName);
    if (leaf == null) {
      throw new UnknownWorkerException(serverName);
    }
    leaf.getServerStatus(cb, NullProto.newBuilder().build());
    return cb;
  }
  
  public Callback<CommandResponseProto> requestCommand(String serverName, 
      CommandRequestProto request) throws UnknownWorkerException {
    Preconditions.checkArgument(serverName != null);
    Callback<CommandResponseProto> cb = new Callback<CommandResponseProto>();
    AsyncWorkerCBProtocol leaf = hm.get(serverName);
    if (leaf == null) {
      throw new UnknownWorkerException(serverName);
    }
    leaf.requestCommand(cb, request);

    return cb;
  }
  
  public Callback<CommandResponseProto> requestCommand(String serverName, 
      int port, CommandRequestProto request) throws UnknownWorkerException {
    return this.requestCommand(serverName + ":" + port, request);
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(NConstants.ZNODE_LEAFSERVERS)) {
      try {
        ZkUtil.listChildrenAndWatchThem(zkClient, NConstants.ZNODE_LEAFSERVERS);
        update(tracker.getMembers());
      } catch (KeeperException e) {
        LOG.error(e.getMessage(), e);
      }
    }

  }

  public Map<String, AsyncWorkerCBProtocol> getProxyMap() {
    return hm;
  }

  public void close() {
    this.zkClient.unsubscribe(this);
    this.zkClient.close();
  }
}