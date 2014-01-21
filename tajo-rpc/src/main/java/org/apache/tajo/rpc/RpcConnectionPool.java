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

package org.apache.tajo.rpc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.TUtil;

import java.net.InetSocketAddress;
import java.util.Map;

public class RpcConnectionPool {
  private static final Log LOG = LogFactory.getLog(RpcConnectionPool.class);

  private Map<RpcConnectionKey, NettyClientBase> connections = TUtil.newConcurrentHashMap();

  private static RpcConnectionPool instance;

  private TajoConf conf;

  private RpcConnectionPool(TajoConf conf) {
    this.conf = conf;
  }

  public synchronized static RpcConnectionPool getPool(TajoConf conf) {
    if(instance == null) {
      instance = new RpcConnectionPool(conf);
    }

    return instance;
  }

  private NettyClientBase makeConnection(RpcConnectionKey rpcConnectionKey) throws Exception {
    if(rpcConnectionKey.asyncMode) {
      return new AsyncRpcClient(rpcConnectionKey.protocolClass, rpcConnectionKey.addr);
    } else {
      return new BlockingRpcClient(rpcConnectionKey.protocolClass, rpcConnectionKey.addr);
    }
  }

  public NettyClientBase getConnection(InetSocketAddress addr,
      Class protocolClass, boolean asyncMode) throws Exception {
    RpcConnectionKey key = new RpcConnectionKey(addr, protocolClass, asyncMode);
    synchronized(connections) {
      if(!connections.containsKey(key)) {
        connections.put(key, makeConnection(key));
      }
      return connections.get(key);
    }
  }

  public void releaseConnection(NettyClientBase client) {
  }

  public void closeConnection(NettyClientBase client) {
    if (client == null) {
      return;
    }

    try {
      if(LOG.isDebugEnabled()) {
        LOG.debug("CloseConnection [" + client.getKey() + "]");
      }
      synchronized(connections) {
        connections.remove(client.getKey());
      }
      client.close();
    } catch (Exception e) {
      LOG.error("Can't close connection:" + client.getKey() + ":" + e.getMessage(), e);
    }
  }

  public synchronized void close() {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Pool Closed");
    }
    synchronized(connections) {
      for(NettyClientBase eachClient: connections.values()) {
        try {
          eachClient.close();
        } catch (Exception e) {
          LOG.error("close client pool error", e);
        }
      }
      connections.clear();
    }
  }

  static class RpcConnectionKey {
    final InetSocketAddress addr;
    final Class protocolClass;
    final boolean asyncMode;

    public RpcConnectionKey(InetSocketAddress addr,
                            Class protocolClass, boolean asyncMode) {
      this.addr = addr;
      this.protocolClass = protocolClass;
      this.asyncMode = asyncMode;
    }

    @Override
    public String toString() {
      return protocolClass + "," + addr + "," + asyncMode;
    }

    @Override
    public boolean equals(Object obj) {
      if(!(obj instanceof RpcConnectionKey)) {
        return false;
      }

      return toString().equals(obj.toString());
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }
  }
}
