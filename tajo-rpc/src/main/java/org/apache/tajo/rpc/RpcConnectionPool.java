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

import com.google.common.base.Objects;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.CommonsLoggerFactory;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class RpcConnectionPool {
  private static final Log LOG = LogFactory.getLog(RpcConnectionPool.class);

  private Map<RpcConnectionKey, NettyClientBase> connections =
      new HashMap<RpcConnectionKey, NettyClientBase>();
  private ChannelGroup accepted = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private static RpcConnectionPool instance;
  private final Object lockObject = new Object();

  public final static int RPC_RETRIES = 3;

  private RpcConnectionPool() {
  }

  public synchronized static RpcConnectionPool getPool() {
    if(instance == null) {
      InternalLoggerFactory.setDefaultFactory(new CommonsLoggerFactory());
      instance = new RpcConnectionPool();
    }
    return instance;
  }

  private NettyClientBase makeConnection(RpcConnectionKey rpcConnectionKey)
      throws NoSuchMethodException, ClassNotFoundException, ConnectTimeoutException {
    NettyClientBase client;
    if(rpcConnectionKey.asyncMode) {
      client = new AsyncRpcClient(rpcConnectionKey.protocolClass, rpcConnectionKey.addr, 
          RPC_RETRIES);
    } else {
      client = new BlockingRpcClient(rpcConnectionKey.protocolClass, rpcConnectionKey.addr, 
          RPC_RETRIES);
    }
    accepted.add(client.getChannel());
    return client;
  }

  public NettyClientBase getConnection(InetSocketAddress addr,
                                       Class<?> protocolClass, boolean asyncMode)
      throws NoSuchMethodException, ClassNotFoundException, ConnectTimeoutException {
    RpcConnectionKey key = new RpcConnectionKey(addr, protocolClass, asyncMode);
    NettyClientBase client = connections.get(key);

    if (client == null) {
      boolean added = false;
      synchronized (lockObject){
        client = connections.get(key);
        if (client == null) {
          client = makeConnection(key);
          connections.put(key, client);
          added = true;
        }
      }

      if (!added) {
        if (client != null) {
          client.close();
        }
        client = connections.get(key);
      }
    }

    if (!client.getChannel().isOpen() || !client.getChannel().isActive()) {
      LOG.warn("Try to reconnect : " + addr);
      client.connect(addr);
    }
    return client;
  }

  public void releaseConnection(NettyClientBase client) {
    if (client == null) return;

    try {
      if (!client.getChannel().isOpen()) {
        connections.remove(client.getKey());
        client.close();
      }

      if(LOG.isDebugEnabled()) {
        LOG.debug("Current Connections [" + connections.size() + "] Accepted: " + accepted.size());

      }
    } catch (Exception e) {
      LOG.error("Can't close connection:" + client.getKey() + ":" + e.getMessage(), e);
    }
  }

  public void closeConnection(NettyClientBase client) {
    if (client == null) {
      return;
    }

    try {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Close connection [" + client.getKey() + "]");
      }

      connections.remove(client.getKey());
      client.close();

    } catch (Exception e) {
      LOG.error("Can't close connection:" + client.getKey() + ":" + e.getMessage(), e);
    }
  }

  public synchronized void close() {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Pool Closed");
    }
    synchronized(lockObject) {
      for(NettyClientBase eachClient: connections.values()) {
        try {
          eachClient.close();
        } catch (Exception e) {
          LOG.error("close client pool error", e);
        }
      }

      connections.clear();
    }

    try {
      accepted.close();
    } catch (Throwable t) {
      LOG.error(t);
    }
  }

  public synchronized void shutdown(){
    close();
    RpcChannelFactory.shutdownGracefully();
  }

  static class RpcConnectionKey {
    final InetSocketAddress addr;
    final Class<?> protocolClass;
    final boolean asyncMode;

    public RpcConnectionKey(InetSocketAddress addr,
                            Class<?> protocolClass, boolean asyncMode) {
      this.addr = addr;
      this.protocolClass = protocolClass;
      this.asyncMode = asyncMode;
    }

    @Override
    public String toString() {
      return "["+ protocolClass + "] " + addr + "," + asyncMode;
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
      return Objects.hashCode(addr, asyncMode);
    }
  }
}
