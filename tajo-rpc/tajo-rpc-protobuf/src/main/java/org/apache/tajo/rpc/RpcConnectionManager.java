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

import io.netty.channel.ConnectTimeoutException;
import io.netty.util.internal.logging.CommonsLoggerFactory;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

@ThreadSafe
public class RpcConnectionManager {
  private static final Log LOG = LogFactory.getLog(RpcConnectionManager.class);

  public static final int RPC_RETRIES = 3;
  private final static Map<RpcConnectionKey, NettyClientBase> connections = new HashMap();

  private static RpcConnectionManager instance;
  private final static Object lockObject = new Object();

  static {
    InternalLoggerFactory.setDefaultFactory(new CommonsLoggerFactory());
    instance = new RpcConnectionManager();
  }

  private RpcConnectionManager() {
  }

  public static RpcConnectionManager getInstance() {
    return instance;
  }

  private NettyClientBase makeConnection(RpcConnectionKey rpcConnectionKey)
      throws NoSuchMethodException, ClassNotFoundException, ConnectTimeoutException {
    NettyClientBase client;
    if (rpcConnectionKey.asyncMode) {
      client = new AsyncRpcClient(rpcConnectionKey, RPC_RETRIES);
    } else {
      client = new BlockingRpcClient(rpcConnectionKey, RPC_RETRIES);
    }
    return client;
  }

  public NettyClientBase getConnection(InetSocketAddress addr,
                                       Class<?> protocolClass, boolean asyncMode)
      throws NoSuchMethodException, ClassNotFoundException, ConnectTimeoutException {
    RpcConnectionKey key = new RpcConnectionKey(addr, protocolClass, asyncMode);

    NettyClientBase client;
    synchronized (lockObject) {
      client = connections.get(key);
      if (client == null) {
        connections.put(key, client = makeConnection(key));
      }
    }

    if (!client.isConnected()) {
      client.connect();
    }
    assert client.isConnected();
    return client;
  }

  public static void close() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Pool Closed");
    }

    synchronized (lockObject) {
      for (NettyClientBase eachClient : connections.values()) {
        try {
          eachClient.close();
        } catch (Exception e) {
          LOG.error("close client pool error", e);
        }
      }
      connections.clear();
    }
  }

  public static void shutdown() {
    close();
    RpcChannelFactory.shutdownGracefully();
  }

  protected static NettyClientBase remove(RpcConnectionKey key) {
    synchronized (lockObject) {
      return connections.remove(key);
    }
  }

  protected static boolean contains(RpcConnectionKey key) {
    synchronized (lockObject) {
      return connections.containsKey(key);
    }
  }

  public static void cleanup(NettyClientBase... clients) {
    for (NettyClientBase client : clients) {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Exception in closing " + client, e);
          }
        }
      }
    }
  }

  static class RpcConnectionKey {
    final InetSocketAddress addr;
    final Class<?> protocolClass;
    final boolean asyncMode;

    final String description;

    public RpcConnectionKey(InetSocketAddress addr,
                            Class<?> protocolClass, boolean asyncMode) {
      this.addr = addr;
      this.protocolClass = protocolClass;
      this.asyncMode = asyncMode;
      this.description = "[" + protocolClass + "] " + addr + "," + asyncMode;
    }

    @Override
    public String toString() {
      return description;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof RpcConnectionKey)) {
        return false;
      }

      return toString().equals(obj.toString());
    }

    @Override
    public int hashCode() {
      return description.hashCode();
    }
  }
}
