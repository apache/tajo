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
import io.netty.channel.ConnectTimeoutException;
import io.netty.util.internal.logging.CommonsLoggerFactory;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class RpcConnectionPool {
  private static final Log LOG = LogFactory.getLog(RpcConnectionPool.class);

  private Map<RpcConnectionKey, NettyClientBase> connections =
      new HashMap<RpcConnectionKey, NettyClientBase>();

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
      client = new AsyncRpcClient(rpcConnectionKey, RPC_RETRIES);
    } else {
      client = new BlockingRpcClient(rpcConnectionKey, RPC_RETRIES);
    }
    return client;
  }

  public static final long DEFAULT_TIMEOUT = 3000;
  public static final long DEFAULT_INTERVAL = 500;

  public NettyClientBase getConnection(InetSocketAddress addr,
                                       Class<?> protocolClass, boolean asyncMode)
      throws NoSuchMethodException, ClassNotFoundException, ConnectTimeoutException {
    return getConnection(addr, protocolClass, asyncMode, DEFAULT_TIMEOUT, DEFAULT_INTERVAL);
  }

  public NettyClientBase getConnection(InetSocketAddress addr,
      Class<?> protocolClass, boolean asyncMode, long timeout, long interval)
      throws NoSuchMethodException, ClassNotFoundException, ConnectTimeoutException {
    RpcConnectionKey key = new RpcConnectionKey(addr, protocolClass, asyncMode);

    RpcUtils.Timer timer = new RpcUtils.Timer(timeout);
    for (; !timer.isTimedOut(); timer.elapsed()) {
      NettyClientBase client;
      synchronized (lockObject) {
        client = connections.get(key);
        if (client == null) {
          connections.put(key, client = makeConnection(key));
        }
      }
      if (client.acquire(timer.remaining())) {
        return client;
      }
      timer.interval(interval);
    }

    throw new ConnectTimeoutException("Failed to get connection for " + timeout + " msec");
  }

  public void releaseConnection(NettyClientBase client) {
    if (client != null) {
      release(client, false);
    }
  }

  public void closeConnection(NettyClientBase client) {
    if (client != null) {
      release(client, true);
    }
  }

  private void release(NettyClientBase client, boolean close) {
    try {
      if (returnToPool(client, close)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Closing connection [" + client.getKey() + "]");
        }
        client.close();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Current Connections in pool [" + connections.size() + "]");
      }
    } catch (Exception e) {
      LOG.error("Can't close connection:" + client.getKey() + ":" + e.getMessage(), e);
    }
  }

  // return true if the connection should be closed
  private boolean returnToPool(NettyClientBase client, boolean close) {
    synchronized (lockObject) {
      if (client.release() && (close || !client.isConnected())) {
        connections.remove(client.getKey());
        return true;
      }
    }
    return false;
  }

  public void close() {
    if(LOG.isDebugEnabled()) {
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

  public void shutdown(){
    close();
    RpcChannelFactory.shutdownGracefully();
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
      this.description = "["+ protocolClass + "] " + addr + "," + asyncMode;
    }

    @Override
    public String toString() {
      return description;
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
      return description.hashCode();
    }
  }
}
