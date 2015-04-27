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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@ThreadSafe
public class RpcClientManager {
  private static final Log LOG = LogFactory.getLog(RpcClientManager.class);

  public static final int RPC_RETRIES = 3;

  /* If all requests is done and client is idle state, client will be removed. */
  public static final int RPC_IDLE_TIMEOUT = 43200; // 12 hour

  /* entries will be removed by ConnectionCloseFutureListener */
  private static final Map<RpcConnectionKey, NettyClientBase>
      clients = Collections.synchronizedMap(new HashMap<RpcConnectionKey, NettyClientBase>());

  private static RpcClientManager instance;

  static {
    InternalLoggerFactory.setDefaultFactory(new CommonsLoggerFactory());
    instance = new RpcClientManager();
  }

  private RpcClientManager() {
  }

  public static RpcClientManager getInstance() {
    return instance;
  }

  private NettyClientBase makeClient(RpcConnectionKey rpcConnectionKey)
      throws NoSuchMethodException, ClassNotFoundException, ConnectTimeoutException {
    NettyClientBase client;
    if (rpcConnectionKey.asyncMode) {
      client = new AsyncRpcClient(rpcConnectionKey, RPC_RETRIES, RPC_IDLE_TIMEOUT);
    } else {
      client = new BlockingRpcClient(rpcConnectionKey, RPC_RETRIES, RPC_IDLE_TIMEOUT);
    }
    return client;
  }

  /**
   * Connect a {@link NettyClientBase} to the remote {@link NettyServerBase}, and returns rpc client by protocol.
   * This client will be shared per protocol and address. Client is removed in shared map when a client is closed
   * @param addr
   * @param protocolClass
   * @param asyncMode
   * @return
   * @throws NoSuchMethodException
   * @throws ClassNotFoundException
   * @throws ConnectTimeoutException
   */
  public NettyClientBase getClient(InetSocketAddress addr,
                                   Class<?> protocolClass, boolean asyncMode)
      throws NoSuchMethodException, ClassNotFoundException, ConnectTimeoutException {
    RpcConnectionKey key = new RpcConnectionKey(addr, protocolClass, asyncMode);

    NettyClientBase client;
    synchronized (clients) {
      client = clients.get(key);
      if (client == null) {
        clients.put(key, client = makeClient(key));
      }
    }

    if (!client.isConnected()) {
      client.connect();
      client.getChannel().closeFuture().addListener(new ConnectionCloseFutureListener(key));
    }
    assert client.isConnected();
    return client;
  }

  /**
   * Request to close this clients
   * After it is closed, it is removed from clients map.
   */
  public static void close() {
    LOG.info("Closing RPC client manager");

    for (NettyClientBase eachClient : clients.values()) {
      try {
        eachClient.close();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  /**
   * Close client manager and shutdown Netty RPC worker pool
   * After it is shutdown it is not possible to reuse it again.
   */
  public static void shutdown() {
    close();
    RpcChannelFactory.shutdownGracefully();
  }

  protected static NettyClientBase remove(RpcConnectionKey key) {
    LOG.debug("Removing shared rpc client :" + key);
    return clients.remove(key);
  }

  protected static boolean contains(RpcConnectionKey key) {
    return clients.containsKey(key);
  }

  public static void cleanup(NettyClientBase... clients) {
    for (NettyClientBase client : clients) {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Exception in closing " + client.getKey(), e);
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
