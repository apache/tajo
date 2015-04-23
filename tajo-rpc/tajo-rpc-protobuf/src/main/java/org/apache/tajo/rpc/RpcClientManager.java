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

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.logging.CommonsLoggerFactory;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@ThreadSafe
public class RpcClientManager {
  private static final Log LOG = LogFactory.getLog(RpcClientManager.class);

  public static final int RPC_RETRIES = 3;
  public static final int DEFAULT_IDLE_TIMEOUT_SECONDS = 60;
  private int idleTimeoutSeconds = DEFAULT_IDLE_TIMEOUT_SECONDS;

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

  private NettyClientBase makeClient(RpcConnectionKey rpcConnectionKey,
                                     int retries,
                                     int idleTimeout,
                                     boolean enablePing)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {
    NettyClientBase client;
    if (rpcConnectionKey.asyncMode) {
      client = new AsyncRpcClient(rpcConnectionKey, retries, idleTimeout, enablePing);
    } else {
      client = new BlockingRpcClient(rpcConnectionKey, retries, idleTimeout, enablePing);
    }
    return client;
  }

  /**
   * Connect a {@link NettyClientBase} to the remote {@link NettyServerBase}, and returns rpc client by protocol.
   * This client will be shared per protocol and address. Client is removed in shared map when a client is closed
   */
  public NettyClientBase getClient(InetSocketAddress addr,
                                   Class<?> protocolClass, boolean asyncMode)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {
    RpcConnectionKey key = new RpcConnectionKey(addr, protocolClass, asyncMode);

    NettyClientBase client;
    synchronized (clients) {
      client = clients.get(key);
      if (client == null) {
        clients.put(key, client = makeClient(key, RPC_RETRIES, getIdleTimeoutSeconds(), true));
      }
    }

    if (!client.isConnected()) {
      client.connect();
      client.getChannel().closeFuture().addListener(new ClientCloseFutureListener(key));

      final NettyClientBase finalClient = client;
      client.subscribeEvent(new ChannelEventListener() {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) {
          /* if client is recovered, it should be add client map */
          clients.put(finalClient.getKey(), finalClient);
          finalClient.getChannel().closeFuture().addListener(new ClientCloseFutureListener(finalClient.getKey()));
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) {
          /* if channel is reused, event is not fired */
        }
      });
    }

    assert client.isConnected();
    return client;
  }

  /**
   * Connect a {@link NettyClientBase} to the remote {@link NettyServerBase}, and returns rpc client by protocol.
   * This client can not managed. It must be closed.
   */
  public synchronized NettyClientBase newClient(InetSocketAddress addr,
                                                Class<?> protocolClass,
                                                boolean asyncMode,
                                                int retries,
                                                int idleTimeout,
                                                boolean enablePing)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {
    RpcConnectionKey key = new RpcConnectionKey(addr, protocolClass, asyncMode);

    NettyClientBase client = makeClient(key, retries, idleTimeout, enablePing);
    client.connect();
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

  public int getIdleTimeoutSeconds() {
    return idleTimeoutSeconds;
  }

  public void setIdleTimeoutSeconds(int idleTimeoutSeconds) {
    this.idleTimeoutSeconds = idleTimeoutSeconds;
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

  static class ClientCloseFutureListener implements GenericFutureListener {
    private RpcClientManager.RpcConnectionKey key;

    public ClientCloseFutureListener(RpcClientManager.RpcConnectionKey key) {
      this.key = key;
    }

    @Override
    public void operationComplete(Future future) throws Exception {
      clients.remove(key);
    }
  }
}
