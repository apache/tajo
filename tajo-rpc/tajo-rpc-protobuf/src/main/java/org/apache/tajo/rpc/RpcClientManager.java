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
import io.netty.channel.EventLoopGroup;
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
import java.util.concurrent.TimeUnit;

@ThreadSafe
public class RpcClientManager {
  private static final Log LOG = LogFactory.getLog(RpcClientManager.class);

  private volatile int timeoutSeconds = RpcConstants.DEFAULT_RPC_TIMEOUT_SECONDS;
  private volatile int retries = RpcConstants.DEFAULT_RPC_RETRIES;

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

  private <T extends NettyClientBase> T makeClient(RpcConnectionKey rpcConnectionKey,
                                                   int retries,
                                                   long timeout,
                                                   TimeUnit timeUnit,
                                                   boolean enablePing)
      throws NoSuchMethodException, ConnectException, ClassNotFoundException {
    return makeClient(rpcConnectionKey, retries, timeout, timeUnit, enablePing, NettyUtils.getDefaultEventLoopGroup());
  }

  private <T extends NettyClientBase> T makeClient(RpcConnectionKey rpcConnectionKey,
                                                   int retries,
                                                   long timeout,
                                                   TimeUnit timeUnit,
                                                   boolean enablePing,
                                                   EventLoopGroup eventLoopGroup)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {
    NettyClientBase client;
    if (rpcConnectionKey.asyncMode) {
      client = new AsyncRpcClient(rpcConnectionKey, retries, timeout, timeUnit, enablePing, eventLoopGroup);
    } else {
      client = new BlockingRpcClient(rpcConnectionKey, retries, timeout, timeUnit, enablePing, eventLoopGroup);
    }
    return (T) client;
  }

  /**
   * Connect a {@link NettyClientBase} to the remote {@link NettyServerBase}, and returns rpc client by protocol.
   * This client will be shared per protocol and address. Client is removed in shared map when a client is closed
   */
  public <T extends NettyClientBase> T getClient(InetSocketAddress addr,
                                                 Class<?> protocolClass, boolean asyncMode)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {
    RpcConnectionKey key = new RpcConnectionKey(addr, protocolClass, asyncMode);

    NettyClientBase client;
    synchronized (clients) {
      client = clients.get(key);
      if (client == null) {
        clients.put(key, client = makeClient(key, retries, getTimeoutSeconds(), TimeUnit.SECONDS, true));
      }
    }

    if (!client.isConnected()) {

      final NettyClientBase target = client;
      client.subscribeEvent(target.getKey(), new ChannelEventListener() {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) {
          /* Register client to managed map */
          clients.put(target.getKey(), target);
          target.getChannel().closeFuture().addListener(new ClientCloseFutureListener(target.getKey()));
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) {
          // nothing to do
        }
      });
    }

    client.connect();
    assert client.isConnected();
    return (T) client;
  }

  /**
   * Connect a {@link NettyClientBase} to the remote {@link NettyServerBase}, and returns rpc client by protocol.
   * This client does not managed. It should close.
   */
  public synchronized <T extends NettyClientBase> T newClient(InetSocketAddress addr,
                                                              Class<?> protocolClass,
                                                              boolean asyncMode,
                                                              int retries,
                                                              long timeout,
                                                              TimeUnit timeUnit,
                                                              boolean enablePing)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {

    return newClient(new RpcConnectionKey(addr, protocolClass, asyncMode), retries, timeout, timeUnit, enablePing);
  }

  public synchronized <T extends NettyClientBase> T newClient(InetSocketAddress addr,
                                                              Class<?> protocolClass,
                                                              boolean asyncMode)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {

    return newClient(new RpcConnectionKey(addr, protocolClass, asyncMode),
        retries, getTimeoutSeconds(), TimeUnit.SECONDS, true);
  }

  public synchronized <T extends NettyClientBase> T newClient(RpcConnectionKey key,
                                                              int retries,
                                                              long timeout,
                                                              TimeUnit timeUnit,
                                                              boolean enablePing)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {

    T client = makeClient(key, retries, timeout, timeUnit, enablePing);
    client.connect();
    assert client.isConnected();
    return client;
  }

  public synchronized <T extends NettyClientBase> T newBlockingClient(InetSocketAddress addr,
                                                                      Class<?> protocolClass,
                                                                      int retries,
                                                                      EventLoopGroup eventLoopGroup)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {

    T client = makeClient(new RpcConnectionKey(addr, protocolClass, false),
        retries, 0, TimeUnit.SECONDS, false, eventLoopGroup);
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

    synchronized (clients) {
      for (NettyClientBase eachClient : clients.values()) {
        try {
          eachClient.close();
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }

  /**
   * Close client manager and shutdown Netty RPC worker pool
   * After it is shutdown it is not possible to reuse it again.
   */
  public static void shutdown() {
    NettyUtils.shutdownGracefully();
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

  public int getTimeoutSeconds() {
    return timeoutSeconds;
  }

  public void setTimeoutSeconds(int timeoutSeconds) {
    this.timeoutSeconds = timeoutSeconds;
  }

  public int getRetries() {
    return retries;
  }

  public void setRetries(int retries) {
    this.retries = retries;
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
