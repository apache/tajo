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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
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
import java.util.Properties;

@ThreadSafe
public class RpcClientManager {
  private static final Log LOG = LogFactory.getLog(RpcClientManager.class);

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
                                                   Properties rpcParams)
      throws NoSuchMethodException, ConnectException, ClassNotFoundException {


    return makeClient(NettyUtils.getDefaultEventLoopGroup(), rpcConnectionKey, rpcParams);
  }

  private <T extends NettyClientBase> T makeClient(EventLoopGroup eventLoopGroup,
                                                   RpcConnectionKey rpcConnectionKey,
                                                   Properties rpcParams)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {
    NettyClientBase client;
    if (rpcConnectionKey.asyncMode) {
      client = new AsyncRpcClient(eventLoopGroup, rpcConnectionKey, rpcParams);

    } else {
      client = new BlockingRpcClient(eventLoopGroup, rpcConnectionKey, rpcParams);
    }
    return (T) client;
  }

  /**
   * Connect a {@link NettyClientBase} to the remote {@link NettyServerBase}, and returns rpc client by protocol.
   * This client will be shared per protocol and address. Client is removed in shared map when a client is closed
   */
  public <T extends NettyClientBase> T getClient(InetSocketAddress addr,
                                                 Class<?> protocolClass,
                                                 boolean asyncMode,
                                                 Properties rpcParams)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {
    RpcConnectionKey key = new RpcConnectionKey(addr, protocolClass, asyncMode);

    NettyClientBase client;
    synchronized (clients) {
      client = clients.get(key);
      if (client == null) {
        clients.put(key, client = makeClient(key, rpcParams));
      }
    }

    if (!client.isConnected()) {

      final NettyClientBase target = client;
      client.subscribeEvent(target.getKey(), new ChannelEventListener() {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) {
          /* Register client to managed map */
          clients.put(target.getKey(), target);
          ctx.channel().closeFuture().addListener(new ClientCloseFutureListener(target.getKey()));
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
  public <T extends NettyClientBase> T newClient(InetSocketAddress addr,
                                                              Class<?> protocolClass,
                                                              boolean asyncMode,
                                                              Properties rpcParams)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {

    return newClient(new RpcConnectionKey(addr, protocolClass, asyncMode), rpcParams);
  }

  /**
   *
   * @param key                 RpcConnectionKey
   * @param <T>                 Rpc Protocol Class
   * @return                    Rpc Client Class
   * @throws NoSuchMethodException
   * @throws ClassNotFoundException
   * @throws ConnectException
   */
  public synchronized <T extends NettyClientBase> T newClient(RpcConnectionKey key,
                                                              Properties connectionParameters)

      throws NoSuchMethodException, ClassNotFoundException, ConnectException {

    T client = makeClient(key, connectionParameters);
    client.connect();
    assert client.isConnected();
    return client;
  }

  public synchronized <T extends NettyClientBase> T newBlockingClient(InetSocketAddress addr,
                                                                      Class<?> protocolClass,
                                                                      EventLoopGroup eventLoopGroup,
                                                                      Properties rpcParams)
      throws NoSuchMethodException, ClassNotFoundException, ConnectException {

    T client = makeClient(eventLoopGroup, new RpcConnectionKey(addr, protocolClass, false), rpcParams);
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

  static class ClientCloseFutureListener implements ChannelFutureListener {
    private RpcConnectionKey key;

    public ClientCloseFutureListener(RpcConnectionKey key) {
      this.key = key;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      clients.remove(key);
    }
  }
}
