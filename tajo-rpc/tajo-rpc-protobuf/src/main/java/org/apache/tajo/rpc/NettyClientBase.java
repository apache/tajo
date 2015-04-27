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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.RpcClientManager.RpcConnectionKey;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class NettyClientBase implements Closeable {
  private static final Log LOG = LogFactory.getLog(NettyClientBase.class);
  private static final int CONNECTION_TIMEOUT = 60000;  // 60 sec
  private static final long PAUSE = 1000; // 1 sec

  private final int numRetries;

  private Bootstrap bootstrap;
  private volatile ChannelFuture channelFuture;

  protected final Class<?> protocol;
  protected final AtomicInteger sequence = new AtomicInteger(0);

  private final RpcConnectionKey key;

  public NettyClientBase(RpcConnectionKey rpcConnectionKey, int numRetries)
      throws ClassNotFoundException, NoSuchMethodException {
    this.key = rpcConnectionKey;
    this.protocol = rpcConnectionKey.protocolClass;
    this.numRetries = numRetries;
  }

  // should be called from sub class
  protected void init(ChannelInitializer<Channel> initializer) {
    this.bootstrap = new Bootstrap();
    this.bootstrap
        .group(RpcChannelFactory.getSharedClientEventloopGroup())
      .channel(NioSocketChannel.class)
      .handler(initializer)
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECTION_TIMEOUT)
      .option(ChannelOption.SO_RCVBUF, 1048576 * 10)
      .option(ChannelOption.TCP_NODELAY, true);
  }

  public RpcClientManager.RpcConnectionKey getKey() {
    return key;
  }

  protected final Class<?> getServiceClass() throws ClassNotFoundException {
    String serviceClassName = protocol.getName() + "$" + protocol.getSimpleName() + "Service";
    return Class.forName(serviceClassName);
  }

  @SuppressWarnings("unchecked")
  protected final <T> T getStub(Method stubMethod, Object rpcChannel) {
    try {
      return (T) stubMethod.invoke(null, rpcChannel);
    } catch (Exception e) {
      throw new RemoteException(e.getMessage(), e);
    }
  }

  public abstract <T> T getStub();


  private InetSocketAddress resolveAddress(InetSocketAddress address) {
    if (address.isUnresolved()) {
      return RpcUtils.createSocketAddr(address.getHostName(), address.getPort());
    }
    return address;
  }

  private ChannelFuture doConnect(SocketAddress address) {
    return this.channelFuture = bootstrap.clone().connect(address);
  }


  public synchronized void connect() throws ConnectTimeoutException {
    if (isConnected()) return;

    final AtomicInteger retries = new AtomicInteger();
    InetSocketAddress address = key.addr;
    if (address.isUnresolved()) {
      address = resolveAddress(address);
    }

    /* do not call await() inside handler */
    ChannelFuture f = doConnect(address).awaitUninterruptibly();
    retries.incrementAndGet();

    if (!f.isSuccess() && numRetries > 0) {
      doReconnect(address, f, retries);
    }
  }

  private void doReconnect(final InetSocketAddress address, ChannelFuture future, AtomicInteger retries)
      throws ConnectTimeoutException {

    for (; ; ) {
      if (numRetries >= retries.getAndIncrement()) {

        LOG.warn(future.cause().getMessage() + " Try to reconnect");
        try {
          Thread.sleep(PAUSE);
        } catch (InterruptedException e) {
        }

        this.channelFuture = doConnect(address).awaitUninterruptibly();
        if (this.channelFuture.isDone() && this.channelFuture.isSuccess()) {
          break;
        }
      } else {
        throw new ConnectTimeoutException("Max retry count has been exceeded. attempts=" + numRetries
            + " caused by: " + future.cause());
      }
    }
  }

  public Channel getChannel() {
    return channelFuture == null ? null : channelFuture.channel();
  }

  public boolean isConnected() {
    Channel channel = getChannel();
    return channel != null && channel.isOpen() && channel.isActive();
  }

  public SocketAddress getRemoteAddress() {
    Channel channel = getChannel();
    return channel == null ? null : channel.remoteAddress();
  }

  @Override
  public void close() {
    Channel channel = getChannel();
    if (channel != null && channel.isOpen()) {
      LOG.debug("Proxy will be disconnected from remote " + channel.remoteAddress());
      channel.close().awaitUninterruptibly();
    }
  }
}
