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

import io.netty.channel.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class NettyClientBase implements Closeable {
  private static Log LOG = LogFactory.getLog(NettyClientBase.class);
  private static final int CLIENT_CONNECTION_TIMEOUT_SEC = 60;
  private static final long PAUSE = 1000; // 1 sec
  private int numRetries;

  protected Bootstrap bootstrap;
  private EventLoopGroup loopGroup;
  private ChannelFuture channelFuture;

  public NettyClientBase() {
  }

  public abstract <T> T getStub();
  public abstract RpcConnectionPool.RpcConnectionKey getKey();
  
  public void init(InetSocketAddress addr, ChannelInitializer<Channel> initializer, EventLoopGroup loopGroup, 
      int numRetries) throws ConnectTimeoutException {
    this.numRetries = numRetries;
    
    init(addr, initializer, loopGroup);
  }

  public void init(InetSocketAddress addr, ChannelInitializer<Channel> initializer, EventLoopGroup loopGroup)
      throws ConnectTimeoutException {
    this.loopGroup = loopGroup;
    this.bootstrap = new Bootstrap();
    this.bootstrap.group(this.loopGroup)
      .channel(NioSocketChannel.class)
      .handler(initializer)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
      .option(ChannelOption.SO_RCVBUF, 1048576 * 10)
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.SO_KEEPALIVE, true);

    connect(addr);
  }

  private void connectUsingNetty(InetSocketAddress address, GenericFutureListener<ChannelFuture> listener) {

    this.channelFuture = bootstrap.clone().connect(address)
            .addListener(listener);
  }
  
  private void handleConnectionInternally(final InetSocketAddress addr) throws ConnectTimeoutException {
    final CountDownLatch latch = new CountDownLatch(1);
    GenericFutureListener<ChannelFuture> listener = new RetryConnectionListener(addr, latch);
    connectUsingNetty(addr, listener);

    try {
      latch.await(CLIENT_CONNECTION_TIMEOUT_SEC, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }

    if (!channelFuture.isSuccess()) {
      throw new ConnectTimeoutException("Connect error to " + addr +
          " caused by " + ExceptionUtils.getMessage(channelFuture.cause()));
    }
  }

  public void connect(InetSocketAddress addr) throws ConnectTimeoutException {
    if(addr.isUnresolved()){
       addr = NetUtils.createSocketAddr(addr.getHostName(), addr.getPort());
    }

    handleConnectionInternally(addr);
  }

  class RetryConnectionListener implements GenericFutureListener<ChannelFuture> {
    private final AtomicInteger retryCount = new AtomicInteger();
    private final InetSocketAddress address;
    private final CountDownLatch latch;

    RetryConnectionListener(InetSocketAddress address, CountDownLatch latch) {
      this.address = address;
      this.latch = latch;
    }

    @Override
    public void operationComplete(ChannelFuture channelFuture) throws Exception {
      if (!channelFuture.isSuccess()) {
        if (numRetries > retryCount.getAndIncrement()) {
          final GenericFutureListener<ChannelFuture> currentListener = this;

          loopGroup.schedule(new Runnable() {
            @Override
            public void run() {
              connectUsingNetty(address, currentListener);
            }
          }, PAUSE, TimeUnit.MILLISECONDS);

          LOG.debug("Connecting to " + address + " has been failed. Retrying to connect.");
        }
        else {
          latch.countDown();

          LOG.error("Max retry count has been exceeded. attempts=" + numRetries);
        }
      }
      else {
        latch.countDown();
      }
    }
  }

  public boolean isActive() {
    return getChannel().isActive();
  }

  public InetSocketAddress getRemoteAddress() {
    if (channelFuture == null || channelFuture.channel() == null) {
      return null;
    }
    return (InetSocketAddress) channelFuture.channel().remoteAddress();
  }

  public Channel getChannel() {
    return channelFuture.channel();
  }

  @Override
  public void close() {
    if (channelFuture != null && getChannel().isActive()) {
      getChannel().close().awaitUninterruptibly();
    }

    if (this.bootstrap != null) {
      InetSocketAddress address = getRemoteAddress();
      if (address != null) {
        LOG.debug("Proxy is disconnected from " + address.getHostName() + ":" + address.getPort());
      }
    }
  }
}
