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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.RpcConnectionPool.RpcConnectionKey;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class NettyClientBase implements Closeable {
  private static final Log LOG = LogFactory.getLog(NettyClientBase.class);
  private static final int CONNECTION_TIMEOUT = 60000;  // 60 sec
  private static final long PAUSE = 1000; // 1 sec

  private final int numRetries;

  private Bootstrap bootstrap;
  private volatile ChannelFuture channelFuture;
  private volatile long lastConnected = -1;

  protected final Class<?> protocol;
  protected final AtomicInteger sequence = new AtomicInteger(0);

  private final RpcConnectionKey key;
  private final AtomicInteger counter = new AtomicInteger(0);   // reference counter

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
      .channel(NioSocketChannel.class)
      .handler(initializer)
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECTION_TIMEOUT)
      .option(ChannelOption.SO_RCVBUF, 1048576 * 10)
      .option(ChannelOption.TCP_NODELAY, true);
  }

  public RpcConnectionPool.RpcConnectionKey getKey() {
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

  public boolean acquire(long timeout) {
    if (!checkConnection(timeout)) {
      return false;
    }
    counter.incrementAndGet();
    return true;
  }

  public boolean release() {
    return counter.decrementAndGet() == 0;
  }

  private boolean checkConnection(long timeout) {
    return isConnected() || handleConnectionInternally(key.addr, timeout);
  }

  private InetSocketAddress resolveAddress(InetSocketAddress address) {
    if (address.isUnresolved()) {
      return RpcUtils.createSocketAddr(address.getHostName(), address.getPort());
    }
    return address;
  }

  private void connectUsingNetty(InetSocketAddress address, GenericFutureListener<ChannelFuture> listener) {
    if (lastConnected > 0) {
      LOG.warn("Try to reconnect : " + address);
    }
    this.channelFuture = bootstrap.clone().group(RpcChannelFactory.getSharedClientEventloopGroup())
            .connect(address)
            .addListener(listener);
  }

  // first attendant kicks connection
  private final RpcUtils.Scrutineer<CountDownLatch> connect = new RpcUtils.Scrutineer<CountDownLatch>();

  private boolean handleConnectionInternally(final InetSocketAddress addr, long timeout) {
    final CountDownLatch ticket = new CountDownLatch(1);
    final CountDownLatch granted = connect.check(ticket);

    // basically, it's double checked lock
    if (ticket == granted && isConnected()) {
      granted.countDown();
      return true;
    }

    if (ticket == granted) {
      InetSocketAddress address = resolveAddress(addr);
      connectUsingNetty(address, new RetryConnectionListener(address, granted));
    }

    try {
      granted.await(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // ignore
    }

    boolean success = channelFuture.isSuccess();

    if (granted.getCount() == 0) {
      connect.clear(granted);
    }

    return success;
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
        channelFuture.channel().close();

        if (numRetries > retryCount.getAndIncrement()) {

          RpcChannelFactory.getSharedClientEventloopGroup().schedule(new Runnable() {
            @Override
            public void run() {
              connectUsingNetty(address, RetryConnectionListener.this);
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
        lastConnected = System.currentTimeMillis();
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
      channel.close();
    }
  }
}
