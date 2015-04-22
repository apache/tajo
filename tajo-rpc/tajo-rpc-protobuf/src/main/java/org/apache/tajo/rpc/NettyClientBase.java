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

import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.RpcClientManager.RpcConnectionKey;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class NettyClientBase implements Closeable {
  private static final Log LOG = LogFactory.getLog(NettyClientBase.class);
  private static final int CONNECTION_TIMEOUT = 60000;  // 60 sec
  protected static final int PAUSE = 1000; // 1 sec

  protected final int maxRetries;

  private Bootstrap bootstrap;
  private volatile ChannelFuture channelFuture;

  protected final Class<?> protocol;
  protected final AtomicInteger sequence = new AtomicInteger(0);

  private final RpcConnectionKey key;

  public NettyClientBase(RpcConnectionKey rpcConnectionKey, int numRetries)
      throws ClassNotFoundException, NoSuchMethodException {
    this.key = rpcConnectionKey;
    this.protocol = rpcConnectionKey.protocolClass;
    this.maxRetries = numRetries;
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

  /**
   *  Repeat invoke rpc request until the connection attempt succeeds or exceeded retries
   */
  protected void invoke(final Message rpcRequest, final int retry) {

    final ChannelPromise channelPromise = getChannel().newPromise();

    channelPromise.addListener(new GenericFutureListener<ChannelFuture>() {
      @Override
      public void operationComplete(final ChannelFuture future) throws Exception {
        if (!future.isSuccess()) {

          if(!future.channel().isOpen() && retry < maxRetries) {
            LOG.warn(future.cause() + " Try to reconnect :" + getKey().addr);
            final EventLoop loop = future.channel().eventLoop();

            loop.schedule(new Runnable() {
              @Override
              public void run() {
                doConnect(getKey().addr).addListener(new ChannelFutureListener() {
                  @Override
                  public void operationComplete(ChannelFuture future) throws Exception {
                    invoke(rpcRequest, retry + 1);
                  }
                });
              }
            }, PAUSE, TimeUnit.MILLISECONDS);
          } else {
            getChannel().pipeline().fireExceptionCaught(new ServiceException(future.cause()));
          }
        } else {
          RpcClientManager.put(NettyClientBase.this);
        }
      }
    });
    getChannel().writeAndFlush(rpcRequest, channelPromise);
  }

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

    int retries = 0;
    InetSocketAddress address = key.addr;
    if (address.isUnresolved()) {
      address = resolveAddress(address);
    }

    /* do not call await() inside handler */
    ChannelFuture f = doConnect(address).awaitUninterruptibly();
    retries++;

    if (!f.isSuccess() && maxRetries > 0) {
      doReconnect(address, f, ++retries);
    }
  }

  private void doReconnect(final InetSocketAddress address, ChannelFuture future, int retries)
      throws ConnectTimeoutException {

    for (; ; ) {
      if (maxRetries >= retries++) {
        LOG.warn(future.cause() + " Try to reconnect");
        try {
          Thread.sleep(PAUSE);
        } catch (InterruptedException e) {
        }

        this.channelFuture = doConnect(address).awaitUninterruptibly();
        if (this.channelFuture.isDone() && this.channelFuture.isSuccess()) {
          break;
        }
      } else {
        throw new ConnectTimeoutException("Max retry count has been exceeded. attempts=" + retries
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
