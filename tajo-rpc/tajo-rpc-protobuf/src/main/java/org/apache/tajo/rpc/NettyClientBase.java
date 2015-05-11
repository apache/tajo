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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.RpcClientManager.RpcConnectionKey;
import org.apache.tajo.rpc.RpcProtos.RpcResponse;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public abstract class NettyClientBase<T> implements ProtoDeclaration, Closeable {
  public final static Log LOG = LogFactory.getLog(NettyClientBase.class);

  private Bootstrap bootstrap;
  private volatile ChannelFuture channelFuture;
  private final RpcConnectionKey key;
  private final int maxRetries;
  private boolean enableMonitor;

  private final ConcurrentMap<RpcConnectionKey, ChannelEventListener> channelEventListeners =
      new ConcurrentHashMap<RpcConnectionKey, ChannelEventListener>();
  private final ConcurrentMap<Integer, T> requests = new ConcurrentHashMap<Integer, T>();

  public NettyClientBase(RpcConnectionKey rpcConnectionKey, int numRetries)
      throws ClassNotFoundException, NoSuchMethodException {
    this.key = rpcConnectionKey;
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
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, RpcConstants.DEFAULT_CONNECT_TIMEOUT)
        .option(ChannelOption.SO_RCVBUF, 1048576 * 10)
        .option(ChannelOption.TCP_NODELAY, true);
  }

  public RpcClientManager.RpcConnectionKey getKey() {
    return key;
  }

  protected final Class<?> getServiceClass() throws ClassNotFoundException {
    String serviceClassName = getKey().protocolClass.getName() + "$" +
        getKey().protocolClass.getSimpleName() + "Service";
    return Class.forName(serviceClassName);
  }

  @SuppressWarnings("unchecked")
  protected final <I> I getStub(Method stubMethod, Object rpcChannel) {
    try {
      return (I) stubMethod.invoke(null, rpcChannel);
    } catch (Exception e) {
      throw new RemoteException(e.getMessage(), e);
    }
  }

  protected static RpcProtos.RpcRequest buildRequest(int seqId,
                                        Descriptors.MethodDescriptor method,
                                        Message param) {
    RpcProtos.RpcRequest.Builder requestBuilder = RpcProtos.RpcRequest.newBuilder()
        .setId(seqId)
        .setMethodName(method.getName());

    if (param != null) {
      requestBuilder.setRequestMessage(param.toByteString());
    }

    return requestBuilder.build();
  }

  /**
   * Repeat invoke rpc request until the connection attempt succeeds or exceeded retries
   */
  protected void invoke(final RpcProtos.RpcRequest rpcRequest, final T callback, final int retry) {

    ChannelPromise promise = getChannel().newPromise();
    promise.addListener(new GenericFutureListener<ChannelFuture>() {

      @Override
      public void operationComplete(final ChannelFuture future) throws Exception {

        if (future.isSuccess()) {

          getHandler().registerCallback(rpcRequest.getId(), callback);
        } else {

          if (!future.channel().isActive() && retry < maxRetries) {

            /* schedule the current request for the retry */
            LOG.warn(future.cause() + " Try to reconnect :" + getKey().addr);

            final EventLoop loop = future.channel().eventLoop();
            loop.schedule(new Runnable() {
              @Override
              public void run() {
                doConnect(getKey().addr).addListener(new GenericFutureListener<ChannelFuture>() {
                  @Override
                  public void operationComplete(ChannelFuture future) throws Exception {
                    invoke(rpcRequest, callback, retry + 1);
                  }
                });
              }
            }, RpcConstants.DEFAULT_PAUSE, TimeUnit.MILLISECONDS);
          } else {

            /* Max retry count has been exceeded or internal failure */
            getHandler().registerCallback(rpcRequest.getId(), callback);
            getHandler().exceptionCaught(getChannel().pipeline().lastContext(),
                new RecoverableException(rpcRequest.getId(), future.cause()));
          }
        }
      }
    });
    getChannel().writeAndFlush(rpcRequest, promise);
  }

  private static InetSocketAddress resolveAddress(InetSocketAddress address) {
    if (address.isUnresolved()) {
      return RpcUtils.createSocketAddr(address.getHostName(), address.getPort());
    }
    return address;
  }

  private ChannelFuture doConnect(SocketAddress address) {
    return this.channelFuture = bootstrap.clone().connect(address);
  }

  public synchronized void connect() throws ConnectException {
    if (isConnected()) return;

    int retries = 0;
    InetSocketAddress address = key.addr;
    if (address.isUnresolved()) {
      address = resolveAddress(address);
    }

    /* do not call await() inside handler */
    ChannelFuture f = doConnect(address).awaitUninterruptibly();

    if (!f.isSuccess()) {
      if (maxRetries > 0) {
        doReconnect(address, f, ++retries);
      } else {
        throw new ConnectException(ExceptionUtils.getMessage(f.cause()));
      }
    }
  }

  private void doReconnect(final InetSocketAddress address, ChannelFuture future, int retries)
      throws ConnectException {

    for (; ; ) {
      if (maxRetries > retries) {
        retries++;

        LOG.warn(getErrorMessage(ExceptionUtils.getMessage(future.cause())) + " Try to reconnect : " + getKey().addr);
        try {
          Thread.sleep(RpcConstants.DEFAULT_PAUSE);
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

  protected abstract NettyChannelInboundHandler getHandler();

  public Channel getChannel() {
    return channelFuture == null ? null : channelFuture.channel();
  }

  public boolean isConnected() {
    Channel channel = getChannel();
    return channel != null && channel.isActive();
  }

  public SocketAddress getRemoteAddress() {
    Channel channel = getChannel();
    return channel == null ? null : channel.remoteAddress();
  }

  public int getActiveRequests() {
    return requests.size();
  }

  public boolean subscribeEvent(RpcConnectionKey key, ChannelEventListener listener) {
    return channelEventListeners.putIfAbsent(key, listener) == null;
  }

  public void removeSubscribers() {
    channelEventListeners.clear();
  }

  public Collection<ChannelEventListener> getSubscribers() {
    return channelEventListeners.values();
  }

  private String getErrorMessage(String message) {
    return "Exception [" + getKey().protocolClass.getCanonicalName() +
        "(" + getKey().addr + ")]: " + message;
  }

  @Override
  public void close() {
    Channel channel = getChannel();
    if (channel != null && channel.isOpen()) {
      LOG.debug("Proxy will be disconnected from remote " + channel.remoteAddress());
      /* channelInactive receives event and then client terminates all the requests */
      channel.close().syncUninterruptibly();
    }
  }

  protected abstract class NettyChannelInboundHandler extends SimpleChannelInboundHandler<RpcResponse> {

    protected void registerCallback(int seqId, T callback) {
      if (requests.putIfAbsent(seqId, callback) != null) {
        throw new RemoteException(
            getErrorMessage("Duplicate Sequence Id " + seqId));
      }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      MonitorClientHandler handler = ctx.pipeline().get(MonitorClientHandler.class);
      if (handler != null) {
        enableMonitor = true;
      }

      for (ChannelEventListener listener : getSubscribers()) {
        listener.channelRegistered(ctx);
      }
      super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      for (ChannelEventListener listener : getSubscribers()) {
        listener.channelUnregistered(ctx);
      }
      super.channelUnregistered(ctx);

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      LOG.debug("Connection established successfully : " + ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      super.channelInactive(ctx);
      sendExceptions("Connection lost :" + getKey().addr);
    }

    @Override
    protected final void channelRead0(ChannelHandlerContext ctx, RpcResponse response) throws Exception {
      T callback = requests.remove(response.getId());
      if (callback == null)
        LOG.warn("Dangling rpc call");
      else run(response, callback);
    }

    /**
     * A {@link #channelRead0} received a message.
     * @param response response proto of type {@link RpcResponse}.
     * @param callback callback of type {@link T}.
     * @throws Exception
     */
    protected abstract void run(RpcResponse response, T callback) throws Exception;

    /**
     * Calls from exceptionCaught
     * @param requestId sequence id of request.
     * @param callback callback of type {@link T}.
     * @param message the error message to handle
     */
    protected abstract void handleException(int requestId, T callback, String message);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {

      Throwable rootCause = ExceptionUtils.getRootCause(cause);
      LOG.error(getErrorMessage(ExceptionUtils.getMessage(rootCause)), rootCause);

      if (cause instanceof RecoverableException) {
        sendException((RecoverableException) cause);
      } else {
        /* unrecoverable fatal error*/
        sendExceptions(ExceptionUtils.getMessage(rootCause));
        if (ctx.channel().isOpen()) {
          ctx.close();
        }
      }
    }

    /**
     * Send an error to all callback
     */
    private void sendExceptions(String message) {
      for (int requestId : requests.keySet()) {
        handleException(requestId, requests.remove(requestId), message);
      }
    }

    /**
     * Send an error to callback
     */
    private void sendException(RecoverableException e) {
      T callback = requests.remove(e.getSeqId());

      if (callback != null) {
        handleException(e.getSeqId(), callback, ExceptionUtils.getRootCauseMessage(e));
      }
    }

    /**
     * Trigger timeout event
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

      if (!enableMonitor && evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        /* If all requests is done and event is triggered, idle channel close. */
        if (e.state() == IdleState.READER_IDLE && requests.isEmpty()) {
          ctx.close();
          LOG.info("Idle connection closed successfully :" + ctx.channel());
        }
      } else if (evt instanceof MonitorStateEvent) {
        MonitorStateEvent e = (MonitorStateEvent) evt;
        if (e.state() == MonitorStateEvent.MonitorState.PING_EXPIRED) {
          exceptionCaught(ctx, new ServiceException("Server has not respond: " + ctx.channel()));
        }
      }

      super.userEventTriggered(ctx, evt);
    }
  }
}
