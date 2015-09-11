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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.MethodDescriptor;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import org.apache.tajo.rpc.RpcClientManager.RpcConnectionKey;
import org.apache.tajo.rpc.RpcProtos.RpcResponse;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingRpcClient extends NettyClientBase<BlockingRpcClient.ProtoCallFuture> {

  private final Method stubMethod;
  private final ProxyRpcChannel rpcChannel;
  private final NettyChannelInboundHandler handler;

  @VisibleForTesting
  BlockingRpcClient(RpcConnectionKey rpcConnectionKey, int retries)
      throws NoSuchMethodException, ClassNotFoundException {
    this(rpcConnectionKey, retries, 0, TimeUnit.NANOSECONDS, false, NettyUtils.getDefaultEventLoopGroup());
  }

  /**
   * Intentionally make this method package-private, avoiding user directly
   * new an instance through this constructor.
   *
   * @param rpcConnectionKey
   * @param retries          retry operation number of times
   * @param timeout          disable ping, it trigger timeout event on idle-state.
   *                         otherwise it is request timeout on active-state
   * @param timeUnit         TimeUnit
   * @param enablePing       enable to detect remote peer hangs
   * @param eventLoopGroup   thread pool of netty's
   * @throws ClassNotFoundException
   * @throws NoSuchMethodException
   */
  BlockingRpcClient(RpcConnectionKey rpcConnectionKey, int retries, long timeout, TimeUnit timeUnit, boolean enablePing,
                    EventLoopGroup eventLoopGroup) throws ClassNotFoundException, NoSuchMethodException {
    super(rpcConnectionKey, retries);

    this.stubMethod = getServiceClass().getMethod("newBlockingStub", BlockingRpcChannel.class);
    this.rpcChannel = new ProxyRpcChannel();
    this.handler = new ClientChannelInboundHandler();
    init(new ProtoClientChannelInitializer(handler,
        RpcResponse.getDefaultInstance(),
        timeUnit.toNanos(timeout),
        enablePing), eventLoopGroup);
  }

  @Override
  public <I> I getStub() {
    return getStub(stubMethod, rpcChannel);
  }

  @Override
  protected NettyChannelInboundHandler getHandler() {
    return handler;
  }

  private class ProxyRpcChannel implements BlockingRpcChannel {

    private final AtomicInteger sequence = new AtomicInteger(0);

    @Override
    public Message callBlockingMethod(final MethodDescriptor method,
                                      final RpcController controller,
                                      final Message param,
                                      final Message responsePrototype)
        throws TajoServiceException {

      int nextSeqId = sequence.getAndIncrement();
      RpcProtos.RpcRequest rpcRequest = buildRequest(nextSeqId, method, param);
      ProtoCallFuture callFuture = new ProtoCallFuture(controller, responsePrototype);

      invoke(rpcRequest, callFuture, 0);

      try {
        return callFuture.get();
      } catch (Throwable t) {
        if (t instanceof ExecutionException) {
          Throwable cause = t.getCause();
          if (cause != null && cause instanceof TajoServiceException) {
            throw (TajoServiceException) cause;
          }
        }
        throw new TajoServiceException(t.getMessage());
      }
    }
  }

  private TajoServiceException makeTajoServiceException(RpcResponse response, Throwable cause) {
    if (getChannel() != null) {
      return new TajoServiceException(response.getErrorMessage(), cause, getKey().protocolClass.getName(),
          RpcUtils.normalizeInetSocketAddress((InetSocketAddress) getChannel().remoteAddress()));
    } else {
      return new TajoServiceException(response.getErrorMessage());
    }
  }

  @ChannelHandler.Sharable
  public class ClientChannelInboundHandler extends NettyChannelInboundHandler {

    @Override
    protected void run(RpcResponse rpcResponse, ProtoCallFuture callback) throws Exception {
      if (rpcResponse.hasErrorMessage()) {
        callback.setFailed(rpcResponse.getErrorMessage(),
            makeTajoServiceException(rpcResponse, new ServiceException(rpcResponse.getErrorTrace())));
      } else {
        Message responseMessage = null;

        if (rpcResponse.hasResponseMessage()) {
          try {
            responseMessage = callback.returnType.newBuilderForType().mergeFrom(rpcResponse.getResponseMessage())
                .build();
          } catch (InvalidProtocolBufferException e) {
            callback.setFailed(e.getMessage(), e);
          }
        }
        callback.setResponse(responseMessage);
      }
    }

    @Override
    protected void handleException(int requestId, ProtoCallFuture callback, String message) {
      callback.setFailed(message + "", new TajoServiceException(message));
    }
  }

  static class ProtoCallFuture implements Future<Message> {
    private Semaphore sem = new Semaphore(0);
    private boolean done = false;
    private Message response = null;
    private Message returnType;

    private RpcController controller;

    private ExecutionException ee;

    public ProtoCallFuture(RpcController controller, Message message) {
      this.controller = controller;
      this.returnType = message;
    }

    @Override
    public boolean cancel(boolean arg0) {
      return false;
    }

    @Override
    public Message get() throws InterruptedException, ExecutionException {
      if(!isDone()) sem.acquire();

      if (ee != null) {
        throw ee;
      }
      return response;
    }

    @Override
    public Message get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if(!isDone()) {
        if (!sem.tryAcquire(timeout, unit)) {
          throw new TimeoutException();
        }
      }

      if (ee != null) {
        throw ee;
      }
      return response;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    public void setResponse(Message response) {
      this.response = response;
      done = true;
      sem.release();
    }

    public void setFailed(String errorText, Throwable t) {
      if (controller != null) {
        this.controller.setFailed(errorText);
      }
      ee = new ExecutionException(errorText, t);
      done = true;
      sem.release();
    }
  }
}