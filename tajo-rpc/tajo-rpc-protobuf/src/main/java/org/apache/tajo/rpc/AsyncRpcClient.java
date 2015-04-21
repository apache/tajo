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

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.*;
import io.netty.channel.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.RpcClientManager.RpcConnectionKey;
import org.apache.tajo.rpc.RpcProtos.RpcRequest;
import org.apache.tajo.rpc.RpcProtos.RpcResponse;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class AsyncRpcClient extends NettyClientBase {
  private static final Log LOG = LogFactory.getLog(AsyncRpcClient.class);

  private final ConcurrentMap<Integer, ResponseCallback> requests =
      new ConcurrentHashMap<Integer, ResponseCallback>();

  private final Method stubMethod;
  private final ProxyRpcChannel rpcChannel;
  private final ClientChannelInboundHandler inboundHandler;

  /**
   * Intentionally make this method package-private, avoiding user directly
   * new an instance through this constructor.
   */
  AsyncRpcClient(RpcConnectionKey rpcConnectionKey, int retries)
      throws ClassNotFoundException, NoSuchMethodException {
    this(rpcConnectionKey, retries, 0);
  }

  AsyncRpcClient(RpcConnectionKey rpcConnectionKey, int retries, int idleTimeSeconds)
      throws ClassNotFoundException, NoSuchMethodException {
    super(rpcConnectionKey, retries);
    stubMethod = getServiceClass().getMethod("newStub", RpcChannel.class);
    rpcChannel = new ProxyRpcChannel();
    inboundHandler = new ClientChannelInboundHandler();
    init(new ProtoClientChannelInitializer(inboundHandler, RpcResponse.getDefaultInstance(), idleTimeSeconds));
  }

  @Override
  public <T> T getStub() {
    return getStub(stubMethod, rpcChannel);
  }

  protected void sendExceptions(String message) {
    for(Map.Entry<Integer, ResponseCallback> callbackEntry: requests.entrySet()) {
      ResponseCallback callback = callbackEntry.getValue();
      Integer id = callbackEntry.getKey();

      RpcResponse.Builder responseBuilder = RpcResponse.newBuilder()
          .setErrorMessage(message)
          .setId(id);

      callback.run(responseBuilder.build());
    }
  }

  @Override
  public void close() {
    sendExceptions("AsyncRpcClient terminates all the connections");

    super.close();
  }

  private class ProxyRpcChannel implements RpcChannel {

    public void callMethod(final MethodDescriptor method,
                           final RpcController controller,
                           final Message param,
                           final Message responseType,
                           final RpcCallback<Message> done) {

      int nextSeqId = sequence.getAndIncrement();

      Message rpcRequest = buildRequest(nextSeqId, method, param);

      inboundHandler.registerCallback(nextSeqId,
          new ResponseCallback(controller, responseType, done));

      invoke(rpcRequest, 0);
    }

    private void invoke(final Message rpcRequest, final int retry) {

      final ChannelPromise channelPromise = getChannel().newPromise();

      channelPromise.addListener(new GenericFutureListener<ChannelFuture>() {
        @Override
        public void operationComplete(final ChannelFuture future) throws Exception {
          if (!future.isSuccess()) {
            /* Repeat retry until the connection attempt succeeds or exceeded retries */
            if(!future.channel().isOpen() && retry < numRetries) {
              final EventLoop loop = future.channel().eventLoop();
              loop.schedule(new Runnable() {
                @Override
                public void run() {
                  AsyncRpcClient.this.doConnect(getKey().addr).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                      invoke(rpcRequest, retry + 1);
                    }
                  });
                }
              }, PAUSE, TimeUnit.MILLISECONDS);
            } else {
              inboundHandler.exceptionCaught(null, new ServiceException(future.cause()));
            }
          }
        }
      });
      getChannel().writeAndFlush(rpcRequest, channelPromise);
    }

    private Message buildRequest(int seqId,
                                 MethodDescriptor method,
                                 Message param) {

      RpcRequest.Builder requestBuilder = RpcRequest.newBuilder()
          .setId(seqId)
          .setMethodName(method.getName());

      if (param != null) {
          requestBuilder.setRequestMessage(param.toByteString());
      }

      return requestBuilder.build();
    }
  }

  private class ResponseCallback implements RpcCallback<RpcResponse> {
    private final RpcController controller;
    private final Message responsePrototype;
    private final RpcCallback<Message> callback;

    public ResponseCallback(RpcController controller,
                            Message responsePrototype,
                            RpcCallback<Message> callback) {
      this.controller = controller;
      this.responsePrototype = responsePrototype;
      this.callback = callback;
    }

    @Override
    public void run(RpcResponse rpcResponse) {
      // if hasErrorMessage is true, it means rpc-level errors.
      // it does not call the callback function\
      if (rpcResponse.hasErrorMessage()) {
        if (controller != null) {
          this.controller.setFailed(rpcResponse.getErrorMessage());
        }
        callback.run(null);
      } else { // if rpc call succeed

        Message responseMessage = null;
        if (rpcResponse.hasResponseMessage()) {

          try {
            responseMessage = responsePrototype.newBuilderForType().mergeFrom(
                rpcResponse.getResponseMessage()).build();
          } catch (InvalidProtocolBufferException e) {
            if (controller != null) {
              this.controller.setFailed(e.getMessage());
            }
          }
        }
        callback.run(responseMessage);
      }
    }
  }

  private String getErrorMessage(String message) {
    return "Exception [" + protocol.getCanonicalName() +
        "(" + RpcUtils.normalizeInetSocketAddress((InetSocketAddress)
        getChannel().remoteAddress()) + ")]: " + message;
  }

  @ChannelHandler.Sharable
  private class ClientChannelInboundHandler extends SimpleChannelInboundHandler<RpcResponse> {

    void registerCallback(int seqId, ResponseCallback callback) {

      if (requests.putIfAbsent(seqId, callback) != null) {
        throw new RemoteException(
            getErrorMessage("Duplicate Sequence Id "+ seqId));
      }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse response) throws Exception {
      ResponseCallback callback = requests.remove(response.getId());

      if (callback == null) {
        LOG.warn("Dangling rpc call");
      } else {
        callback.run(response);
      }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      LOG.info("Connection established successfully : " + ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      super.channelInactive(ctx);
      sendExceptions("Connection is closed :" + ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      sendExceptions(cause.getMessage());
      
      if(LOG.isDebugEnabled()) {
        LOG.error(getRemoteAddress() + "," + protocol + "," + cause.getMessage(), cause);
      } else {
        LOG.error(getRemoteAddress() + "," + protocol + "," + cause.getMessage());
      }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        if (e.state() == IdleState.WRITER_IDLE && requests.size() == 0) {
          /* If all requests is done and event is triggered, idle channel will be closed. */
//          LOG.info("If all requests is done and event is triggered, idle channel will be closed.");
//          ctx.close();
//          LOG.warn("Idle connection closed successfully :" + ctx.channel());
        }
      }
    }

    /*@Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        if (e.state() == IdleState.READER_IDLE) {
          if (enablePing) {
            *//* did not receive ping. may be server hangs*//*
            LOG.info("did not receive ping. may be server hangs");
            ctx.close();
          } else {
            if (requests.size() == 0) {
              *//* If all requests is done and event is triggered, idle channel will be closed. *//*
              LOG.info("If all requests is done and event is triggered, idle channel will be closed.");
              ctx.close();
              LOG.warn("Idle connection closed successfully :" + ctx.channel().remoteAddress());
            }
          }
        } else if (enablePing && e.state() == IdleState.WRITER_IDLE) {
          *//* ping packet*//*
          LOG.info("ping packet");
          ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
        }
      }
    }*/
  }
}