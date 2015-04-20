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
    init(new ProtoChannelInitializer(inboundHandler, RpcResponse.getDefaultInstance(), idleTimeSeconds));
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
                           RpcCallback<Message> done) {

      int nextSeqId = sequence.getAndIncrement();

      Message rpcRequest = buildRequest(nextSeqId, method, param);

      inboundHandler.registerCallback(nextSeqId,
          new ResponseCallback(controller, responseType, done));

      ChannelPromise channelPromise = getChannel().newPromise();
      channelPromise.addListener(new GenericFutureListener<ChannelFuture>() {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (!future.isSuccess()) {
            inboundHandler.exceptionCaught(null, new ServiceException(future.cause()));
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
        try {
          Message responseMessage;
          if (!rpcResponse.hasResponseMessage()) {
            responseMessage = null;
          } else {
            responseMessage = responsePrototype.newBuilderForType().mergeFrom(
                rpcResponse.getResponseMessage()).build();
          }

          callback.run(responseMessage);

        } catch (InvalidProtocolBufferException e) {
          throw new RemoteException(getErrorMessage(""), e);
        }
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
      LOG.info("Connection established successfully : " + ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      LOG.error(getRemoteAddress() + "," + protocol + "," + cause.getMessage(), cause);

      sendExceptions(cause.getMessage());
      
      if(LOG.isDebugEnabled()) {
        LOG.error(cause.getMessage(), cause);
      } else {
        LOG.error("RPC Exception:" + cause.getMessage());
      }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        /* If all requests is done and event is triggered, channel will be closed. */
        if (e.state() == IdleState.ALL_IDLE && requests.size() == 0) {
          ctx.close();
          LOG.warn("Idle connection closed successfully :" + ctx.channel().remoteAddress());
        }
      }
    }
  }
}