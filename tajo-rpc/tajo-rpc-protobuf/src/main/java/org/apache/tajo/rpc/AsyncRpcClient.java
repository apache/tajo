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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.RpcClientManager.RpcConnectionKey;
import org.apache.tajo.rpc.RpcProtos.RpcRequest;
import org.apache.tajo.rpc.RpcProtos.RpcResponse;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AsyncRpcClient extends NettyClientBase {
  private static final Log LOG = LogFactory.getLog(AsyncRpcClient.class);

  private final ConcurrentMap<Integer, ResponseCallback> requests =
      new ConcurrentHashMap<Integer, ResponseCallback>();

  private final Method stubMethod;
  private final ProxyRpcChannel rpcChannel;
  private final ClientChannelInboundHandler inboundHandler;
  private final boolean enablePing;

  /**
   * Intentionally make this method package-private, avoiding user directly
   * new an instance through this constructor.
   */
  AsyncRpcClient(RpcConnectionKey rpcConnectionKey, int retries)
      throws ClassNotFoundException, NoSuchMethodException {
    this(rpcConnectionKey, retries, 0, false);
  }

  /**
   *
   * @param rpcConnectionKey
   * @param retries retry operation number of times
   * @param idleTimeoutSeconds  connection timeout seconds
   * @param enablePing enable network inactive detecting
   * @throws ClassNotFoundException
   * @throws NoSuchMethodException
   */
  AsyncRpcClient(RpcConnectionKey rpcConnectionKey, int retries, int idleTimeoutSeconds, boolean enablePing)
      throws ClassNotFoundException, NoSuchMethodException {
    super(rpcConnectionKey, retries);
    this.stubMethod = getServiceClass().getMethod("newStub", RpcChannel.class);
    this.rpcChannel = new ProxyRpcChannel();
    this.inboundHandler = new ClientChannelInboundHandler();
    this.enablePing = enablePing;
    init(new ProtoClientChannelInitializer(inboundHandler,
        RpcResponse.getDefaultInstance(),
        idleTimeoutSeconds,
        enablePing));
  }

  @Override
  public <T> T getStub() {
    return getStub(stubMethod, rpcChannel);
  }

  @Override
  public int getActiveRequests() {
    return requests.size();
  }

  private void sendExceptions(String message) {
    synchronized (this) {
      for (int requestId : requests.keySet()) {
        sendException(requestId, message);
      }
    }
  }

  private void sendException(RecoverableException e) {
    sendException(e.getSeqId(), ExceptionUtils.getRootCauseMessage(e));
  }

  private void sendException(int requestId, String message) {
    ResponseCallback callback = requests.remove(requestId);
    if (callback != null) {
      RpcResponse.Builder responseBuilder = RpcResponse.newBuilder()
          .setErrorMessage(message + "")
          .setId(requestId);

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

      invoke(rpcRequest, nextSeqId, 0);
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

  static class ResponseCallback implements RpcCallback<RpcResponse> {
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
      // it can be called the callback function with null response.
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
            getErrorMessage("Duplicate Sequence Id " + seqId));
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
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
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
      LOG.info("Connection established successfully : " + ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {

      if (LOG.isDebugEnabled()) {
        LOG.error(getRemoteAddress() + "," + protocol + "," + ExceptionUtils.getRootCauseMessage(cause), cause);
      } else {
        LOG.error(getRemoteAddress() + "," + protocol + "," + ExceptionUtils.getRootCauseMessage(cause));
      }

      if (cause instanceof RecoverableException) {
        sendException((RecoverableException) cause);
      } else {
        /* unrecoverable fatal error*/
        sendExceptions(cause.getMessage());
        if(ctx.channel().isOpen()) {
          ctx.close();
        }
      }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

      if (!enablePing && evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        /* If all requests is done and event is triggered, idle channel will be closed. */
        if (e.state() == IdleState.READER_IDLE && getActiveRequests() == 0) {
          ctx.close();
          LOG.info("Idle connection closed successfully :" + ctx.channel());
        }
      } else if (evt instanceof MonitorStateEvent) {
        MonitorStateEvent e = (MonitorStateEvent) evt;
        if (e.state() == MonitorStateEvent.MonitorState.PING_EXPIRED) {
          exceptionCaught(ctx, new ServiceException("Server has not respond: " + ctx.channel()));
        }
      }
    }
  }
}