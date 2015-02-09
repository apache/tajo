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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.RpcProtos.RpcRequest;
import org.apache.tajo.rpc.RpcProtos.RpcResponse;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.GenericFutureListener;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.rpc.RpcConnectionPool.RpcConnectionKey;

public class AsyncRpcClient extends NettyClientBase {
  private static final Log LOG = LogFactory.getLog(AsyncRpcClient.class);

  private final ChannelInboundHandler handler;
  private final ChannelInitializer<Channel> initializer;
  private final ProxyRpcChannel rpcChannel;

  private final AtomicInteger sequence = new AtomicInteger(0);
  private final Map<Integer, ResponseCallback> requests =
      new ConcurrentHashMap<Integer, ResponseCallback>();

  private final Class<?> protocol;
  private final Method stubMethod;

  private RpcConnectionKey key;

  /**
   * Intentionally make this method package-private, avoiding user directly
   * new an instance through this constructor.
   */
  AsyncRpcClient(final Class<?> protocol,
                        final InetSocketAddress addr, int retries)
      throws ClassNotFoundException, NoSuchMethodException, ConnectTimeoutException {

    this.protocol = protocol;
    String serviceClassName = protocol.getName() + "$"
        + protocol.getSimpleName() + "Service";
    Class<?> serviceClass = Class.forName(serviceClassName);
    stubMethod = serviceClass.getMethod("newStub", RpcChannel.class);

    this.handler = new ClientChannelInboundHandler();
    initializer = new ProtoChannelInitializer(handler,
        RpcResponse.getDefaultInstance());
    super.init(addr, initializer, retries);
    rpcChannel = new ProxyRpcChannel();
    this.key = new RpcConnectionKey(addr, protocol, true);
  }

  @Override
  public RpcConnectionKey getKey() {
    return key;
  }

  @Override
  public <T> T getStub() {
    try {
      return (T) stubMethod.invoke(null, rpcChannel);
    } catch (Exception e) {
      throw new RemoteException(e.getMessage(), e);
    }
  }

  public RpcChannel getRpcChannel() {
    return this.rpcChannel;
  }

  private class ProxyRpcChannel implements RpcChannel {
    private final ClientChannelInboundHandler handler;

    public ProxyRpcChannel() {
      this.handler = getChannel().pipeline()
          .get(ClientChannelInboundHandler.class);

      if (handler == null) {
        throw new IllegalArgumentException("Channel does not have " +
            "proper handler");
      }
    }

    public void callMethod(final MethodDescriptor method,
                           final RpcController controller,
                           final Message param,
                           final Message responseType,
                           RpcCallback<Message> done) {

      int nextSeqId = sequence.getAndIncrement();

      Message rpcRequest = buildRequest(nextSeqId, method, param);

      handler.registerCallback(nextSeqId,
          new ResponseCallback(controller, responseType, done));

      ChannelPromise channelPromise = getChannel().newPromise();
      channelPromise.addListener(new GenericFutureListener<ChannelFuture>() {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (!future.isSuccess()) {
            handler.exceptionCaught(null, new ServiceException(future.cause()));
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

  @Sharable
  private class ClientChannelInboundHandler extends ChannelInboundHandlerAdapter {

    synchronized void registerCallback(int seqId, ResponseCallback callback) {

      if (requests.containsKey(seqId)) {
        throw new RemoteException(
            getErrorMessage("Duplicate Sequence Id "+ seqId));
      }

      requests.put(seqId, callback);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
      try {
        if (msg instanceof RpcResponse) {
          RpcResponse response = (RpcResponse) msg;
          ResponseCallback callback = requests.remove(response.getId());

          if (callback == null) {
            LOG.warn("Dangling rpc call");
          } else {
            callback.run(response);
          }
        }
      } finally {
        ReferenceCountUtil.release(msg);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      LOG.error(getRemoteAddress() + "," + protocol + "," + cause.getMessage(), cause);
      
      for(Map.Entry<Integer, ResponseCallback> callbackEntry: requests.entrySet()) {
        ResponseCallback callback = callbackEntry.getValue();
        Integer id = callbackEntry.getKey();

        RpcResponse.Builder responseBuilder = RpcResponse.newBuilder()
            .setErrorMessage(cause.getMessage())
            .setId(id);
        
        callback.run(responseBuilder.build());
      }
      
      if(LOG.isDebugEnabled()) {
        LOG.error(cause.getMessage(), cause);
      } else {
        LOG.error("RPC Exception:" + cause.getMessage());
      }
      
      if (ctx != null) {
        ctx.close();
      }
    }
  }
}