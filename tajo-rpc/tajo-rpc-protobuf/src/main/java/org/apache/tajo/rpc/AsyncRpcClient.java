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
import org.apache.tajo.rpc.RpcClientManager.RpcConnectionKey;
import org.apache.tajo.rpc.RpcProtos.RpcResponse;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncRpcClient extends NettyClientBase<AsyncRpcClient.ResponseCallback> {

  private final Method stubMethod;
  private final ProxyRpcChannel rpcChannel;
  private final NettyChannelInboundHandler handler;

  AsyncRpcClient(RpcConnectionKey rpcConnectionKey, int retries)
      throws ClassNotFoundException, NoSuchMethodException {
    this(rpcConnectionKey, retries, 0, TimeUnit.NANOSECONDS, false);
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
   * @throws ClassNotFoundException
   * @throws NoSuchMethodException
   */
  AsyncRpcClient(RpcConnectionKey rpcConnectionKey, int retries, long timeout, TimeUnit timeUnit, boolean enablePing)
      throws ClassNotFoundException, NoSuchMethodException {
    super(rpcConnectionKey, retries);

    this.stubMethod = getServiceClass().getMethod("newStub", RpcChannel.class);
    this.rpcChannel = new ProxyRpcChannel();
    this.handler = new ClientChannelInboundHandler();
    init(new ProtoClientChannelInitializer(handler,
        RpcResponse.getDefaultInstance(),
        timeUnit.toNanos(timeout),
        enablePing));
  }

  @Override
  public <I> I getStub() {
    return getStub(stubMethod, rpcChannel);
  }

  @Override
  protected NettyChannelInboundHandler getHandler() {
    return handler;
  }

  private class ProxyRpcChannel implements RpcChannel {

    private final AtomicInteger sequence = new AtomicInteger(0);

    public void callMethod(final MethodDescriptor method,
                           final RpcController controller,
                           final Message param,
                           final Message responseType,
                           final RpcCallback<Message> done) {

      int nextSeqId = sequence.getAndIncrement();
      RpcProtos.RpcRequest rpcRequest = buildRequest(nextSeqId, method, param);

      invoke(rpcRequest, new ResponseCallback(controller, responseType, done), 0);
    }
  }

  @ChannelHandler.Sharable
  private class ClientChannelInboundHandler extends NettyChannelInboundHandler {

    @Override
    protected void run(RpcResponse response, ResponseCallback callback) throws Exception {
      callback.run(response);
    }

    @Override
    protected void handleException(int requestId, ResponseCallback callback, String message) {
      RpcResponse.Builder responseBuilder = RpcResponse.newBuilder()
          .setErrorMessage(message + "")
          .setId(requestId);

      callback.run(responseBuilder.build());
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
}