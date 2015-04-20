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

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
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
import java.util.concurrent.*;

public class BlockingRpcClient extends NettyClientBase {
  private static final Log LOG = LogFactory.getLog(RpcProtos.class);

  private final Map<Integer, ProtoCallFuture> requests =
      new ConcurrentHashMap<Integer, ProtoCallFuture>();

  private final Method stubMethod;
  private final ProxyRpcChannel rpcChannel;
  private final ChannelInboundHandlerAdapter inboundHandler;

  /**
   * Intentionally make this method package-private, avoiding user directly
   * new an instance through this constructor.
   */
  BlockingRpcClient(RpcConnectionKey rpcConnectionKey, int retries)
      throws NoSuchMethodException, ClassNotFoundException {
    this(rpcConnectionKey, retries, 0);
  }

  BlockingRpcClient(RpcConnectionKey rpcConnectionKey, int retries, int idleTimeSeconds)
      throws ClassNotFoundException, NoSuchMethodException {
    super(rpcConnectionKey, retries);
    stubMethod = getServiceClass().getMethod("newBlockingStub", BlockingRpcChannel.class);
    rpcChannel = new ProxyRpcChannel();
    inboundHandler = new ClientChannelInboundHandler();
    init(new ProtoChannelInitializer(inboundHandler, RpcResponse.getDefaultInstance(), idleTimeSeconds));
  }

  @Override
  public <T> T getStub() {
    return getStub(stubMethod, rpcChannel);
  }

  @Override
  public void close() {
    for(ProtoCallFuture callback: requests.values()) {
      callback.setFailed("BlockingRpcClient terminates all the connections",
          new ServiceException("BlockingRpcClient terminates all the connections"));
    }
    requests.clear();
    super.close();
  }

  private class ProxyRpcChannel implements BlockingRpcChannel {

    @Override
    public Message callBlockingMethod(final MethodDescriptor method,
                                      final RpcController controller,
                                      final Message param,
                                      final Message responsePrototype)
        throws TajoServiceException {

      int nextSeqId = sequence.getAndIncrement();

      Message rpcRequest = buildRequest(nextSeqId, method, param);

      ProtoCallFuture callFuture =
          new ProtoCallFuture(controller, responsePrototype);
      requests.put(nextSeqId, callFuture);

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

      try {
        return callFuture.get(60, TimeUnit.SECONDS);
      } catch (Throwable t) {
        if (t instanceof ExecutionException) {
          Throwable cause = t.getCause();
          if (cause != null && cause instanceof TajoServiceException) {
            throw (TajoServiceException)cause;
          }
        }
        throw new TajoServiceException(t.getMessage());
      }
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

  private String getErrorMessage(String message) {
    if(getChannel() != null) {
      return protocol.getName() +
          "(" + RpcUtils.normalizeInetSocketAddress((InetSocketAddress)
          getChannel().remoteAddress()) + "): " + message;
    } else {
      return "Exception " + message;
    }
  }

  private TajoServiceException makeTajoServiceException(RpcResponse response, Throwable cause) {
    if(getChannel() != null) {
      return new TajoServiceException(response.getErrorMessage(), cause, protocol.getName(),
          RpcUtils.normalizeInetSocketAddress((InetSocketAddress)getChannel().remoteAddress()));
    } else {
      return new TajoServiceException(response.getErrorMessage());
    }
  }

  @ChannelHandler.Sharable
  private class ClientChannelInboundHandler extends SimpleChannelInboundHandler<RpcResponse> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse rpcResponse) throws Exception {
      ProtoCallFuture callback = requests.remove(rpcResponse.getId());

      if (callback == null) {
        LOG.warn("Dangling rpc call");
      } else {
        if (rpcResponse.hasErrorMessage()) {
          callback.setFailed(rpcResponse.getErrorMessage(),
              makeTajoServiceException(rpcResponse, new ServiceException(rpcResponse.getErrorTrace())));
        } else {
          Message responseMessage;

          if (!rpcResponse.hasResponseMessage()) {
            responseMessage = null;
          } else {
            responseMessage = callback.returnType.newBuilderForType().mergeFrom(rpcResponse.getResponseMessage())
                .build();
          }

          callback.setResponse(responseMessage);
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      /* Current requests will be failed */
      for(ProtoCallFuture callback: requests.values()) {
        callback.setFailed(cause.getMessage(), cause);
      }
      requests.clear();

      if(LOG.isDebugEnabled()) {
        LOG.error("" + cause.getMessage(), cause);
      } else {
        LOG.error("RPC Exception:" + cause.getMessage());
      }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      LOG.info("Connection established successfully : " + ctx.channel().remoteAddress());
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

 static class ProtoCallFuture implements Future<Message> {
    private Semaphore sem = new Semaphore(0);
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
      sem.acquire();
      if(ee != null) {
        throw ee;
      }
      return response;
    }

    @Override
    public Message get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if(sem.tryAcquire(timeout, unit)) {
        if (ee != null) {
          throw ee;
        }
        return response;
      } else {
        throw new TimeoutException();
      }
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return sem.availablePermits() > 0;
    }

    public void setResponse(Message response) {
      this.response = response;
      sem.release();
    }

    public void setFailed(String errorText, Throwable t) {
      if(controller != null) {
        this.controller.setFailed(errorText);
      }
      ee = new ExecutionException(errorText, t);
      sem.release();
    }
  }
}