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

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.MethodDescriptor;
import io.netty.channel.*;
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
import java.util.Map;
import java.util.concurrent.*;

public class BlockingRpcClient extends NettyClientBase {
  private static final Log LOG = LogFactory.getLog(RpcProtos.class);

  private final Map<Integer, ProtoCallFuture> requests =
      new ConcurrentHashMap<Integer, ProtoCallFuture>();

  private final Method stubMethod;
  private final ProxyRpcChannel rpcChannel;
  private final ChannelInboundHandlerAdapter inboundHandler;
  private final boolean enablePing;

  /**
   * Intentionally make this method package-private, avoiding user directly
   * new an instance through this constructor.
   */
  BlockingRpcClient(RpcConnectionKey rpcConnectionKey, int retries)
      throws NoSuchMethodException, ClassNotFoundException {
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
  BlockingRpcClient(RpcConnectionKey rpcConnectionKey, int retries, int idleTimeoutSeconds, boolean enablePing)
      throws ClassNotFoundException, NoSuchMethodException {
    super(rpcConnectionKey, retries);
    this.stubMethod = getServiceClass().getMethod("newBlockingStub", BlockingRpcChannel.class);
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
    ProtoCallFuture callback = requests.remove(requestId);
    if (callback != null) {
      callback.setFailed(message+"", new TajoServiceException(message));
    }
  }

  @Override
  public void close() {
    sendExceptions("BlockingRpcClient terminates all the connections");
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

      invoke(rpcRequest, nextSeqId, 0);

      try {
        return callFuture.get();
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
        sendExceptions(ExceptionUtils.getRootCauseMessage(cause));
        if(ctx.channel().isOpen()) {
          ctx.close();
        }
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
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

      if (!enablePing && evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        /* If all requests is done and event is triggered, channel will be closed. */
        if (e.state() == IdleState.READER_IDLE && getActiveRequests() == 0) {
          ctx.close();
          LOG.info("Idle connection closed successfully :" + ctx.channel().remoteAddress());
        }
      } else if (evt instanceof MonitorStateEvent) {
        MonitorStateEvent e = (MonitorStateEvent) evt;
        if (e.state() == MonitorStateEvent.MonitorState.PING_EXPIRED) {
          exceptionCaught(ctx, new ServiceException("Server has not respond: " + ctx.channel()));
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