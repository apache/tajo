/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.rpc;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.*;
import tajo.rpc.RpcProtos.RpcRequest;
import tajo.rpc.RpcProtos.RpcResponse;
import tajo.util.NetUtils;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Hyunsik Choi
 */
public class ProtoBlockingRpcClient extends NettyClientBase {
  private static final Log LOG = LogFactory.getLog(RpcProtos.class);

  private final ChannelUpstreamHandler handler;
  private final ChannelPipelineFactory pipeFactory;
  private final ProxyRpcChannel rpcChannel;

  private final AtomicInteger sequence = new AtomicInteger(0);
  private final Map<Integer, ProtoCallFuture> requests =
      new ConcurrentHashMap<Integer, ProtoCallFuture>();

  private final Class<?> protocol;
  private final Method stubMethod;

  public ProtoBlockingRpcClient(final Class<?> protocol,
                                final InetSocketAddress addr)
      throws Exception {

    this.protocol = protocol;
    String serviceClassName = protocol.getName() + "$"
        + protocol.getSimpleName() + "Service";
    Class<?> serviceClass = Class.forName(serviceClassName);
    stubMethod = serviceClass.getMethod("newBlockingStub",
        BlockingRpcChannel.class);

    this.handler = new ClientChannelUpstreamHandler();
    pipeFactory = new ProtoPipelineFactory(handler,
        RpcResponse.getDefaultInstance());
    super.init(addr, pipeFactory);
    rpcChannel = new ProxyRpcChannel(getChannel());
  }

  public <T> T getStub() throws Exception {
    return (T) stubMethod.invoke(null, rpcChannel);
  }

  public BlockingRpcChannel getBlockingRpcChannel() {
    return this.rpcChannel;
  }

  private class ProxyRpcChannel implements BlockingRpcChannel {
    private final Channel channel;
    private final ClientChannelUpstreamHandler handler;

    public ProxyRpcChannel(Channel channel) {
      this.channel = channel;
      this.handler = channel.getPipeline().
          get(ClientChannelUpstreamHandler.class);

      if (handler == null) {
        throw new IllegalArgumentException("Channel does not have " +
            "proper handler");
      }
    }

    public Message callBlockingMethod(final MethodDescriptor method,
                                      final RpcController controller,
                                      final Message param,
                                      final Message responsePrototype)
        throws ServiceException {

      int nextSeqId = sequence.getAndIncrement();

      Message rpcRequest = buildRequest(nextSeqId, method, param);

      ProtoCallFuture callFuture =
          new ProtoCallFuture(controller, responsePrototype);
      requests.put(nextSeqId, callFuture);
      channel.write(rpcRequest);

      try {
        return callFuture.get();
      } catch (Throwable t) {
        throw new RemoteException(t);
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
    return "Exception [" + protocol.getCanonicalName() +
        "(" + NetUtils.getIpPortString((InetSocketAddress)
        getChannel().getRemoteAddress()) + ")]: " + message;
  }

  private class ClientChannelUpstreamHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {

      RpcResponse rpcResponse = (RpcResponse) e.getMessage();
      ProtoCallFuture callback = requests.remove(rpcResponse.getId());

      if (callback == null) {
        LOG.warn("Dangling rpc call");
      } else {
        if (rpcResponse.hasErrorMessage()) {
          if (callback.controller != null) {
            callback.setFailed(rpcResponse.getErrorMessage());
          }
          throw new RemoteException(
              getErrorMessage(rpcResponse.getErrorMessage()));
        } else {
          Message responseMessage;

          if (!rpcResponse.hasResponseMessage()) {
            responseMessage = null;
          } else {
            responseMessage =
                callback.returnType.newBuilderForType().
                    mergeFrom(rpcResponse.getResponseMessage()).build();
          }

          callback.setResponse(responseMessage);
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      e.getChannel().close();
      throw new RemoteException(getErrorMessage(""), e.getCause());
    }
  }

  class ProtoCallFuture implements Future<Message> {
    private Semaphore sem = new Semaphore(0);
    private Message response = null;
    private Message returnType;

    private RpcController controller;

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
      return response;
    }

    @Override
    public Message get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if(sem.tryAcquire(timeout, unit)) {
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

    public void setFailed(String errorText) {
      this.controller.setFailed(errorText);
      sem.release();
    }
  }
}