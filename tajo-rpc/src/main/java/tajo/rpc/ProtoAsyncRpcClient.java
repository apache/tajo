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

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.*;
import tajo.rpc.RpcProtos.RpcRequest;
import tajo.rpc.RpcProtos.RpcResponse;
import tajo.util.NetUtils;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Hyunsik Choi
 */
public class ProtoAsyncRpcClient extends NettyClientBase {
  private static final Log LOG = LogFactory.getLog(RpcProtos.class);

  private final ChannelUpstreamHandler handler;
  private final ChannelPipelineFactory pipeFactory;
  private final ProxyRpcChannel rpcChannel;

  private final AtomicInteger sequence = new AtomicInteger(0);
  private final Map<Integer, ResponseCallback> requests =
      new ConcurrentHashMap<Integer, ResponseCallback>();

  private final Class<?> protocol;
  private final Method stubMethod;

  public ProtoAsyncRpcClient(final Class<?> protocol,
                             final InetSocketAddress addr)
      throws Exception {

    this.protocol = protocol;
    String serviceClassName = protocol.getName() + "$"
        + protocol.getSimpleName() + "Service";
    Class<?> serviceClass = Class.forName(serviceClassName);
    stubMethod = serviceClass.getMethod("newStub", RpcChannel.class);

    this.handler = new ClientChannelUpstreamHandler();
    pipeFactory = new ProtoPipelineFactory(handler,
        RpcResponse.getDefaultInstance());
    super.init(addr, pipeFactory);
    rpcChannel = new ProxyRpcChannel(getChannel());
  }

  public <T> T getStub() throws Exception {
    return (T) stubMethod.invoke(null, rpcChannel);
  }

  public RpcChannel getRpcChannel() {
    return this.rpcChannel;
  }

  private class ProxyRpcChannel implements RpcChannel {
    private final Channel channel;
    private final ClientChannelUpstreamHandler handler;

    public ProxyRpcChannel(Channel channel) {
      this.channel = channel;
      this.handler = channel.getPipeline()
          .get(ClientChannelUpstreamHandler.class);

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
      channel.write(rpcRequest);
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

    public void run(RpcResponse rpcResponse) {

      // if hasErrorMessage is true, it means rpc-level errors.
      // it does not call the callback function
      if (rpcResponse.hasErrorMessage()) {

        if (controller != null) {
          this.controller.setFailed(rpcResponse.getErrorMessage());
        }
        callback.run(null);
        throw new RemoteException(getErrorMessage(rpcResponse.getErrorMessage()));

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
        "(" + NetUtils.getIpPortString((InetSocketAddress)
        getChannel().getRemoteAddress()) + ")]: " + message;
  }

  private class ClientChannelUpstreamHandler extends SimpleChannelUpstreamHandler {

    synchronized void registerCallback(int seqId, ResponseCallback callback) {

      if (requests.containsKey(seqId)) {
        throw new RemoteException(
            getErrorMessage("Duplicate Sequence Id "+ seqId));
      }

      requests.put(seqId, callback);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {

      RpcResponse response = (RpcResponse) e.getMessage();
      ResponseCallback callback = requests.remove(response.getId());

      if (callback == null) {
        LOG.warn("Dangling rpc call");
      } else {
        callback.run(response);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      e.getChannel().close();
      throw new RemoteException(getErrorMessage(""), e.getCause());
    }
  }
}