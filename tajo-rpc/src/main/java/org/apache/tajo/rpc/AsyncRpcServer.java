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
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.RpcProtos.RpcRequest;
import org.apache.tajo.rpc.RpcProtos.RpcResponse;
import org.jboss.netty.channel.*;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;

public class AsyncRpcServer extends NettyServerBase {
  private static final Log LOG = LogFactory.getLog(AsyncRpcServer.class);

  private final Service service;
  private final ChannelPipelineFactory pipeline;

  public AsyncRpcServer(final Class<?> protocol,
                        final Object instance,
                        final InetSocketAddress bindAddress,
                        final int workerNum)
      throws Exception {
    super(protocol.getSimpleName(), bindAddress);

    String serviceClassName = protocol.getName() + "$" +
        protocol.getSimpleName() + "Service";
    Class<?> serviceClass = Class.forName(serviceClassName);
    Class<?> interfaceClass = Class.forName(serviceClassName + "$Interface");
    Method method = serviceClass.getMethod("newReflectiveService", interfaceClass);
    this.service = (Service) method.invoke(null, instance);

    ServerHandler handler = new ServerHandler();
    this.pipeline = new ProtoPipelineFactory(handler,
        RpcRequest.getDefaultInstance());
    super.init(this.pipeline, workerNum);
  }

  private class ServerHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent evt)
        throws Exception {

      accepted.add(evt.getChannel());
      if(LOG.isDebugEnabled()){
        LOG.debug(String.format(serviceName + " accepted number of connections (%d)", accepted.size()));
      }
      super.channelOpen(ctx, evt);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {

      final RpcRequest request = (RpcRequest) e.getMessage();

      String methodName = request.getMethodName();
      MethodDescriptor methodDescriptor = service.getDescriptorForType().
          findMethodByName(methodName);

      if (methodDescriptor == null) {
        throw new RemoteCallException(request.getId(),
            new NoSuchMethodException(methodName));
      }

      Message paramProto = null;
      if (request.hasRequestMessage()) {
        try {
          paramProto = service.getRequestPrototype(methodDescriptor)
                  .newBuilderForType().mergeFrom(request.getRequestMessage()).
                  build();
        } catch (Throwable t) {
          throw new RemoteCallException(request.getId(), methodDescriptor, t);
        }
      }

      final Channel channel = e.getChannel();
      final RpcController controller = new NettyRpcController();

      RpcCallback<Message> callback =
          !request.hasId() ? null : new RpcCallback<Message>() {

        public void run(Message returnValue) {

          RpcResponse.Builder builder = RpcResponse.newBuilder()
              .setId(request.getId());

          if (returnValue != null) {
            builder.setResponseMessage(returnValue.toByteString());
          }

          if (controller.failed()) {
            builder.setErrorMessage(controller.errorText());
          }

          channel.write(builder.build());
        }
      };

      service.callMethod(methodDescriptor, controller, paramProto, callback);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception{

      if (e.getCause() instanceof RemoteCallException) {
        RemoteCallException callException = (RemoteCallException) e.getCause();
        e.getChannel().write(callException.getResponse());
      } else {
        LOG.error(e.getCause());
      }
    }
  }
}