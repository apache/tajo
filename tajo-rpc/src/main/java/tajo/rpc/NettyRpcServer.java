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

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.*;
import tajo.rpc.ProtoParamRpcProtos.Invocation;
import tajo.rpc.ProtoParamRpcProtos.Response;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class NettyRpcServer extends NettyServerBase {
  private static Log LOG = LogFactory.getLog(NettyRpcServer.class);
  private final Object instance;
  private final Class<?> clazz;
  private final ChannelPipelineFactory pipeline;
  private Map<String, Method> methods;
  private Map<String, Method> builderMethods;

  @Deprecated
  public NettyRpcServer(Object proxy, InetSocketAddress bindAddress) {
    super(bindAddress);
    this.instance = proxy;
    this.clazz = instance.getClass();
    this.methods = new HashMap<String, Method>();
    this.builderMethods = new HashMap<String, Method>();
    this.pipeline =
        new ProtoPipelineFactory(new ServerHandler(),
            Invocation.getDefaultInstance());

    super.init(this.pipeline);
    for (Method m : this.clazz.getDeclaredMethods()) {
      String methodName = m.getName();
      Class<?> params[] = m.getParameterTypes();
      try {
        methods.put(methodName, m);

        Method mtd = params[0].getMethod("newBuilder", new Class[] {});
        builderMethods.put(methodName, mtd);
      } catch (Exception e) {
        e.printStackTrace();
        continue;
      }
    }
  }

  public NettyRpcServer(Object proxy, Class<?> interfaceClass,
                        InetSocketAddress bindAddress) {
    super(bindAddress);
    this.instance = proxy;
    this.clazz = instance.getClass();
    this.methods = new HashMap<String, Method>();
    this.builderMethods = new HashMap<String, Method>();
    this.pipeline =
        new ProtoPipelineFactory(new ServerHandler(),
            Invocation.getDefaultInstance());

    super.init(this.pipeline);
    for (Method m : interfaceClass.getDeclaredMethods()) {
      String methodName = m.getName();
      Class<?> params[] = m.getParameterTypes();
      try {
        methods.put(methodName, this.clazz.getMethod(methodName, params));

        Method mtd = params[0].getMethod("newBuilder", new Class[] {});
        builderMethods.put(methodName, mtd);
      } catch (Exception e) {
        e.printStackTrace();
        continue;
      }
    }
  }

  private class ServerHandler extends SimpleChannelUpstreamHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {
      final Invocation request = (Invocation) e.getMessage();
      Object res = null;
      Response response = null;

      try {
        String methodName = request.getMethodName();
        if (methods.containsKey(methodName) == false) {
          throw new NoSuchMethodException(methodName);
        }

        Method method = methods.get(methodName);
        Method builderGenMethod = builderMethods.get(methodName);
        Builder builder =
            (Builder) builderGenMethod.invoke(null, new Object[] {});
        Message msg = builder.mergeFrom(request.getParam(0)).build();
        res = method.invoke(instance, msg);

      } catch (InvocationTargetException internalException) {
        LOG.error(ExceptionUtils.getStackTrace(internalException
            .getTargetException()));
        response =
            Response
                .newBuilder()
                .setId(request.getId())
                .setHasReturn(false)
                .setExceptionMessage(
                    internalException.getTargetException().toString()).build();
        e.getChannel().write(response);
        return;
      } catch (Exception otherException) {
        otherException.printStackTrace();
        response =
            Response.newBuilder().setId(request.getId()).setHasReturn(false)
                .setExceptionMessage(otherException.toString()).build();
        e.getChannel().write(response);
        return;
      }

      if (res == null) {
        response =
            Response.newBuilder().setId(request.getId()).setHasReturn(false)
                .build();
      } else {
        ByteString str = ((Message) res).toByteString();
        response =
            Response.newBuilder().setId(request.getId()).setHasReturn(true)
                .setReturnValue(str).build();
      }
      e.getChannel().write(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      e.getChannel().close();
      LOG.error(e.getCause());
    }
  }
}
