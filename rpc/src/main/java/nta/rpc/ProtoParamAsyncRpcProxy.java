package nta.rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import nta.rpc.ProtoParamRpcProtos.Invocation;
import nta.rpc.ProtoParamRpcProtos.Response;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

public class ProtoParamAsyncRpcProxy extends NettyClientBase {
  private static Log LOG = LogFactory.getLog(ProtoParamAsyncRpcProxy.class);

  private final Class<?> protocol;
  private final ClientHandler handler;
  InetSocketAddress addr;

  private final AtomicInteger sequence = new AtomicInteger(0);
  private final Map<Integer, ResponseRpcCallback> requests =
      new ConcurrentHashMap<Integer, ResponseRpcCallback>();

  @SuppressWarnings("rawtypes")
  private final Map<String, Class> returnTypeMap = new HashMap<String, Class>();

  public ProtoParamAsyncRpcProxy(Class<?> server, Class<?> client,
      InetSocketAddress addr) {
    this.protocol = client;

    this.handler = new ClientHandler();
    ChannelPipelineFactory pipeFactory =
        new ProtoPipelineFactory(handler, Response.getDefaultInstance());

    for (Method method : server.getMethods()) {
      returnTypeMap.put(method.getName(), method.getReturnType());
    }

    super.init(addr, pipeFactory);
  }

  public Object getProxy() {
    return Proxy.newProxyInstance(protocol.getClassLoader(),
        new Class[] { protocol }, new Invoker(getChannel()));
  }

  private class Invoker implements InvocationHandler {
    private final Channel channel;

    public Invoker(Channel channel) {
      this.channel = channel;
    }

    @SuppressWarnings("rawtypes")
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {

      int seqId = sequence.incrementAndGet();
      Invocation.Builder builder = Invocation.newBuilder();

      if (args != null) {
        for (int i = 1; i < args.length; i++) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream(baos);
          oos.writeObject(args[i]);
          oos.flush();
          oos.close();
          baos.close();
          ByteString str = ByteString.copyFrom(baos.toByteArray());
          builder.addParam(str);
        }
      }
      Callback userCallBack = (Callback) args[0];
      ResponseRpcCallback rpcCallback =
          new ResponseRpcCallback(returnTypeMap.get(method.getName()),
              userCallBack);

      Invocation request =
          builder.setId(seqId).setMethodName(method.getName()).build();
      requests.put(seqId, rpcCallback);
      this.channel.write(request);
      return null;
    }
  }

  private class ResponseRpcCallback implements RpcCallback<Response> {
    @SuppressWarnings("rawtypes")
    private final Callback callback;
    private Response response;

    @SuppressWarnings("rawtypes")
    public ResponseRpcCallback(Class clazz, Callback callback) {
      this.callback = callback;
    }

    @SuppressWarnings("unchecked")
    public void run(Response message) {
      response = message;

      Object retObj = null;
      if (response != null) {
        if (!response.getHasReturn()) {
          if (response.hasExceptionMessage()) {
            callback.onFailure(new RemoteException(response
                .getExceptionMessage()));
            return;
          }
          retObj = null;
        } else {
          try {
            ByteArrayInputStream bais =
                new ByteArrayInputStream(response.getReturnValue()
                    .toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            retObj = ois.readObject();
          } catch (Exception e) {
            e.printStackTrace();
          }

        }
      }

      callback.onComplete(retObj);
    }
  }

  private class ClientHandler extends SimpleChannelUpstreamHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {
      Response response = (Response) e.getMessage();
      ResponseRpcCallback callback = requests.remove(response.getId());

      if (callback == null) {
        LOG.debug("dangling rpc call");
      } else {
        callback.run(response);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      LOG.error(e.getCause());
      e.getChannel().close();
    }
  }
}