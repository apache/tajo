package nta.rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import nta.rpc.ProtoParamRpcProtos.Invocation;
import nta.rpc.ProtoParamRpcProtos.Response;
import com.google.protobuf.ByteString;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class ProtoParamBlockingRpcProxy extends NettyClientBase {

  private static Log LOG = LogFactory.getLog(ProtoParamBlockingRpcProxy.class);

  private final Class<?> protocol;
  private final ChannelPipelineFactory pipeFactory;
  private final ClientHandler handler;
  private final AtomicInteger sequence = new AtomicInteger(0);
  private Map<Integer, CallFuture> requests =
      new ConcurrentHashMap<Integer, CallFuture>();

  public ProtoParamBlockingRpcProxy(Class<?> protocol, InetSocketAddress addr) {
    this.protocol = protocol;
    this.handler = new ClientHandler();
    this.pipeFactory =
        new ProtoPipelineFactory(handler, Response.getDefaultInstance());
    super.init(addr, pipeFactory);
  }

  public Object getProxy() {
    return Proxy.newProxyInstance(protocol.getClassLoader(),
        new Class[] { protocol }, new Invoker(getChannel()));
  }

  public String getExceptionMessage() {
    return handler.getExceptionMessage();
  }

  private class Invoker implements InvocationHandler {
    private final Channel channel;

    public Invoker(Channel channel) {
      this.channel = channel;
    }

    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {

      int nextSeqId = sequence.incrementAndGet();

      Invocation.Builder builder = Invocation.newBuilder();

      if (args != null) {
        for (int i = 0; i < args.length; i++) {
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

      Invocation request =
          builder.setId(nextSeqId).setMethodName(method.getName()).build();

      CallFuture callFuture = new CallFuture(method.getReturnType());
      requests.put(nextSeqId, callFuture);
      this.channel.write(request);
      Object retObj = callFuture.get();
      String exceptionMessage = handler.getExceptionMessage();

      if (exceptionMessage == "") {
        return retObj;
      } else {
        throw new RemoteException(exceptionMessage);
      }
    }
  }

  private class ClientHandler extends SimpleChannelUpstreamHandler {
    private String exceptionMessage = "";

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {
      Response response = (Response) e.getMessage();
      CallFuture callFuture = requests.get(response.getId());

      Object r = null;
      if (response != null) {
        if (!response.getHasReturn()) {
          if (response.hasExceptionMessage()) {
            this.exceptionMessage = response.getExceptionMessage();
          }
          response = null;
        } else {
          ByteArrayInputStream bais =
              new ByteArrayInputStream(response.getReturnValue().toByteArray());
          ObjectInputStream ois = new ObjectInputStream(bais);
          r = ois.readObject();

        }
      }

      if (callFuture == null) {
        LOG.debug("dangling rpc call");
      } else {
        callFuture.setResponse(r);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      e.getChannel().close();
      LOG.error(e.getCause());
    }

    public String getExceptionMessage() {
      return this.exceptionMessage;
    }
  }

}
