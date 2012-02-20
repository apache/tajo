package nta.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import nta.rpc.ProtoParamRpcProtos.Invocation;
import nta.rpc.ProtoParamRpcProtos.Response;

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

  public class Invoker implements InvocationHandler {
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
          ByteString str = ((Message) args[i]).toByteString();
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

    public void shutdown() {
      LOG.info("[RPC] Client terminates connection "
          + channel.getRemoteAddress());
      this.channel.close().awaitUninterruptibly();
      bootstrap.releaseExternalResources();
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
          @SuppressWarnings("unchecked")
          Method mtd =
              callFuture.getReturnType().getMethod("parseFrom",
                  new Class[] { ByteString.class });
          r = mtd.invoke(null, response.getReturnValue());

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
      LOG.error("[RPC] ERROR " + e.getChannel().getRemoteAddress() + " "
          + e.getCause());
      e.getChannel().close();
    }

    public String getExceptionMessage() {
      return this.exceptionMessage;
    }
  }

}
