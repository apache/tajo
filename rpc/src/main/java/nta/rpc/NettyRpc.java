package nta.rpc;

import java.net.InetSocketAddress;

public class NettyRpc {
  public static WritableRpcServer getWritableAsyncRpcServer(Object instance,
      InetSocketAddress addr) {
    return new WritableRpcServer(instance, addr);
  }

  public static Object getWritableAsyncRpcProxy(Class<?> serverClass,
      Class<?> clientClass, InetSocketAddress addr) {
    return new WritableAsyncRpcProxy(serverClass, clientClass, addr).getProxy();
  }

  public static Object getWritableBlockingRpcProxy(Class<?> protocol,
      InetSocketAddress addr) {
    return new WritableBlockingRpcProxy(protocol, addr).getProxy();
  }
}
