package tajo.rpc;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class NettyRpc {

  public static NettyRpcServer getProtoParamRpcServer(Object instance,
      Class<?> interfaceClass, InetSocketAddress addr) {

    InetSocketAddress newAddress = null;

    if (addr.getPort() == 0) {
      try {
        int port = getUnusedPort(addr.getHostName());
        newAddress = new InetSocketAddress(addr.getHostName(), port);
      } catch (IOException e) {
        e.printStackTrace();
      }

    } else {
      newAddress = addr;
    }

    return new NettyRpcServer(instance, interfaceClass, newAddress);
  }

  @Deprecated
  public static NettyRpcServer getProtoParamRpcServer(Object instance,
      InetSocketAddress addr) {

    InetSocketAddress newAddress = null;

    if (addr.getPort() == 0) {
      try {
        int port = getUnusedPort(addr.getHostName());
        newAddress = new InetSocketAddress(addr.getHostName(), port);
      } catch (IOException e) {
        e.printStackTrace();
      }

    } else {
      newAddress = addr;
    }

    return new NettyRpcServer(instance, newAddress);
  }

  public static Object getProtoParamAsyncRpcProxy(Class<?> serverClass,
      Class<?> clientClass, InetSocketAddress addr) {
    return new NettyAsyncRpcProxy(serverClass, clientClass, addr)
        .getProxy();
  }

  public static Object getProtoParamBlockingRpcProxy(Class<?> protocol,
      InetSocketAddress addr) {
    return new NettyBlockingRpcProxy(protocol, addr).getProxy();
  }

  public static int getUnusedPort(String hostname) throws IOException {
    while (true) {
      int port = (int) (10000 * Math.random() + 10000);
      if (available(port)) {
        return port;
      }
    }
  }

  public static boolean available(int port) {
    if (port < 10000 || port > 20000) {
      throw new IllegalArgumentException("Invalid start port: " + port);
    }

    ServerSocket ss = null;
    DatagramSocket ds = null;
    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);
      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException e) {
    } finally {
      if (ds != null) {
        ds.close();
      }

      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          /* should not be thrown */
        }
      }
    }

    return false;
  }
}
