package nta.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ConnectException;
import java.net.Socket;

public class NettyRpc {

  public static ProtoParamRpcServer getProtoParamRpcServer(Object instance,
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
    
    return new ProtoParamRpcServer(instance, newAddress);
  }

  public static Object getProtoParamAsyncRpcProxy(Class<?> serverClass,
      Class<?> clientClass, InetSocketAddress addr) {
    return new ProtoParamAsyncRpcProxy(serverClass, clientClass, addr)
        .getProxy();
  }

  public static Object getProtoParamBlockingRpcProxy(Class<?> protocol,
      InetSocketAddress addr) {
    return new ProtoParamBlockingRpcProxy(protocol, addr).getProxy();
  }
  
  public static int getUnusedPort(String hostname) throws IOException {
    while (true) {
        int port = (int) (45536 * Math.random() + 10000);
        try {
            Socket s = new Socket(hostname, port);
            s.close();
        } catch (ConnectException e) {
            return port;
        } catch (IOException e) {
            if (e.getMessage().contains("refused"))
                return port;
            throw e;
        }
    }
  }
}
