package tajo.rpc.benchmark;

import tajo.rpc.Callback;
import tajo.rpc.NettyRpc;
import tajo.rpc.ProtoParamRpcServer;
import tajo.rpc.RemoteException;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;

import java.net.InetSocketAddress;

public class BenchmarkAsyncRPC {

  public static class ClientWrapper extends Thread {
    public void run() {
      BenchmarkClientInterface service = null;
      service =
          (BenchmarkClientInterface) NettyRpc.getProtoParamAsyncRpcProxy(
              BenchmarkServerInterface.class, BenchmarkClientInterface.class,
              new InetSocketAddress(15010));

      long start = System.currentTimeMillis();
      Callback<StringProto> cb = new Callback<StringProto>();
      StringProto ps = StringProto.newBuilder().setValue("ABCD").build();
      for (int i = 0; i < 100000; i++) {
        service.shoot(cb, ps);
      }
      long end = System.currentTimeMillis();

      System.out.println("elapsed time: " + (end - start) + "msc");
    }
  }

  public static interface BenchmarkClientInterface {
    public void shoot(Callback<StringProto> ret, StringProto l)
        throws RemoteException;
  }

  public static interface BenchmarkServerInterface {
    public StringProto shoot(StringProto l) throws RemoteException;
  }

  public static class BenchmarkImpl implements BenchmarkServerInterface {
    @Override
    public StringProto shoot(StringProto l) {
      return l;
    }
  }

  public static void main(String[] args) throws Exception {
    ProtoParamRpcServer rpcServer =
        NettyRpc.getProtoParamRpcServer(new BenchmarkImpl(),
            BenchmarkServerInterface.class, new InetSocketAddress(15010));
    rpcServer.start();
    Thread.sleep(1000);

    int numThreads = 1;
    ClientWrapper client[] = new ClientWrapper[numThreads];
    for (int i = 0; i < numThreads; i++) {
      client[i] = new ClientWrapper();
    }

    for (int i = 0; i < numThreads; i++) {
      client[i].start();
    }

    for (int i = 0; i < numThreads; i++) {
      client[i].join();
    }

    rpcServer.shutdown();
    System.exit(0);
  }
}
