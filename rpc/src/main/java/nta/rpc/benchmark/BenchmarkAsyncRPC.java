package nta.rpc.benchmark;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import nta.rpc.Callback;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;
import nta.rpc.RemoteException;

public class BenchmarkAsyncRPC {

  public static class ClientWrapper extends Thread {
    public void run() {
      BenchmarkClientInterface service = null;
      service =
          (BenchmarkClientInterface) NettyRpc.getProtoParamAsyncRpcProxy(
              BenchmarkServerInterface.class, BenchmarkClientInterface.class,
              new InetSocketAddress(15010));

      long start = System.currentTimeMillis();
      Callback<Long> cb = new Callback<Long>();
      for (int i = 0; i < 10000; i++) {
        service.shoot(cb, System.currentTimeMillis());
      }
      long end = System.currentTimeMillis();
      Callback<Integer> cbCount = new Callback<Integer>();
      service.getCount(cbCount);

      System.out.println("elapsed time: " + (end - start) + "msc");
    }
  }

  public static interface BenchmarkClientInterface {
    public void shoot(Callback<Long> ret, long l) throws RemoteException;

    public void getCount(Callback<Integer> ret) throws RemoteException;
  }

  public static interface BenchmarkServerInterface {
    public Long shoot(long l) throws RemoteException;

    public Integer getCount() throws RemoteException;
  }

  public static class BenchmarkImpl implements BenchmarkServerInterface {
    AtomicInteger count = new AtomicInteger();

    @Override
    public Long shoot(long l) {
      count.addAndGet(1);
      return System.currentTimeMillis();
    }

    public Integer getCount() {
      return count.get();
    }
  }

  public static void main(String[] args) throws Exception {
    ProtoParamRpcServer rpcServer =
        NettyRpc.getProtoParamRpcServer(new BenchmarkImpl(),
            new InetSocketAddress(15010));
    rpcServer.start();
    Thread.sleep(1000);

    int numThreads = 5;
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
