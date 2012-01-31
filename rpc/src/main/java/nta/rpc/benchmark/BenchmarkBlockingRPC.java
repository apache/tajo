package nta.rpc.benchmark;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;
import nta.rpc.RemoteException;

public class BenchmarkBlockingRPC {

  public static class ClientWrapper extends Thread {
    @SuppressWarnings("unused")
    public void run() {
      InetSocketAddress addr = new InetSocketAddress("localhost", 15001);
      BenchmarkInterface proxy = null;
      proxy =
          (BenchmarkInterface) NettyRpc.getProtoParamBlockingRpcProxy(
              BenchmarkInterface.class, addr);

      long start = System.currentTimeMillis();
      for (int i = 0; i < 10000; i++) {
        try {
          long response = proxy.shoot(System.currentTimeMillis());
        } catch (RemoteException e1) {
          System.out.println(e1.getMessage());
        }
      }
      long end = System.currentTimeMillis();
      System.out.println("Total Count: " + proxy.getCount());
      System.out.println("elapsed time: " + (end - start) + "msc");
      
    }
  }

  public static interface BenchmarkInterface {
    public long shoot(long l) throws RemoteException;

    public int getCount() throws RemoteException;
  }

  public static class BenchmarkImpl implements BenchmarkInterface {
    AtomicInteger count = new AtomicInteger();

    @Override
    public long shoot(long l) {
      count.addAndGet(1);
      return System.currentTimeMillis();
    }

    public int getCount() {
      return count.get();
    }
  }

  public static void main(String[] args) throws InterruptedException,
      RemoteException {

    ProtoParamRpcServer server =
        NettyRpc.getProtoParamRpcServer(new BenchmarkImpl(),
            new InetSocketAddress("localhost", 15001));

    server.start();
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

    server.shutdown();
    System.exit(0);
  }
}
