package nta.rpc.benchmark;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.ipc.RPC.Server;

public class BenchmarkHadoopRPC {

  public static class ClientWrapper extends Thread {
    @SuppressWarnings("unused")
    public void run() {
      InetSocketAddress addr = new InetSocketAddress("localhost", 15000);
      BenchmarkInterface proxy = null;
      try {
        proxy =
            (BenchmarkInterface) RPC.waitForProxy(BenchmarkInterface.class, 1,
                addr, new Configuration());
      } catch (IOException e1) {
        e1.printStackTrace();
      }

      long start = System.currentTimeMillis();
      for (int i = 0; i < 100000; i++) {
        String response = proxy.shoot("ABCD");
      }
      long end = System.currentTimeMillis();
      System.out.println("elapsed time: " + (end - start) + "msc");
    }
  }

  public static interface BenchmarkInterface extends VersionedProtocol {
    public String shoot(String l);
  }

  public static class BenchmarkImpl implements BenchmarkInterface {
    @Override
    public String shoot(String l) {
      return l;
    }

    @Override
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
      return 1l;
    }
  }

  public static void main(String[] args) throws InterruptedException,
      IOException {
    Server server =
        RPC.getServer(new BenchmarkImpl(), "localhost", 15000,
            new Configuration());
    server.start();
    Thread.sleep(100);

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

    server.stop();
  }
}
