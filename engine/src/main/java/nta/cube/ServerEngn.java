package nta.cube;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;

public class ServerEngn {

  public static Cuboid big_cuboid;
  private static int fNode = 0;

  public ServerEngn() throws IOException {
    big_cuboid = new Cuboid();
  }

  public static void getWriteFinNode(int nodenum) {
    // System.out.println("writefinnode : " + nodenum);
    fNode++;
  }

  public void run(CubeConf conf) throws IOException, InterruptedException {

    ProtoParamRpcServer server = NettyRpc.getProtoParamRpcServer(
        new CubeRpc.Server(), new InetSocketAddress(10011));

    server.start();

    LinkedList<KVpair> mapped_list;

    /* 1. initialize variables */
    SummaryTable summarytable;

    summarytable = new SummaryTable();
    mapped_list = new LinkedList<KVpair>();

    /* 2. wait for local generated cuboid */
    while (fNode < Cons.totalnodes) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
      }
    }
    server.shutdown();

    /* 3. groupby execute */
    Groupby g = new Groupby();
    g.execute2(conf, Scan.globalscan(conf), mapped_list);

    for (KVpair tuple : mapped_list) {
      summarytable.summary_table.add(tuple);
      summarytable.count += tuple.count;
    }
    big_cuboid = new Cuboid();
    big_cuboid.cuboid.add(summarytable);

    /* 4. write to hdfs */
    Write.write(conf, big_cuboid, conf.getGlobalOutput());

//    System.out.println("WriteFin");
    
  }
}
