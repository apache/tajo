package nta.cube;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;

import nta.cube.CubeRpc.ClientInterface;
import nta.cube.CubeRpc.ServerInterface;
import nta.rpc.Callback;
import nta.rpc.NettyRpc;

/* local groupby execute */
public class LocalEngn {
  Cuboid cuboid = null;

  public void run(CubeConf conf) throws IOException, InterruptedException,
      ExecutionException {

    LinkedList<KVpair> mapped_list;
    SummaryTable tablelist = null;

    /* 1. initialize */
    mapped_list = new LinkedList<KVpair>();

    /* 2. groupby execute */
    Groupby g = new Groupby();
    g.execute(conf, Scan.localscan(conf), mapped_list);

    /* 3. create instances for cuboid */
    tablelist = new SummaryTable();
    cuboid = new Cuboid();
    cuboid.cuboid = new LinkedList<SummaryTable>();

    /* 4. make summary table */
    for (KVpair tuple : mapped_list) {
      tablelist.summary_table.add(tuple);
      tablelist.count += tuple.count;
    }

    /* 5. make cuboid */
    cuboid.cuboid.add(tablelist);
    Write.write(conf, cuboid, Cons.immediatepath);

    ClientInterface proxy = (ClientInterface) NettyRpc
        .getProtoParamAsyncRpcProxy(ServerInterface.class,
            ClientInterface.class, new InetSocketAddress(10011));
    Callback<Integer> cb = new Callback<Integer>();
    proxy.send(cb, conf.getNodenum());
  }
}
