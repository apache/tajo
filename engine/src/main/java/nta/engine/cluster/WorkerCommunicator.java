package nta.engine.cluster;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;

import nta.catalog.proto.CatalogProtos.TabletProto;
import nta.engine.NConstants;
import nta.engine.LeafServerProtos.AssignTabletRequestProto;
import nta.engine.LeafServerProtos.SubQueryRequestProto;
import nta.engine.LeafServerProtos.SubQueryResponseProto;
import nta.engine.QueryUnitProtos.QueryUnitRequestProto;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto;
import nta.engine.ipc.AsyncWorkerClientInterface;
import nta.engine.ipc.AsyncWorkerInterface;
import nta.rpc.Callback;
import nta.rpc.NettyRpc;
import nta.rpc.RemoteException;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkListener;
import nta.zookeeper.ZkUtil;

public class WorkerCommunicator extends ZkListener {
  private final Log LOG = LogFactory.getLog(LeafServerTracker.class);

  private ZkClient zkClient;
  private LeafServerTracker tracker;

  private List<String> servers;
  private HashMap<String, AsyncWorkerClientInterface> hm = new HashMap<String, AsyncWorkerClientInterface>();

  public WorkerCommunicator(Configuration conf) throws Exception {

    zkClient = new ZkClient(conf);
    tracker = new LeafServerTracker(zkClient);

    servers = tracker.getMembers();

    for (String servername : servers) {
      AsyncWorkerClientInterface leaf;

      leaf = (AsyncWorkerClientInterface) NettyRpc.getProtoParamAsyncRpcProxy(
          AsyncWorkerInterface.class, AsyncWorkerClientInterface.class,
          new InetSocketAddress(extractHost(servername),
              extractPort(servername)));
      hm.put(servername, leaf);
    }
  }

  public void start() throws Exception {
    this.zkClient.subscribe(this);
    ZkUtil.listChildrenAndWatchThem(zkClient, NConstants.ZNODE_LEAFSERVERS);
  }

  private String extractHost(String servername) {
    return servername.substring(0, servername.indexOf(":"));
  }

  private int extractPort(String servername) {
    return Integer.parseInt(servername.substring(servername.indexOf(":") + 1));
  }

  public Callback<SubQueryResponseProto> requestSubQuery(String serverName,
      SubQueryRequestProto requestProto) throws Exception {
    Callback<SubQueryResponseProto> cb;
    cb = new Callback<SubQueryResponseProto>();
    AsyncWorkerClientInterface leaf = hm.get(serverName);
    leaf.requestSubQuery(cb, requestProto);
    return cb;
  }

  public Callback<SubQueryResponseProto> requestSubQuery(String serverName,
      int port, SubQueryRequestProto requestProto) throws Exception {
    return requestSubQuery(serverName + ":" + port, requestProto);
  }

  public Callback<SubQueryResponseProto> requestQueryUnit(String serverName,
      QueryUnitRequestProto requestProto) throws Exception {
    Callback<SubQueryResponseProto> cb;
    cb = new Callback<SubQueryResponseProto>();
    AsyncWorkerClientInterface leaf = hm.get(serverName);
    leaf.requestQueryUnit(cb, requestProto);
    return cb;
  }

  public Callback<SubQueryResponseProto> requestQueryUnit(String serverName,
      int port, QueryUnitRequestProto requestProto) throws Exception {
    return requestQueryUnit(serverName + ":" + port, requestProto);
  }

  public void assignTablets(String serverName, TabletProto tablet) {
    AsyncWorkerClientInterface leaf = hm.get(serverName);

    AssignTabletRequestProto tabletRequest = AssignTabletRequestProto
        .newBuilder().setTablets(tablet).build();

    leaf.assignTablets(tabletRequest);
  }

  public Callback<ServerStatusProto> getServerStatus(String serverName)
      throws RemoteException, InterruptedException {
    Callback<ServerStatusProto> cb;
    cb = new Callback<ServerStatusProto>();
    AsyncWorkerClientInterface leaf = hm.get(serverName);
    leaf.getServerStatus(cb);
    return cb;
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(NConstants.ZNODE_LEAFSERVERS)) {
      try {
        ZkUtil.listChildrenAndWatchThem(zkClient, NConstants.ZNODE_LEAFSERVERS);

        List<String> servers = tracker.getMembers();

        if (hm.size() > servers.size()) {
          LOG.info("Leafserver stopped: delete proxy");
          Iterator<String> iterator = hm.keySet().iterator();
          while (iterator.hasNext()) {
            String key = (String) iterator.next();
            if (!servers.contains(key)) {
              hm.remove(key);
              break;
            }
          }
        } else if (hm.size() < servers.size()) {
          LOG.info("Leafserver added: open proxy");
          for (String servername : servers) {
            if (hm.get(servername) == null) {
              AsyncWorkerClientInterface leaf;

              leaf = (AsyncWorkerClientInterface) NettyRpc
                  .getProtoParamAsyncRpcProxy(AsyncWorkerInterface.class,
                      AsyncWorkerClientInterface.class, new InetSocketAddress(
                          extractHost(servername), extractPort(servername)));
              hm.put(servername, leaf);
              break;
            }
          }
        } else {
          LOG.error("Unexpected watching!");
        }
      } catch (KeeperException e) {
        LOG.error(e.getMessage(), e);
      }
    }

  }

  public HashMap<String, AsyncWorkerClientInterface> getProxyMap() {
    return hm;
  }

  public void close() {
    zkClient.close();
  }
}