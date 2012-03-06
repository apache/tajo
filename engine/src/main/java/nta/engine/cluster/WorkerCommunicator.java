package nta.engine.cluster;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nta.engine.MasterInterfaceProtos.QueryUnitRequestProto;
import nta.engine.MasterInterfaceProtos.ServerStatusProto;
import nta.engine.MasterInterfaceProtos.SubQueryResponseProto;
import nta.engine.NConstants;
import nta.engine.exception.UnknownWorkerException;
import nta.engine.ipc.AsyncWorkerClientInterface;
import nta.engine.ipc.AsyncWorkerInterface;
import nta.rpc.Callback;
import nta.rpc.NettyRpc;
import nta.rpc.RemoteException;
import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkListener;
import nta.zookeeper.ZkUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.MapMaker;

public class WorkerCommunicator extends ZkListener {
  private final Log LOG = LogFactory.getLog(LeafServerTracker.class);

  private ZkClient zkClient;
  private LeafServerTracker tracker;

  private Map<String, AsyncWorkerClientInterface> hm =
      new MapMaker().concurrencyLevel(4).makeMap();

  public WorkerCommunicator(ZkClient zkClient, LeafServerTracker tracker)
      throws Exception {

    this.zkClient = zkClient;
    this.tracker = tracker;
  }

  public void start() throws Exception {
    this.zkClient.subscribe(this);
    ZkUtil.listChildrenAndWatchThem(zkClient, NConstants.ZNODE_LEAFSERVERS);
    update(this.tracker.getMembers());
  }
  
  private void update(final List<String> servers) {
    if (hm.size() > servers.size()) {
      HashSet<String> serverset = new HashSet<String>();
      serverset.addAll(servers);
      removeUpdate(serverset);
    } else if (hm.size() < servers.size()) {
      addUpdate(servers);
    }
  }

  private void removeUpdate(final Set<String> servers) {
    Iterator<String> iterator = hm.keySet().iterator();
    while (iterator.hasNext()) {
      String key = (String) iterator.next();
      if (!servers.contains(key)) {
        hm.remove(key);
      }
    }
  }

  private void addUpdate(final List<String> servers) {
    for (String servername : servers) {
      if (!hm.containsKey(servername)) {
        hm.put(servername, 
            (AsyncWorkerClientInterface) NettyRpc
            .getProtoParamAsyncRpcProxy(AsyncWorkerInterface.class,
                AsyncWorkerClientInterface.class, new InetSocketAddress(
                    extractHost(servername), extractPort(servername))));
      }
    }
  }

  private String extractHost(String servername) {
    return servername.substring(0, servername.indexOf(":"));
  }

  private int extractPort(String servername) {
    return Integer.parseInt(servername.substring(servername.indexOf(":") + 1));
  }

  public Callback<SubQueryResponseProto> requestQueryUnit(String serverName,
      QueryUnitRequestProto requestProto) throws Exception {
    Callback<SubQueryResponseProto> cb = new Callback<SubQueryResponseProto>();
    AsyncWorkerClientInterface leaf = hm.get(serverName);
    if (leaf == null) {
      throw new UnknownWorkerException("server name: " + serverName);
    }
    leaf.requestQueryUnit(cb, requestProto);
    return cb;
  }

  public Callback<SubQueryResponseProto> requestQueryUnit(String serverName,
      int port, QueryUnitRequestProto requestProto) throws Exception {
    return this.requestQueryUnit(serverName + ":" + port, requestProto);
  }

  public Callback<ServerStatusProto> getServerStatus(String serverName)
      throws RemoteException, InterruptedException, UnknownWorkerException {
    Callback<ServerStatusProto> cb = new Callback<ServerStatusProto>();
    AsyncWorkerClientInterface leaf = hm.get(serverName);
    if (leaf == null) {
      throw new UnknownWorkerException("server name: " + serverName);
    }
    leaf.getServerStatus(cb, NullProto.newBuilder().build());
    return cb;
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(NConstants.ZNODE_LEAFSERVERS)) {
      try {
        ZkUtil.listChildrenAndWatchThem(zkClient, NConstants.ZNODE_LEAFSERVERS);
        update(tracker.getMembers());
      } catch (KeeperException e) {
        LOG.error(e.getMessage(), e);
      }
    }

  }

  public Map<String, AsyncWorkerClientInterface> getProxyMap() {
    return hm;
  }

  public void close() {
    this.zkClient.unsubscribe(this);
    this.zkClient.close();
  }
}