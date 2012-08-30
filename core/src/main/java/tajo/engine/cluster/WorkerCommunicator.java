package tajo.engine.cluster;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import tajo.engine.MasterInterfaceProtos.*;
import tajo.NConstants;
import tajo.engine.exception.UnknownWorkerException;
import tajo.engine.ipc.AsyncWorkerClientInterface;
import tajo.engine.ipc.AsyncWorkerInterface;
import tajo.rpc.Callback;
import tajo.rpc.NettyRpc;
import tajo.rpc.RemoteException;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkListener;
import tajo.zookeeper.ZkUtil;

import java.net.InetSocketAddress;
import java.util.*;

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
        AsyncWorkerClientInterface proxy = null;
        try {
        proxy = (AsyncWorkerClientInterface) NettyRpc
            .getProtoParamAsyncRpcProxy(AsyncWorkerInterface.class,
                AsyncWorkerClientInterface.class, new InetSocketAddress(
                    extractHost(servername), extractPort(servername)));
        } catch (Exception e) {
          LOG.error("cannot connect to the worker (" + servername + ")");
          try {
            zkClient.delete(ZkUtil.concat(NConstants.ZNODE_LEAFSERVERS, servername));
          } catch (InterruptedException ie) {
            ie.printStackTrace();
          } catch (KeeperException ke) {
            ke.printStackTrace();
          }
        }
        if (proxy != null) {
          hm.put(servername, proxy);
        }
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
      throw new UnknownWorkerException(serverName);
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
      throw new UnknownWorkerException(serverName);
    }
    leaf.getServerStatus(cb, NullProto.newBuilder().build());
    return cb;
  }
  
  public Callback<CommandResponseProto> requestCommand(String serverName, 
      CommandRequestProto request) throws UnknownWorkerException {
    Preconditions.checkArgument(serverName != null);
    Callback<CommandResponseProto> cb = new Callback<CommandResponseProto>();
    AsyncWorkerClientInterface leaf = hm.get(serverName);
    if (leaf == null) {
      throw new UnknownWorkerException(serverName);
    }
    leaf.requestCommand(cb, request);

    return cb;
  }
  
  public Callback<CommandResponseProto> requestCommand(String serverName, 
      int port, CommandRequestProto request) throws UnknownWorkerException {
    return this.requestCommand(serverName + ":" + port, request);
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