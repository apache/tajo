/**
 * 
 */
package nta.catalog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionDescProto;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.engine.NConstants;
import nta.engine.cluster.ServerNodeTracker;
import nta.rpc.NettyRpc;
import nta.zookeeper.ZkClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

/**
 * CatalogClient provides a client API to access the catalog server.
 * 
 * @author Hyunsik Choi
 * 
 */
public class CatalogClient implements CatalogService {
  private final Log LOG = LogFactory.getLog(CatalogClient.class);
  private static final int waitTime = 5 * 1000;

  private final ZkClient zkClient;
  private ServerNodeTracker tracker;
  private CatalogServiceProtocol proxy;

  /**
   * @throws IOException
   * 
   */
  public CatalogClient(final Configuration conf) throws IOException {
    this.zkClient = new ZkClient(conf);
    init();    
  }
  
  public CatalogClient(final ZkClient zkClient) 
      throws IOException {
    this.zkClient = zkClient;
    init();
  }
  
  private void init() throws IOException {
    this.tracker = new ServerNodeTracker(zkClient, NConstants.ZNODE_CATALOG);
    this.tracker.start();
    String serverName = null;
    try {
      byte[] byteName;
      byteName = tracker.blockUntilAvailable(waitTime);
      if (byteName == null) {
        throw new InternalError("cannot access the catalog service");
      }
      serverName = new String(byteName);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    LOG.info("Trying to connect the catalog (" + serverName + ")");
    InetSocketAddress addr = NetUtils.createSocketAddr(serverName);
    proxy = (CatalogServiceProtocol) NettyRpc.getProtoParamBlockingRpcProxy(
        CatalogServiceProtocol.class, addr);
    LOG.info("Connected to the catalog server (" + serverName + ")");
  }
  
  public void close() {
    this.zkClient.close();
  }

  @Override
  public final TableDesc getTableDesc(final String name) {
    return TableDesc.Factory.create(proxy.getTableDesc(name));
  }

  @Override
  public final Collection<TableDesc> getAllTableDescs() {
    List<TableDesc> list = new ArrayList<TableDesc>();
    Collection<TableDescProto> protos = proxy.getAllTableDescs();
    for (TableDescProto proto : protos) {
      list.add(TableDesc.Factory.create(proto));
    }
    return list;
  }

  @Override
  public final Collection<FunctionDesc> getFunctions() {
    List<FunctionDesc> list = new ArrayList<FunctionDesc>();
    Collection<FunctionDescProto> protos = proxy.getFunctions();
    for (FunctionDescProto proto : protos) {
      list.add(new FunctionDesc(proto));
    }
    return list;
  }

  @Override
  public final void addTable(final TableDesc desc) {
    proxy.addTable((TableDescProto) desc.getProto());
  }

  @Override
  public final void deleteTable(final String name) {
    proxy.deleteTable(name);
  }

  @Override
  public final boolean existsTable(final String tableId) {
    return proxy.existsTable(tableId);
  }

  @Override
  public final void registerFunction(final FunctionDesc funcDesc) {
    proxy.registerFunction(funcDesc.getProto());
  }

  @Override
  public final void unregisterFunction(final String signature,
      DataType...paramTypes) {      
    proxy.unregisterFunction(signature, paramTypes);
  }

  @Override
  public final FunctionDesc getFunction(final String signature,
      DataType...paramTypes) {
    return FunctionDesc.create(proxy.getFunctionMeta(signature,paramTypes));
  }

  @Override
  public final boolean containFunction(final String signature,
      DataType...paramTypes) {
    return proxy.containFunction(signature, paramTypes);
  }

  @Override
  public final List<TabletServInfo> getHostByTable(final String tableId) {
    // TODO - to be implemented
    return null;
  }

  @Override
  public void updateAllTabletServingInfo(final List<String> onlineServers)
      throws IOException {
    // TODO - to be implemented
  }
}