/**
 * 
 */
package tajo.catalog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import tajo.catalog.proto.CatalogProtos.*;
import tajo.conf.TajoConf;
import tajo.NConstants;
import tajo.engine.cluster.ServerNodeTracker;
import tajo.rpc.NettyRpc;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import tajo.zookeeper.ZkClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
  public CatalogClient(final TajoConf conf) throws IOException {
    this.zkClient = new ZkClient(conf);
    init();
  }

  public CatalogClient(final ZkClient zkClient) throws IOException {
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
    proxy =
        (CatalogServiceProtocol) NettyRpc.getProtoParamBlockingRpcProxy(
            CatalogServiceProtocol.class, addr);
    LOG.info("Connected to the catalog server (" + serverName + ")");
  }

  public void close() {
    this.zkClient.close();
  }

  @Override
  public final TableDesc getTableDesc(final String name) {
    return TCatUtil.newTableDesc(proxy.getTableDesc(StringProto.newBuilder()
        .setValue(name).build()));
  }

  @Override
  public final Collection<String> getAllTableNames() {
    List<String> protos = new ArrayList<String>();
    GetAllTableNamesResponse response =
        proxy.getAllTableNames(NullProto.newBuilder().build());
    int size = response.getTableNameCount();
    for (int i = 0; i < size; i++) {
      protos.add(response.getTableName(i));
    }
    return protos;
  }

  @Override
  public final Collection<FunctionDesc> getFunctions() {
    List<FunctionDesc> list = new ArrayList<FunctionDesc>();
    GetFunctionsResponse response =
        proxy.getFunctions(NullProto.newBuilder().build());
    int size = response.getFunctionDescCount();
    for (int i = 0; i < size; i++) {
      list.add(new FunctionDesc(response.getFunctionDesc(i)));
    }
    return list;
  }

  @Override
  public final void addTable(final TableDesc desc) {
    proxy.addTable((TableDescProto) desc.getProto());
  }

  @Override
  public final void deleteTable(final String name) {
    proxy.deleteTable(StringProto.newBuilder().setValue(name).build());
  }

  @Override
  public final boolean existsTable(final String tableId) {
    return proxy
        .existsTable(StringProto.newBuilder().setValue(tableId).build())
        .getValue();
  }

  @Override
  public void addIndex(IndexDesc index) {
    proxy.addIndex(index.getProto());
  }

  @Override
  public boolean existIndex(String indexName) {
    return proxy.existIndex(StringProto.newBuilder().
        setValue(indexName).build()).getValue();
  }

  @Override
  public boolean existIndex(String tableName, String columnName) {
    GetIndexRequest.Builder builder = GetIndexRequest.newBuilder();
    builder.setTableName(tableName);
    builder.setColumnName(columnName);
    return proxy.existIndex(builder.build()).getValue();
  }

  @Override
  public IndexDesc getIndex(String indexName) {
    return new IndexDesc(
        proxy.getIndex(StringProto.newBuilder().setValue(indexName).build()));
  }

  @Override
  public IndexDesc getIndex(String tableName, String columnName) {
    GetIndexRequest.Builder builder = GetIndexRequest.newBuilder();
    builder.setTableName(tableName);
    builder.setColumnName(columnName);
    return new IndexDesc(proxy.getIndex(builder.build()));
  }

  @Override
  public void deleteIndex(String indexName) {
    // TODO Auto-generated method stub

  }

  @Override
  public final void registerFunction(final FunctionDesc funcDesc) {
    proxy.registerFunction(funcDesc.getProto());
  }

  @Override
  public final void unregisterFunction(final String signature,
      DataType... paramTypes) {
    UnregisterFunctionRequest.Builder builder =
        UnregisterFunctionRequest.newBuilder();
    builder.setSignature(signature);
    int size = paramTypes.length;
    for (int i = 0; i < size; i++) {
      builder.addParameterTypes(paramTypes[i]);
    }
    proxy.unregisterFunction(builder.build());
  }

  @Override
  public final FunctionDesc getFunction(final String signature,
      DataType... paramTypes) {
    GetFunctionMetaRequest.Builder builder =
        GetFunctionMetaRequest.newBuilder();
    builder.setSignature(signature);
    int size = paramTypes.length;
    for (int i = 0; i < size; i++) {
      builder.addParameterTypes(paramTypes[i]);
    }
    return new FunctionDesc(proxy.getFunctionMeta(builder.build()));
  }

  @Override
  public final boolean containFunction(final String signature,
      DataType... paramTypes) {
    ContainFunctionRequest.Builder builder =
        ContainFunctionRequest.newBuilder();
    builder.setSignature(signature);
    int size = paramTypes.length;
    for (int i = 0; i < size; i++) {
      builder.addParameterTypes(paramTypes[i]);
    }
    return proxy.containFunction(builder.build()).getValue();
  }

}