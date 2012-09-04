package tajo.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import tajo.LocalTajoCluster;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableDesc;
import tajo.catalog.TableDescImpl;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.TableDescProto;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.ClientServiceProtos.*;
import tajo.engine.query.ResultSetImpl;
import tajo.engine.utils.ProtoUtil;
import tajo.rpc.NettyRpc;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.List;

/**
 * @author Hyunsik Choi
 */
public class TajoClient {
  private final Log LOG = LogFactory.getLog(TajoClient.class);
  
  private final TajoConf conf;
  private ClientService service;
  private static LocalTajoCluster cluster = null;
  private String resultPath;

  public TajoClient(TajoConf conf) throws IOException {
    this.conf = conf;

    boolean mode = this.conf.getBoolVar(ConfVars.CLUSTER_DISTRIBUTED);
    // If cli is executed in local mode
    if (mode == false) {
      initLocalCluster(conf);
    } else {
      String masterAddr = this.conf.getVar(ConfVars.CLIENT_SERVICE_ADDRESS);
      InetSocketAddress addr = NetUtils.createSocketAddr(masterAddr);
      init(addr);
    }
  }

  public TajoClient(InetSocketAddress addr) throws IOException {
    this.conf = new TajoConf();
    this.conf.setBoolVar(ConfVars.CLUSTER_DISTRIBUTED, true);
    init(addr);
  }

  public TajoClient(String hostname, int port) throws IOException {
    this.conf = new TajoConf();
    this.conf.setBoolVar(ConfVars.CLUSTER_DISTRIBUTED, true);
    init(NetUtils.createSocketAddr(hostname, port));
  }

  private void init(InetSocketAddress addr) throws IOException {
    service =
        (ClientService) NettyRpc.getProtoParamBlockingRpcProxy(
            ClientService.class, addr);
    LOG.info("Connected to tajo client service (" + addr.getHostName() + ": " + addr.getPort() +")");
  }

  private void initLocalCluster(TajoConf conf) throws IOException {
    try {
      if (cluster == null) {
        cluster = new LocalTajoCluster(conf);
        cluster.startup();
      }
      service = cluster.getMaster();
    } catch (Exception e) {
      LOG.error(e);
      throw new IOException(e);
    }
  }
  
  public boolean isConnected() {
    return service != null;
  }
  
  public ResultSet executeQuery(String tql) throws IOException {
    // TODO - to be implemented in NTA-647
    ExecuteQueryRequest.Builder builder = ExecuteQueryRequest.newBuilder();
    builder.setQuery(tql);
    ExecuteQueryRespose response = service.executeQuery(builder.build());
    ResultSet resultSet = new ResultSetImpl(conf, response.getPath());
    LOG.info(">>>> Output path: " + response.getPath());
    LOG.info(">>>> Response time: " + response.getResponseTime());
    this.resultPath = response.getPath();
    return resultSet;
  }
  
  public String getResultPath() {
    return resultPath;
  }
  
  public void updateQuery(String tql) {
    ExecuteQueryRequest.Builder builder = ExecuteQueryRequest.newBuilder();
    builder.setQuery(tql);
    service.executeQuery(builder.build());
  }
  
  public boolean existTable(String name) {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(name);
    BoolProto res = service.existTable(builder.build());
    return res.getValue();
  }
  
  public TableDesc attachTable(String name, String path) {
    AttachTableRequest.Builder builder = AttachTableRequest.newBuilder();
    builder.setName(name);
    builder.setPath(path);
    AttachTableResponse res = service.attachTable(builder.build());    
    return new TableDescImpl(res.getDesc());
  }
  
  public void detachTable(String name) {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(name);
    service.detachTable(builder.build());
  }
  
  public TableDesc createTable(String name, Path path, TableMeta meta) {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setName(name);
    builder.setPath(path.toString());
    builder.setMeta(meta.getProto());
    CreateTableResponse res = service.createTable(builder.build());    
    return new TableDescImpl(res.getDesc());
  }
  
  public void dropTable(String name) {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(name);
    service.dropTable(builder.build());
  }
  
  public List<String> getClusterInfo() {
    GetClusterInfoResponse res = service.getClusterInfo(GetClusterInfoRequest.getDefaultInstance());
    return res.getServerNameList();
  }
  
  public List<String> getTableList() {
    GetTableListResponse res = service.getTableList(GetTableListRequest.getDefaultInstance());
    return res.getTablesList();
  }
  
  public TableDesc getTableDesc(String tableName) {    
    TableDescProto res = service.getTableDesc(ProtoUtil.newProto(tableName));
    return TCatUtil.newTableDesc(res);
  }
}