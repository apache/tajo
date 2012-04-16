package tajo.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.List;

import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.engine.ClientServiceProtos.AttachTableRequest;
import nta.engine.ClientServiceProtos.AttachTableResponse;
import nta.engine.ClientServiceProtos.CreateTableRequest;
import nta.engine.ClientServiceProtos.CreateTableResponse;
import nta.engine.ClientServiceProtos.ExecuteQueryRequest;
import nta.engine.ClientServiceProtos.ExecuteQueryRespose;
import nta.engine.ClientServiceProtos.GetClusterInfoRequest;
import nta.engine.ClientServiceProtos.GetClusterInfoResponse;
import nta.engine.ClientServiceProtos.GetTableListRequest;
import nta.engine.ClientServiceProtos.GetTableListResponse;
import nta.engine.NConstants;
import nta.engine.query.ResultSetImpl;
import nta.engine.utils.ProtoUtil;
import nta.rpc.NettyRpc;
import nta.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import nta.rpc.protocolrecords.PrimitiveProtos.StringProto;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;

/**
 * @author Hyunsik Choi
 */
public class TajoClient {
  private final Log LOG = LogFactory.getLog(TajoClient.class);
  
  private final Configuration conf;
  private final ClientService service;
  
  public TajoClient(Configuration conf) {
    this.conf = conf;
    
    String masterAddr = this.conf.get(NConstants.CLIENT_SERVICE_ADDRESS, 
        NConstants.DEFAULT_CLIENT_SERVICE_ADDRESS);
    InetSocketAddress addr = NetUtils.createSocketAddr(masterAddr);
    
    service =
        (ClientService) NettyRpc.getProtoParamBlockingRpcProxy(
            ClientService.class, addr);
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
    return resultSet;
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