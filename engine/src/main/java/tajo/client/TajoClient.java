package tajo.client;

import java.net.InetSocketAddress;
import java.sql.ResultSet;

import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.engine.ClientServiceProtos.AttachTableRequest;
import nta.engine.ClientServiceProtos.AttachTableResponse;
import nta.engine.ClientServiceProtos.CreateTableRequest;
import nta.engine.ClientServiceProtos.CreateTableResponse;
import nta.engine.ClientServiceProtos.QuerySubmitRequest;
import nta.engine.NConstants;
import nta.rpc.NettyRpc;
import nta.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import nta.rpc.protocolrecords.PrimitiveProtos.StringProto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;

/**
 * @author Hyunsik Choi
 */
public class TajoClient {
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
  
  public ResultSet executeQuery(String tql) {
    // TODO - to be implemented in NTA-647
    return null;
  }
  
  public void updateQuery(String tql) {
    QuerySubmitRequest.Builder builder = QuerySubmitRequest.newBuilder();
    builder.setQuery(tql);
    service.submitQuery(builder.build());
  }
  
  public boolean existTable(String name) {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(name);
    BoolProto res = service.existTable(builder.build());
    return res.getValue();
  }
  
  public TableDesc attachTable(String name, Path path) {
    AttachTableRequest.Builder builder = AttachTableRequest.newBuilder();
    builder.setName(name);
    builder.setPath(path.toString());
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
}