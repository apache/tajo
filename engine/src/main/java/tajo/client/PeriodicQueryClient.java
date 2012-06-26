package tajo.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.List;

import nta.conf.NtaConf;
import nta.engine.query.ResultSetImpl;
import nta.rpc.NettyRpc;
import tajo.client.PeriodicQueryProtos.*;
import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;

public class PeriodicQueryClient {
  private PeriodicQueryService service;
  
  public PeriodicQueryClient(){
    InetSocketAddress addr = new InetSocketAddress(
        PeriodicQueryDaemon.LOCALHOST , PeriodicQueryDaemon.PORT);
    service =
        (PeriodicQueryService) NettyRpc.getProtoParamBlockingRpcProxy(
            PeriodicQueryService.class, addr);
  }
  
  public boolean registerNewPeriodicQuery(String query, String content, long period){
    QueryStatusProto.Builder builder = QueryStatusProto.newBuilder();
    builder.setQuery(query);
    builder.setPeriod(period);
    builder.setContent(content);
    return service.registerNewPeriodicQuery(builder.build()).getFinished();
  }
  
  public boolean regiAndExeNewPeriodicQuery(String query, long period, String content) {
    QueryStatusProto.Builder builder = QueryStatusProto.newBuilder();
    builder.setQuery(query);
    builder.setPeriod(period);
    builder.setContent(content);
    return service.regiAndexeNewPeriodicQuery(builder.build()).getFinished();
  }
  
  public boolean executePeriodicQuery(String query) {
    ChooseQueryRequest.Builder builder = ChooseQueryRequest.newBuilder();
    builder.setQuery(query);
    return service.executePeriodicQuery(builder.build()).getFinished();
  }
  
  public boolean cancelPeriodicQuery(String query) {
    ChooseQueryRequest.Builder builder = ChooseQueryRequest.newBuilder();
    builder.setQuery(query);
    return service.executePeriodicQuery(builder.build()).getFinished();
  }
  
  public List<QueryStatusProto> getQueryList() {
    QueryListResponse response = service.getQueryList(
        NullProto.newBuilder().build());
    return response.getQueryList();
  }
  
  public void executeAll(){
    service.executeAllPeriodicQuery(
        NullProto.newBuilder().build());
  }
  
  public void cancelAll() {
    service.cancelAllPeirodicQuery(
        NullProto.newBuilder().build());
  }
  
  public ResultSet getQueryResult(String query) throws IOException {
    String path = service.getQueryResultPath(
        ChooseQueryRequest.newBuilder().setQuery(query).build()).getPath();
    if(path.equals("null")) {
      return null;
    }
    return new ResultSetImpl(new NtaConf(), path);
  }
}
