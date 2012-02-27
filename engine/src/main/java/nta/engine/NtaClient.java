package nta.engine;

import java.net.InetSocketAddress;

import nta.engine.ipc.QueryClientInterface;
import nta.rpc.Callback;
import nta.rpc.NettyRpc;
import nta.rpc.RemoteException;

import org.apache.hadoop.conf.Configuration;

public class NtaClient {
  private Configuration conf = null;
  private QueryClientInterface asyncProtocol = null;
  private QueryClientInterface blockingProtocol = null;

  public NtaClient(String ip, int port) {
    this(new Configuration(), ip, port);
  }

  public NtaClient(Configuration conf, String ip, int port) {
    init(conf, ip, port);
  }

  /**
   * NTA 마스터 생성 및 초기화
   * 
   * @param conf
   */
  public void init(Configuration conf, String ip, int port) {
    this.conf = conf;
    InetSocketAddress addr = new InetSocketAddress(ip, port);
    this.blockingProtocol =
        (QueryClientInterface) NettyRpc.getProtoParamBlockingRpcProxy(
            QueryClientInterface.class, addr);
    this.asyncProtocol =
        (QueryClientInterface) NettyRpc.getProtoParamAsyncRpcProxy(
            NtaEngineMaster.class, QueryClientInterface.class, addr);
  }

  /**
   * NTA 마스터 종료
   */
  public void close() {
  }

  public String executeQuery(String query) throws RemoteException {
    return blockingProtocol.executeQuery(query);
  }

  public void executeQueryAsync(Callback<String> callback, String query) {
    asyncProtocol.executeQueryAsync(callback, query);
  }

  public void attachTable(String name, String path) throws RemoteException {
    blockingProtocol.attachTable(name, path);
  }

  public void detachTable(String name) throws RemoteException {
    blockingProtocol.detachTable(name);
  }

  public boolean existsTable(String name) throws RemoteException {
    return blockingProtocol.existsTable(name);
  }
}
