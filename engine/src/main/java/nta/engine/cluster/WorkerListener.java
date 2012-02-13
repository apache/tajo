/**
 * 
 */
package nta.engine.cluster;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;

import nta.engine.NConstants;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitProtos.QueryUnitReportProto;
import nta.engine.ipc.AsyncMasterInterface;
import nta.engine.ipc.QueryUnitReport;
import nta.engine.query.QueryUnitReportImpl;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;

/**
 * @author jihoon
 *
 */
public class WorkerListener implements Runnable, AsyncMasterInterface {
  
  private Map<QueryUnitId, Float> progressMap;
  
  private final ProtoParamRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private volatile boolean stopped = false;
  
  public WorkerListener(Configuration conf, Map<QueryUnitId, Float> progressMap) {
    String confMasterAddr = conf.get(NConstants.MASTER_ADDRESS,
        NConstants.DEFAULT_MASTER_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterAddr);
    if (initIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initIsa);
    }
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this, initIsa);
    this.progressMap = progressMap;
  }
  
  public InetSocketAddress getBindAddress() {
    return this.bindAddr;
  }
  
  public String getAddress() {
    return this.addr;
  }
  
  public boolean isStopped() {
    return this.stopped;
  }
  
  public void start() {
    this.stopped = false;
    this.rpcServer.start();
    this.bindAddr = rpcServer.getBindAddress();
    this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();
  }
  
  public void stop() {
    this.rpcServer.shutdown();
    this.stopped = true;
  }

  /* (non-Javadoc)
   * @see nta.engine.ipc.AsyncMasterInterface#reportQueryUnit(nta.engine.QueryUnitProtos.QueryUnitReportProto)
   */
  @Override
  public void reportQueryUnit(QueryUnitReportProto proto) {
    QueryUnitReport report = new QueryUnitReportImpl(proto);
    progressMap.put(report.getId(), report.getProgress());
  }

  @Override
  public void run() {
    // rpc listen
    try {
      while (!this.stopped) {
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
