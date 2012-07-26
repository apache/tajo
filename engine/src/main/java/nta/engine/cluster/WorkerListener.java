/**
 * 
 */
package nta.engine.cluster;

import java.net.InetSocketAddress;

import nta.engine.MasterInterfaceProtos.InProgressStatusProto;
import nta.engine.MasterInterfaceProtos.PingRequestProto;
import nta.engine.MasterInterfaceProtos.PingResponseProto;
import nta.engine.NConstants;
import nta.engine.QueryUnitId;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.ipc.MasterInterface;
import nta.engine.ipc.PingRequest;
import nta.engine.query.PingRequestImpl;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

/**
 * @author jihoon
 *
 */
public class WorkerListener implements Runnable, MasterInterface {
  
  private final static Log LOG = LogFactory.getLog(WorkerListener.class);
  private final ProtoParamRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private volatile boolean stopped = false;
  private QueryManager qm;
  
  public WorkerListener(Configuration conf, QueryManager qm) {
    String confMasterAddr = conf.get(NConstants.MASTER_ADDRESS,
        NConstants.DEFAULT_MASTER_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterAddr);
    if (initIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initIsa);
    }
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this, 
        MasterInterface.class, initIsa);
    this.qm = qm;
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
  public PingResponseProto reportQueryUnit(PingRequestProto proto) {
//    LOG.info("master received reports from " + proto.getServerName() +
//        ": " + proto.getStatusCount());

    PingRequest report = new PingRequestImpl(proto);
    for (InProgressStatusProto status : report.getProgressList()) {
      QueryUnitId uid = new QueryUnitId(status.getId());
//      LOG.info(uid + " will be changed to " + status.getStatus());
      try {
        qm.updateProgress(uid, status);
      } catch (NoSuchQueryIdException e) {
        e.printStackTrace();
      }
    }
    PingResponseProto.Builder response 
      = PingResponseProto.newBuilder();
    return response.build();
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
