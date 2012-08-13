/**
 * 
 */
package nta.engine.cluster;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import nta.common.Sleeper;
import nta.engine.MasterInterfaceProtos.InProgressStatusProto;
import nta.engine.MasterInterfaceProtos.PingRequestProto;
import nta.engine.MasterInterfaceProtos.PingResponseProto;
import nta.engine.NConstants;
import nta.engine.NtaEngineMaster;
import nta.engine.QueryUnitAttemptId;
import nta.engine.QueryUnitId;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.ipc.MasterInterface;
import nta.engine.ipc.PingRequest;
import nta.engine.planner.global.QueryUnitAttempt;
import nta.engine.query.PingRequestImpl;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

/**
 * @author jihoon
 *
 */
public class WorkerListener extends Thread implements MasterInterface {
  
  private final static Log LOG = LogFactory.getLog(WorkerListener.class);
  private final ProtoParamRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private volatile boolean stopped = false;
  private QueryManager qm;
  private NtaEngineMaster master;
  private AtomicInteger processed;
  private Sleeper sleeper;
  
  public WorkerListener(Configuration conf, QueryManager qm, NtaEngineMaster master) {
    String confMasterAddr = conf.get(NConstants.MASTER_ADDRESS,
        NConstants.DEFAULT_MASTER_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterAddr);
    if (initIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initIsa);
    }
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this, 
        MasterInterface.class, initIsa);
    this.qm = qm;
    this.stopped = false;
    this.rpcServer.start();
    this.bindAddr = rpcServer.getBindAddress();
    this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();
    processed = new AtomicInteger(0);
    sleeper = new Sleeper();
    this.master = master;
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
  
  public void shutdown() {
    this.stopped = true;
  }

  /* (non-Javadoc)
   * @see nta.engine.ipc.AsyncMasterInterface#reportQueryUnit(nta.engine.QueryUnitProtos.QueryUnitReportProto)
   */
  @Override
  public PingResponseProto reportQueryUnit(PingRequestProto proto) {
//    LOG.info("master received reports from " + proto.getServerName() +
//        ": " + proto.getStatusCount());
    if (master.getClusterManager().getFailedWorkers().contains(proto.getServerName())) {
      LOG.info("**** Dead man alive!!!!!!");
    }

    PingRequest report = new PingRequestImpl(proto);
    for (InProgressStatusProto status : report.getProgressList()) {
      QueryUnitAttemptId uid = new QueryUnitAttemptId(status.getId());
      //qm.updateProgress(uid, status);
      QueryUnitAttempt attempt = qm.getQueryUnitAttempt(uid);
      attempt.updateProgress(status);
      processed.incrementAndGet();
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
        processed.set(0);
        sleeper.sleep(1000);
        LOG.info("processed: " + processed);
      }
    } catch (InterruptedException e) {
      LOG.error(ExceptionUtils.getFullStackTrace(e));
    } finally {
      rpcServer.shutdown();
    }
  }
}
