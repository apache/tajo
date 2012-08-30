/**
 * 
 */
package tajo.engine.cluster;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import tajo.common.Sleeper;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.MasterInterfaceProtos.InProgressStatusProto;
import tajo.engine.MasterInterfaceProtos.PingRequestProto;
import tajo.engine.MasterInterfaceProtos.PingResponseProto;
import tajo.QueryUnitAttemptId;
import tajo.master.TajoMaster;
import tajo.engine.ipc.MasterInterface;
import tajo.engine.ipc.PingRequest;
import tajo.engine.planner.global.QueryUnitAttempt;
import tajo.engine.query.PingRequestImpl;
import tajo.rpc.NettyRpc;
import tajo.rpc.NettyRpcServer;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jihoon
 *
 */
public class WorkerListener extends Thread implements MasterInterface {
  
  private final static Log LOG = LogFactory.getLog(WorkerListener.class);
  private final NettyRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private volatile boolean stopped = false;
  private QueryManager qm;
  private TajoMaster master;
  private AtomicInteger processed;
  private Sleeper sleeper;
  
  public WorkerListener(TajoConf conf, QueryManager qm, TajoMaster master) {
    String confMasterAddr = conf.getVar(ConfVars.MASTER_ADDRESS);
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
