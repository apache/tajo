/**
 * 
 */
package nta.engine;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import nta.catalog.CatalogClient;
import nta.conf.NtaConf;
import nta.engine.LeafServerProtos.AssignTabletRequestProto;
import nta.engine.LeafServerProtos.QueryStatus;
import nta.engine.LeafServerProtos.ReleaseTabletRequestProto;
import nta.engine.LeafServerProtos.SubQueryResponseProto;
import nta.engine.QueryUnitProtos.InProgressStatus;
import nta.engine.QueryUnitProtos.QueryUnitReportProto;
import nta.engine.QueryUnitProtos.QueryUnitRequestProto;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto;
import nta.engine.cluster.MasterAddressTracker;
import nta.engine.ipc.AsyncWorkerInterface;
import nta.engine.ipc.MasterInterface;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.planner.physical.PhysicalExec;
import nta.engine.query.QueryUnitRequestImpl;
import nta.engine.query.TQueryEngine;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;
import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.MapMaker;

/**
 * @author Hyunsik Choi
 */
public class LeafServer extends Thread implements AsyncWorkerInterface {
  private static final Log LOG = LogFactory.getLog(LeafServer.class);

  private final Configuration conf;

  // Server States
  /**
   * This servers address.
   */
  // private final Server rpcServer;
  private ProtoParamRpcServer rpcServer;
  private InetSocketAddress isa;

  private volatile boolean stopped = false;
  private volatile boolean isOnline = false;

  private String serverName;

  // Cluster Management
  private ZkClient zkClient;
  private MasterAddressTracker masterAddrTracker;
  private MasterInterface master;

  // Query Processing
  private FileSystem defaultFS;

  private CatalogClient catalog;
  private TQueryEngine queryEngine;
  private QueryLauncher queryLauncher;
  private List<EngineService> services = new ArrayList<EngineService>();
  
  Map<QueryUnitId, InProgressQuery> queries = new MapMaker()
    .concurrencyLevel(4)
    .makeMap();

  public LeafServer(final Configuration conf) {
    this.conf = conf;
  }
  
  private void prepareServing() throws IOException {
    // Server to handle client requests.
    String hostname = DNS.getDefaultHost(
        conf.get("nta.master.dns.interface", "default"),
        conf.get("nta.master.dns.nameserver", "default"));
    int port = this.conf.getInt(NConstants.LEAFSERVER_PORT,
        NConstants.DEFAULT_LEAFSERVER_PORT);
    // Creation of a HSA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + this.isa);
    }
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this, AsyncWorkerInterface.class, initialIsa);
    this.rpcServer.start();
    
    // Set our address.
    this.isa = this.rpcServer.getBindAddress();
    this.serverName = this.isa.getHostName() + ":" + this.isa.getPort();
    
    this.zkClient = new ZkClient(this.conf);
    this.catalog = new CatalogClient(zkClient);
    this.queryLauncher = new QueryLauncher();
    this.queryLauncher.start();
    this.queryEngine = new TQueryEngine(conf, catalog, zkClient);
    
    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
  }

  private void participateCluster() throws IOException, InterruptedException,
      KeeperException {
    this.masterAddrTracker = new MasterAddressTracker(zkClient);
    this.masterAddrTracker.start();

    byte[] master = null;
    do {    
      master = masterAddrTracker.blockUntilAvailable(1000);
      LOG.info("Waiting for the Tajo master.....");
    } while (master == null);

    LOG.info("Got the master address (" + new String(master) + ")");
    // if the znode already exists, it will be updated for notification.
    ZkUtil.upsertEphemeralNode(zkClient,
        ZkUtil.concat(NConstants.ZNODE_LEAFSERVERS, serverName));
    LOG.info("Created the znode " + NConstants.ZNODE_LEAFSERVERS + "/" 
        + serverName);
    
    InetSocketAddress addr = NetUtils.createSocketAddr(new String(master));
    this.master = (MasterInterface) NettyRpc.getProtoParamBlockingRpcProxy(
        MasterInterface.class, addr);
  }

  public void run() {
    LOG.info("NtaLeafServer startup");

    try {
      try {
        prepareServing();
        participateCluster();
      } catch (Exception e) {
        abort(e.getMessage(), e);
      }

      if (!this.stopped) {
        this.isOnline = true;
        while (!this.stopped) {
          Thread.sleep(1000);
          sendHeartbeat();
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
    } finally {
      for (EngineService service : services) {
        try {
          service.shutdown();
          shutdown("Shutting Down (" + serverName + ")");
        } catch (Exception e) {
          LOG.error(e);
        }
      }

      masterAddrTracker.stop();
      catalog.close();
      zkClient.close();
    }

    LOG.info("LeafServer (" + serverName + ") main thread exiting");
  }
  
  private void sendHeartbeat() throws IOException {
    QueryUnitReportProto.Builder report = QueryUnitReportProto.newBuilder();
    
    // to send
    List<InProgressStatus> list 
      = new ArrayList<QueryUnitProtos.InProgressStatus>();
    InProgressStatus status = null;
    // to be removed
    List<QueryUnitId> tobeRemoved = new ArrayList<QueryUnitId>();
    
    // builds one status for each in-progress query
    for (InProgressQuery ipq : queries.values()) {
      if (ipq.status == QueryStatus.FAILED 
          || ipq.status == QueryStatus.ABORTED
          || ipq.status == QueryStatus.FAILED
          || ipq.status == QueryStatus.FINISHED) {
        // TODO - in-progress queries should be kept until this leafserver 
        // ensures that this report is deliveried.
        tobeRemoved.add(ipq.getId());
      }
      
      status = InProgressStatus.newBuilder()
        .setId(ipq.getId().toString())
        .setProgress(ipq.progress)
        .setStatus(ipq.status)
        .build();      
      
      list.add(status);
    }
    
    report.addAllStatus(list);
    // eliminates aborted, failed, finished queries
    for (QueryUnitId rid : tobeRemoved) {
      this.queries.remove(rid);
    }
    
    master.reportQueryUnit(report.build());
  }

  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      shutdown("Shutdown Hook");
    }
  }

  public String getServerName() {
    return this.serverName;
  }

  /**
   * @return true if a stop has been requested.
   */
  public boolean isStopped() {
    return this.stopped;
  }

  public boolean isOnline() {
    return this.isOnline;
  }

  public void shutdown(final String msg) {
    this.stopped = true;
    LOG.info("STOPPED: " + msg);
    synchronized (this) {
      notifyAll();
    }
  }

  public void abort(String reason, Throwable cause) {
    if (cause != null) {
      LOG.fatal("ABORTING leaf server " + this + ": " + reason, cause);
    } else {
      LOG.fatal("ABORTING leaf server " + this + ": " + reason);
    }
    // TODO - abortRequest : to be implemented
    shutdown(reason);
  }

  // ////////////////////////////////////////////////////////////////////////////
  // LeafServerInterface
  // ////////////////////////////////////////////////////////////////////////////
  @Override
  public SubQueryResponseProto requestQueryUnit(QueryUnitRequestProto proto)
      throws Exception {
    QueryUnitRequest request = new QueryUnitRequestImpl(proto);
    PhysicalExec executor = queryEngine.createPlan(request);    
    InProgressQuery newQuery = new InProgressQuery(request.getId(), executor);
    queryLauncher.addSubQuery(newQuery);

    SubQueryResponseProto.Builder res = SubQueryResponseProto.newBuilder();
    return res.build();
  }

  @Override
  public void assignTablets(AssignTabletRequestProto request) {
    // TODO - not implemented yet
  }

  @Override
  public void releaseTablets(ReleaseTabletRequestProto request) {
    // TODO - not implemented yet
  }

  @Override
  public ServerStatusProto getServerStatus(NullProto request) {
    // serverStatus builder
    ServerStatusProto.Builder serverStatus = ServerStatusProto.newBuilder();

    // system(CPU, memory) status builder
    ServerStatusProto.System.Builder systemStatus = ServerStatusProto.System
        .newBuilder();

    systemStatus.setAvailableProcessors(Runtime.getRuntime()
        .availableProcessors());
    systemStatus.setFreeMemory(Runtime.getRuntime().freeMemory());
    systemStatus.setMaxMemory(Runtime.getRuntime().maxMemory());
    systemStatus.setTotalMemory(Runtime.getRuntime().totalMemory());

    serverStatus.setSystem(systemStatus);

    // disk status builder
    File[] roots = File.listRoots();
    for (File root : roots) {
      ServerStatusProto.Disk.Builder diskStatus = ServerStatusProto.Disk
          .newBuilder();

      diskStatus.setAbsolutePath(root.getAbsolutePath());
      diskStatus.setTotalSpace(root.getTotalSpace());
      diskStatus.setFreeSpace(root.getFreeSpace());
      diskStatus.setUsableSpace(root.getUsableSpace());

      serverStatus.addDisk(diskStatus);
    }
    return serverStatus.build();
  }
  
  private class QueryLauncher extends Thread {
    private final int coreNum = Runtime.getRuntime().availableProcessors();
    private final BlockingQueue<InProgressQuery> queriesToLaunch
      = new ArrayBlockingQueue<InProgressQuery>(coreNum);
    private final ExecutorService executor
      = Executors.newFixedThreadPool(coreNum);
    
    public void addSubQuery(InProgressQuery query) {
      this.queriesToLaunch.add(query);
    }
    
    @Override
    public void run() {
      try {
        LOG.info("Started the query launcher (maximum concurrent tasks: " 
            + coreNum);
        while (!Thread.interrupted()) {
          // wait for add
          InProgressQuery q = queriesToLaunch.take();
          queries.put(q.qid, q);
          executor.submit(q);
        }
      } catch (Throwable t) {
        LOG.error(t);
      }
    }
  }
  
  private static class InProgressQuery implements Runnable {
    private final QueryUnitId qid;
    private final PhysicalExec executor;
    private float progress;
    private QueryStatus status;
    
    private InProgressQuery(QueryUnitId qid, PhysicalExec exec) {
      this.qid = qid;
      this.executor = exec;
      this.progress = 0;
      this.status = QueryStatus.PENDING;
    }
    
    public QueryUnitId getId() {
      return this.qid;
    }
    
    public float getProgress() {
      return this.progress;
    }
    
    public QueryStatus getStatus() {
      return this.status;
    }

    @Override
    public void run() {
      try {
        this.status = QueryStatus.INPROGRESS;
        LOG.info("Query status of " + qid + " is changed to " + status);
        while(executor.next() != null) {}
      } catch (IOException e) {
        this.status = QueryStatus.FAILED;
        this.progress = 0.0f;
        LOG.error("Query unit ("+qid+") is failed", e);
      } finally {
        this.progress = 1.0f;
        this.status = QueryStatus.FINISHED;
      }
    }
  }

  public static void main(String[] args) throws IOException {
    NtaConf conf = new NtaConf();
    LeafServer leafServer = new LeafServer(conf);

    leafServer.start();
  }
}