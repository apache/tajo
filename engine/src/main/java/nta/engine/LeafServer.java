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
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import nta.catalog.CatalogClient;
import nta.catalog.statistics.StatSet;
import nta.conf.NtaConf;
import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.MasterInterfaceProtos.PingRequestProto;
import nta.engine.MasterInterfaceProtos.PingResponseProto;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.MasterInterfaceProtos.QueryUnitRequestProto;
import nta.engine.MasterInterfaceProtos.ServerStatusProto;
import nta.engine.MasterInterfaceProtos.SubQueryResponseProto;
import nta.engine.cluster.MasterAddressTracker;
import nta.engine.ipc.AsyncWorkerInterface;
import nta.engine.ipc.MasterInterface;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.physical.PhysicalExec;
import nta.engine.query.QueryUnitRequestImpl;
import nta.engine.query.TQueryEngine;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;
import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;
import nta.storage.StorageUtil;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;

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
  private FileSystem localFS;
  private FileSystem defaultFS;
  private final Path workDirPath;

  private CatalogClient catalog;
  private SubqueryContext.Factory ctxFactory;
  private TQueryEngine queryEngine;
  private QueryLauncher queryLauncher;
  private List<EngineService> services = new ArrayList<EngineService>();
  
  Map<QueryUnitId, InProgressQuery> queries = new MapMaker()
    .concurrencyLevel(4)
    .makeMap();

  public LeafServer(final Configuration conf) {
    this.conf = conf;
    this.workDirPath = new Path(conf.get(NConstants.WORKER_TMP_DIR));    
  }
  
  private void prepareServing() throws IOException {
    localFS = LocalFileSystem.get(this.workDirPath.toUri(), conf);
    if (!localFS.exists(workDirPath)) {
      localFS.mkdirs(workDirPath);
      LOG.info("local temporal dir (" + workDirPath + ") is created");
    }
    
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
    this.ctxFactory = new SubqueryContext.Factory(catalog);
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
  
  public FileSystem getLocalFS() {
    return this.localFS;
  }
  
  public FileSystem getDefaultFS() {
    return this.defaultFS;
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
          long time = System.currentTimeMillis();
          @SuppressWarnings("unused")
          PingResponseProto response = sendHeartbeat(time);
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
      rpcServer.shutdown();
      queryLauncher.shutdown();      
      masterAddrTracker.stop();
      catalog.close();
      zkClient.close();
    }

    LOG.info("LeafServer (" + serverName + ") main thread exiting");
  }
  
  private PingResponseProto sendHeartbeat(long time) throws IOException {
    PingRequestProto.Builder ping = PingRequestProto.newBuilder();
    ping.setTimestamp(time);
    ping.setServerName(serverName);
    
    // to send
    List<InProgressStatus> list 
      = new ArrayList<InProgressStatus>();
    InProgressStatus.Builder status = null;
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
      
      status = InProgressStatus.newBuilder();      
      status.setId(ipq.getId().toString())
        .setProgress(ipq.getProgress())
        .setStatus(ipq.getStatus());        
      
      if (ipq.getStats() != null) {
        status.setStats(ipq.getStats().getProto());
      }
      
      list.add(status.build());
    }
    
    ping.addAllStatus(list);
    // eliminates aborted, failed, finished queries
    for (QueryUnitId rid : tobeRemoved) {
      this.queries.remove(rid);
    }
    
    return master.reportQueryUnit(ping.build());
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
   
  public Path createLocalDir(String...subdir) throws IOException {
    Path tmpDir = StorageUtil.concatPath(workDirPath, subdir);
    localFS.mkdirs(tmpDir);
    
    return tmpDir;
  }
  
  public Path createQueryTmpDir(QueryUnitId quid) throws IOException {
    Path path = createLocalDir(getQueryUnitDir(quid).toString());
    
    return path;
  }
  
  public static Path getQueryUnitDir(QueryUnitId quid) {
    Path workDir = 
        StorageUtil.concatPath(            
            quid.getLogicalQueryUnitId().getSubQueryId()
            .getQueryId().toString(),
            String.valueOf(quid.getLogicalQueryUnitId().getSubQueryId().getId()),
            String.valueOf((quid.getLogicalQueryUnitId().getId())),
            String.valueOf(quid.getId()));
    return workDir;
  }

  // ////////////////////////////////////////////////////////////////////////////
  // LeafServerInterface
  // ////////////////////////////////////////////////////////////////////////////
  @Override
  public SubQueryResponseProto requestQueryUnit(QueryUnitRequestProto proto)
      throws Exception {
    QueryUnitRequest request = new QueryUnitRequestImpl(proto);
    Path localQueryTmpDir = createQueryTmpDir(request.getId());
    SubqueryContext ctx = ctxFactory.create(request);
    PhysicalExec executor = queryEngine.createPlan(ctx, request, 
        localQueryTmpDir);    
    InProgressQuery newQuery = new InProgressQuery(ctx, executor);
    queryLauncher.addSubQuery(newQuery);

    SubQueryResponseProto.Builder res = SubQueryResponseProto.newBuilder();
    return res.build();
  }
  
  @VisibleForTesting
  void requestTestQuery(PhysicalExec exec) {    
    SubqueryContext ctx = new SubqueryContext(QueryIdFactory.newQueryUnitId(),
        new Fragment[] {});
    InProgressQuery newQuery = new InProgressQuery(ctx, exec);
    queryLauncher.addSubQuery(newQuery);
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
    private final BlockingQueue<InProgressQuery> blockingQueue
      = new ArrayBlockingQueue<InProgressQuery>(coreNum);
    private final ExecutorService executor
      = Executors.newFixedThreadPool(coreNum);
    private boolean stopped = false;
    @SuppressWarnings("rawtypes")
    private Map<InProgressQuery, Future> futures =
      Maps.newConcurrentMap();
    
    
    public void addSubQuery(InProgressQuery query) {
      this.blockingQueue.add(query);
    }
    
    public void shutdown() {
      stopped = true;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void run() {
      try {
        LOG.info("Started the query launcher (maximum concurrent tasks: " 
            + coreNum);
        while (!Thread.interrupted() && !stopped) {
          // wait for add
          InProgressQuery q = blockingQueue.poll(1000, TimeUnit.MILLISECONDS);
          if (q != null) {
            queries.put(q.getId(), q);
            futures.put(q, executor.submit(q));
          }
          
          for (Entry<InProgressQuery,Future> entry : futures.entrySet()) {
            if (entry.getValue().isDone()) {
              futures.remove(entry.getKey());
            }
          }
        }
      } catch (Throwable t) {
        LOG.error(t);
      } finally {
        executor.shutdown();
        for (Entry<InProgressQuery,Future> entry : futures.entrySet()) {
          if (!entry.getValue().isDone()) {
            entry.getKey().abort();
          }
        }
      }
    }
  }
  
  private static class InProgressQuery implements Runnable {
    private final SubqueryContext ctx;
    private final PhysicalExec executor;
    private float progress;
    private QueryStatus status;
    private boolean stopped = false;
    private boolean aborted = false;
    
    private InProgressQuery(SubqueryContext ctx, PhysicalExec exec) {
      this.ctx = ctx;
      this.executor = exec;
      this.progress = 0;
      this.status = QueryStatus.PENDING;
    }
    
    public QueryUnitId getId() {
      return ctx.getQueryId();
    }
    
    public float getProgress() {
      return this.progress;
    }
    
    public QueryStatus getStatus() {
      return this.status;
    }
    
    public StatSet getStats() {
      StatSet stats = ctx.getStatSet(ExprType.STORE.toString());
      if (stats != null) {
        return stats;
      } else {
        return null;
      }
    }
    
    public void abort() {
      stopped = true;
      aborted = true;
      synchronized (this) {
        this.notifyAll();
      }
    }

    @Override
    public void run() {
      try {
        this.status = QueryStatus.INPROGRESS;
        LOG.info("Query status of " + ctx + " is changed to " + status);
        while(executor.next() != null && !stopped) {
        }
      } catch (IOException e) {
        this.status = QueryStatus.FAILED;
        this.progress = 0.0f;
        LOG.info("Query status of " + ctx + " is changed to "   + QueryStatus.FAILED);
      } finally {
        if (aborted == true) {
          this.progress = 0.0f;
          this.status = QueryStatus.ABORTED;
          LOG.info("Query status of " + ctx + " is changed to " 
              + QueryStatus.ABORTED);
        } else {
          this.progress = 1.0f;
          this.status = QueryStatus.FINISHED;
          LOG.info("Query status of " + ctx + " is changed to " 
              + QueryStatus.FINISHED);
        }
      }
    }
    
    public int hashCode() {
      return this.ctx.hashCode();
    }
    
    public boolean equals(Object obj) {
      if (obj instanceof InProgressQuery) {
        InProgressQuery other = (InProgressQuery) obj;
        return this.ctx.equals(other.ctx);
      }      
      return false;
    }
  }

  public static void main(String[] args) throws IOException {
    NtaConf conf = new NtaConf();
    LeafServer leafServer = new LeafServer(conf);

    leafServer.start();
  }
}