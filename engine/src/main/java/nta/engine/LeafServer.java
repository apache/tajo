/**
 * 
 */
package nta.engine;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import nta.catalog.CatalogClient;
import nta.catalog.TableMeta;
import nta.conf.NtaConf;
import nta.engine.MasterInterfaceProtos.Command;
import nta.engine.MasterInterfaceProtos.Fetch;
import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.MasterInterfaceProtos.Partition;
import nta.engine.MasterInterfaceProtos.PingRequestProto;
import nta.engine.MasterInterfaceProtos.PingResponseProto;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.MasterInterfaceProtos.QueryUnitRequestProto;
import nta.engine.MasterInterfaceProtos.ServerStatusProto;
import nta.engine.MasterInterfaceProtos.SubQueryResponseProto;
import nta.engine.cluster.MasterAddressTracker;
import nta.engine.exception.InternalException;
import nta.engine.ipc.AsyncWorkerInterface;
import nta.engine.ipc.MasterInterface;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.json.GsonCreator;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;

import tajo.datachannel.Fetcher;
import tajo.worker.dataserver.HttpDataServer;

import com.google.common.collect.Lists;
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
  private final int coreNum = Runtime.getRuntime().availableProcessors();
  private final ExecutorService fetchLauncher = 
      Executors.newFixedThreadPool(coreNum);  
  private final Map<QueryUnitId, Task> tasks = Maps.newConcurrentMap();
  private HttpDataServer dataServer;
  private InterDataRetriever retriever;
  private String dataServerURL;

  public LeafServer(final Configuration conf) {
    this.conf = conf;
    this.workDirPath = new Path(conf.get(NConstants.WORKER_TMP_DIR));
  }
  
  private void prepareServing() throws IOException {
    NtaConf c = NtaConf.create(this.conf);
    c.set("fs.default.name", "file:///");
    localFS = LocalFileSystem.get(c);
    if (!localFS.exists(workDirPath)) {
      localFS.mkdirs(workDirPath);
      LOG.info("local temporal dir is created: " + localFS.exists(workDirPath));
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
    
    this.retriever = new InterDataRetriever();
    this.dataServer = new HttpDataServer(NetUtils.createSocketAddr(hostname, 0), 
        retriever);
    this.dataServer.start();
    
    InetSocketAddress dataServerAddr = this.dataServer.getBindAddress(); 
    this.dataServerURL = "http://" + dataServerAddr.getAddress().getHostAddress() + ":" 
        + dataServerAddr.getPort();
    LOG.info("dataserver listens on " + dataServerURL);
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
          PingResponseProto response = sendHeartbeat(time);
                    
          QueryUnitId qid = null;
          Task task = null;
          QueryStatus status = null;
          for (Command cmd : response.getCommandList()) {
            qid = new QueryUnitId(cmd.getId());
            if (!tasks.containsKey(qid)) {
              LOG.error("ERROR: no such task " + qid);
              continue;
            }
            task = tasks.get(qid);
            status = task.getStatus();

            switch (cmd.getType()) {
            case FINALIZE:
              if (status == QueryStatus.FINISHED 
              || status == QueryStatus.DATASERVER
              || status == QueryStatus.ABORTED 
              || status == QueryStatus.KILLED) {
                task.finalize();          
                tasks.remove(qid);
                LOG.info("Query unit ( " + qid + ") is finalized");
              } else {
                LOG.error("ERROR: Illegal State of " + qid + "(" + status + ")");
              }

              break;
            case STOP:
              if (status == QueryStatus.INPROGRESS) {
                task.kill();
                LOG.info("Query unit ( " + qid + ") is killed");
              } else {
                LOG.error("ERROR: Illegal State of " + qid + "(" + status + ")");
              }
              
              break;
            }
          }
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
    } finally {     
      for (Task t : tasks.values()) {
        if (t.getStatus() != QueryStatus.FINISHED) {
          t.kill();
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
    InProgressStatus status = null;
    // to be removed
    List<QueryUnitId> tobeRemoved = new ArrayList<QueryUnitId>();
    
    // builds one status for each in-progress query
    QueryStatus qs = null;
    for (Task task : tasks.values()) {
      qs = task.getStatus();
      if (qs == QueryStatus.ABORTED 
          || qs == QueryStatus.FINISHED) {
        // TODO - in-progress queries should be kept until this leafserver 
        // ensures that this report is delivered.
        tobeRemoved.add(task.getId());
      }
      
      status = task.getReport();
      list.add(status);
    }
    
    ping.addAllStatus(list);    
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
    Task task = new Task(request);
    synchronized(tasks) {
      if (tasks.containsKey(task.getId())) {
        throw new IllegalStateException("Query unit (" + task.getId() + ") is already is submitted");
      }    
      tasks.put(task.getId(), task);
    }        
    if (task.hasFetchPhase()) {
      task.fetch(); // The fetch is performed in an asynchronous way.
    }
    task.init();
    queryLauncher.schedule(task);
    
    SubQueryResponseProto.Builder res = SubQueryResponseProto.newBuilder();
    return res.build();
  }  
  
  private List<Fetcher> getFetchRunners(SubqueryContext ctx, 
      List<Fetch> fetches) {    

    if (fetches.size() > 0) {      
      Path inputDir = new Path(ctx.getWorkDir(), "in");
      File storeDir = new File(inputDir.toString());
      storeDir.mkdirs();
      
      int i = 0;
      File storeFile = null;
      List<Fetcher> runnerList = Lists.newArrayList();      
      for (Fetch f : fetches) {
        storeFile = new File(inputDir.toString(), "in_" + i);        
        Fetcher fetcher = new Fetcher(URI.create(f.getUrls()), storeFile);
        runnerList.add(fetcher);
        i++;
      }
      ctx.addFetchPhase(runnerList.size(), storeDir);
      return runnerList;
    } else {
      return Lists.newArrayList();
    }
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
  
  private class FetchRunner implements Runnable {
    private final SubqueryContext ctx;
    private final Fetcher fetcher;
    
    public FetchRunner(SubqueryContext ctx, Fetcher fetcher) {
      this.ctx = ctx;
      this.fetcher = fetcher;
    }
    @Override
    public void run() {
//      ctx.setStatus(QueryStatus.FETCHING);
      try {
        fetcher.get();
      } catch (IOException e) {
        LOG.error("Fetch failed: " + fetcher.getURI(), e);
      } finally {
        ctx.getFetchLatch().countDown();
//        ctx.setStatus(QueryStatus.PENDING);
      }
    }
  }
  
  private class QueryLauncher extends Thread {
    private final BlockingQueue<Task> blockingQueue
      = new ArrayBlockingQueue<Task>(coreNum);
    private final ExecutorService executor
      = Executors.newFixedThreadPool(coreNum);
    private boolean stopped = false;    
    
    public void schedule(Task task) {      
      this.blockingQueue.add(task);
      task.setStatus(QueryStatus.PENDING);
    }
    
    public void shutdown() {
      stopped = true;
    }
    
    @Override
    public void run() {
      try {
        LOG.info("Started the query launcher (maximum concurrent tasks: " 
            + coreNum + ")");
        while (!Thread.interrupted() && !stopped) {
          // wait for add
          Task task = blockingQueue.poll(1000, TimeUnit.MILLISECONDS);
          
          // the futures keeps submitted tasks for force kill when
          // the leafserver shutdowns.
          if (task != null) {
            executor.submit(task);
          }          
        }
      } catch (Throwable t) {
        LOG.error(t);
      } finally {
        executor.shutdown();
      }
    }
  }  
  
  public class Task implements Runnable {
    private final SubqueryContext ctx;
    private final List<Fetcher> fetcherRunners;
    private final LogicalNode plan;
    private PhysicalExec executor;
    private boolean interQuery;    
    private boolean killed = false;

    public Task(QueryUnitRequest request) throws IOException {
      Path localQueryTmpDir = createWorkDir(request.getId());            
      this.ctx = ctxFactory.create(request, localQueryTmpDir);
      plan = GsonCreator.getInstance().fromJson(request.getSerializedData(),
          LogicalNode.class);      
      interQuery = request.getProto().getInterQuery();      
      fetcherRunners = getFetchRunners(ctx, request.getFetches());      
      ctx.setStatus(QueryStatus.INITED);
    }

    public void init() throws InternalException {      
    }

    public QueryUnitId getId() {
      return ctx.getQueryId();
    }
    
    public QueryStatus getStatus() {
      return ctx.getStatus();
    }
    
    public void setStatus(QueryStatus status) {
      ctx.setStatus(status);
    }

    public boolean hasFetchPhase() {
      return fetcherRunners.size() > 0;
    }
    
    public void fetch() {      
      for (Fetcher f : fetcherRunners) {
        fetchLauncher.submit(new FetchRunner(ctx, f));
      }
    }

    private Path createWorkDir(QueryUnitId quid) throws IOException {
      Path path = createLocalDir(getQueryUnitDir(quid).toString());

      return path;
    }

    public void startDataServer() {

    }
    
    public void kill() {
      killed = true;
      ctx.stop();
      ctx.setStatus(QueryStatus.KILLED);    
    }
    
    public void finalize() {
      // remove itself from worker
    }

    public InProgressStatus getReport() {
      InProgressStatus.Builder builder = InProgressStatus.newBuilder();
      builder.setId(ctx.getQueryId().toString())
          .setProgress(ctx.getProgress())
          .setStatus(ctx.getStatus());

      if (ctx.getStatSet(ExprType.STORE.toString()) != null) {
        builder.setStats(ctx.getStatSet(ExprType.STORE.toString()).getProto());
      }
      
      
      if (ctx.getStatus() == QueryStatus.FINISHED && interQuery) {
        Iterator<Entry<Integer,String>> it = ctx.getRepartitions();
        if (it.hasNext()) {          
          do {
            Partition.Builder part = Partition.newBuilder();
            Entry<Integer,String> entry = it.next();
            part = Partition.newBuilder();
            part.setPartitionKey(entry.getKey());
            part.setFileName(
                dataServerURL + "/?qid=" + getId().toString() + "&fn=" + 
                entry.getValue());
            builder.addPartitions(part.build());
          } while (it.hasNext());
        }
      }

      return builder.build();
    }

    @Override
    public void run() {
      try {
        ctx.setStatus(QueryStatus.INPROGRESS);
        LOG.info("Query status of " + ctx + " is changed to " + getStatus());        
        if (ctx.hasFetchPhase()) {
          // If the fetch is still in progress, the query unit must wait for 
          // complete.
          ctx.getFetchLatch().await();
          Collection<String> inputs = Lists.newArrayList(ctx.getInputTables());
          for (String inputTable: inputs) {
            Fragment [] frags = list(ctx.getFetchIn(), inputTable,
                ctx.getTable(inputTable).getMeta());
            ctx.changeFragment(inputTable, frags);
          }
        }
        
        this.executor = queryEngine.createPlan(ctx, plan);
        while(executor.next() != null && !killed) {
        }
      } catch (Exception e) {
        LOG.error(e);
        ctx.setStatus(QueryStatus.ABORTED);
        ctx.setProgress(0.0f);
        LOG.info("Query status of " + ctx.getQueryId() + " is changed to "   
            + QueryStatus.ABORTED);        
      } finally {
        if (killed == true) {
          ctx.setProgress(0.0f);
          ctx.setStatus(QueryStatus.ABORTED);
          LOG.info("Query status of " + ctx + " is changed to " 
              + QueryStatus.ABORTED);
        } else {
          ctx.setProgress(1.0f);
          if (interQuery) {
           retriever.register(this.getId(), ctx.getWorkDir() + "/out/data");
           LOG.info("LeafServer starts to serve as HTTP data server for " 
               + getId());
          }
          ctx.setStatus(QueryStatus.FINISHED);
          LOG.info("Query status of " + ctx + " is changed to " 
              + QueryStatus.FINISHED);
        }
      }
    }
    
    public int hashCode() {
      return ctx.hashCode();
    }
    
    public boolean equals(Object obj) {
      if (obj instanceof Task) {
        Task other = (Task) obj;
        return this.ctx.equals(other.ctx);
      }      
      return false;
    }
    
    private Fragment[] list(File file, String name, TableMeta meta)
        throws IOException {
      NtaConf c = NtaConf.create(conf);
      c.set("fs.default.name", "file:///");
      FileSystem fs = FileSystem.get(c);
      Path tablePath = new Path(file.getAbsolutePath());      
      
      List<Fragment> listTablets = new ArrayList<Fragment>();
      Fragment tablet = null;
      
      FileStatus[] fileLists = fs.listStatus(tablePath);
      for (FileStatus f : fileLists) {
        tablet = new Fragment(name, f.getPath(), meta, 0l, f.getLen());
        listTablets.add(tablet);         
      }

      Fragment[] tablets = new Fragment[listTablets.size()];
      listTablets.toArray(tablets);

      return tablets;
    }
  }

  public static void main(String[] args) throws IOException {
    NtaConf conf = new NtaConf();
    LeafServer leafServer = new LeafServer(conf);

    leafServer.start();
  }
}