package nta.engine;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import nta.catalog.CatalogClient;
import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.conf.NtaConf;
import nta.engine.MasterInterfaceProtos.CommandRequestProto;
import nta.engine.MasterInterfaceProtos.CommandResponseProto;
import nta.engine.MasterInterfaceProtos.*;
import nta.engine.cluster.MasterAddressTracker;
import nta.engine.exception.InternalException;
import nta.engine.exception.UnfinishedTaskException;
import nta.engine.ipc.AsyncWorkerInterface;
import nta.engine.ipc.MasterInterface;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.json.GsonCreator;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.physical.PhysicalExec;
import nta.engine.planner.physical.TupleComparator;
import nta.engine.query.QueryUnitRequestImpl;
import nta.engine.query.TQueryEngine;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;
import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;
import nta.storage.StorageUtil;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
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
import tajo.webapp.HttpServer;
import tajo.worker.dataserver.HttpDataServer;
import tajo.worker.dataserver.retriever.AdvancedDataRetriever;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;


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
  private final File workDir;

  private CatalogClient catalog;
  private SubqueryContext.Factory ctxFactory;
  private TQueryEngine queryEngine;
  private QueryLauncher queryLauncher;
  private final int coreNum = Runtime.getRuntime().availableProcessors();
  private final ExecutorService fetchLauncher = 
      Executors.newFixedThreadPool(coreNum);  
  private final Map<QueryUnitId, Task> tasks = Maps.newConcurrentMap();
  private HttpDataServer dataServer;
  private AdvancedDataRetriever retriever;
  private String dataServerURL;
  
  //Web server
  private HttpServer webServer;

  public LeafServer(final Configuration conf) {
    this.conf = conf;
    LOG.info(conf.get(NConstants.WORKER_TMP_DIR));
    this.workDir = new File(conf.get(NConstants.WORKER_TMP_DIR));
  }
  
  private void prepareServing() throws IOException {
    NtaConf c = NtaConf.create(this.conf);
    c.set("fs.default.name", "file:///");
    localFS = LocalFileSystem.get(c);
    Path workDirPath = new Path(workDir.toURI());
    if (!localFS.exists(workDirPath)) {
      localFS.mkdirs(workDirPath);
      LOG.info("local temporal dir is created: " + localFS.exists(workDirPath));
      LOG.info("local temporal dir (" + workDir + ") is created");
    }

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
    this.ctxFactory = new SubqueryContext.Factory();
    this.queryLauncher = new QueryLauncher();
    this.queryLauncher.start();
    this.queryEngine = new TQueryEngine(conf, catalog, zkClient);
    
    this.retriever = new AdvancedDataRetriever();
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

    byte[] master;
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
        
        webServer = new HttpServer("admin", this.isa.getHostName() ,8080 , 
            true, null, conf, null);
        webServer.setAttribute("tajo.master.addr", conf.get(NConstants.MASTER_ADDRESS));
        webServer.start();
      } catch (Exception e) {
        abort(e.getMessage(), e);
      }

      if (!this.stopped) {
        long before = -1;
        long sleeptime = 3000;
        long time;
        this.isOnline = true;
        while (!this.stopped) {
          time = System.currentTimeMillis();
          if (before == -1) {
            sleeptime = 3000;
          } else {
            sleeptime = 3000 - (time - before);
            if (sleeptime > 0) {
              Thread.sleep(sleeptime);
            }
          }
          
          PingResponseProto response = sendHeartbeat(time);
          before = time;
                    
          QueryUnitId qid;
          Task task;
          QueryStatus status;
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
              if (status == QueryStatus.QUERY_FINISHED
              || status == QueryStatus.QUERY_DATASERVER
              || status == QueryStatus.QUERY_ABORTED
              || status == QueryStatus.QUERY_KILLED) {
                task.finalize();
                LOG.info("Query unit ( " + qid + ") is finalized");
              } else {
                LOG.error("ERROR: Illegal State of " + qid + "(" + status + ")");
              }

              break;
            case STOP:
              if (status == QueryStatus.QUERY_INPROGRESS) {
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
        if (t.getStatus() != QueryStatus.QUERY_FINISHED) {
          t.kill();
        }
      }

      // remove the znode
      ZkUtil.concat(NConstants.ZNODE_LEAFSERVERS, serverName);

      try {
        webServer.stop();
      } catch (Exception e) {
        e.printStackTrace();
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
    List<InProgressStatusProto> list 
      = new ArrayList<InProgressStatusProto>();
    InProgressStatusProto status;
    // to be removed
    List<QueryUnitId> tobeRemoved = new ArrayList<QueryUnitId>();
    
    // builds one status for each in-progress query
    QueryStatus qs;
//    LOG.info("========================================");
//    LOG.info("server name: " + serverName);
//    LOG.info("tasks: ");
//    LOG.info(serverName + " # of tasks: " + tasks.size());
    for (Task task : tasks.values()) {
//      LOG.info(task);
      qs = task.getStatus();
      if (qs == QueryStatus.QUERY_ABORTED
          || qs == QueryStatus.QUERY_KILLED
          || qs == QueryStatus.QUERY_FINISHED) {
        // TODO - in-progress queries should be kept until this leafserver 
        // ensures that this report is delivered.
        tobeRemoved.add(task.getId());
      }
      
      status = task.getReport();
      list.add(status);
    }

    ping.addAllStatus(list);
    PingRequestProto proto = ping.build();
//    LOG.info(serverName + " send heartbeat: " + tasks.size());
//    LOG.info("========================================");
    PingResponseProto res = master.reportQueryUnit(proto);
//    LOG.info(serverName + " received PingResponse");
    return res;
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
   
  public File createLocalDir(String...subdir) throws IOException {
    Path tmpDir = StorageUtil.concatPath(new Path(workDir.toString()),
      subdir);
    localFS.mkdirs(tmpDir);
    if(tmpDir.toUri().isAbsolute()){
      return new File(tmpDir.toUri());
    } else {
      return new File(new Path("file:"+tmpDir).toUri());
    }
  }
  
  public static Path getQueryUnitDir(QueryUnitId quid) {
    Path workDir = 
        StorageUtil.concatPath(            
            quid.getScheduleUnitId().getSubQueryId()
            .getQueryId().toString(),
            String.valueOf(quid.getScheduleUnitId().getSubQueryId().getId()),
            String.valueOf((quid.getScheduleUnitId().getId())),
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
      File inputDir = new File(ctx.getWorkDir(), "in");
      inputDir.mkdirs();
      File storeDir;
      
      int i = 0;
      File storeFile;
      List<Fetcher> runnerList = Lists.newArrayList();      
      for (Fetch f : fetches) {
        storeDir = new File(inputDir, f.getName());
        if (!storeDir.exists()) {
          storeDir.mkdirs();
        }
        storeFile = new File(storeDir, "in_" + i);
        Fetcher fetcher = new Fetcher(URI.create(f.getUrls()), storeFile);
        runnerList.add(fetcher);
        i++;
      }
      ctx.addFetchPhase(runnerList.size(), inputDir);
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
      int retryNum = 0;
      int maxRetryNum = 5;
      int retryWaitTime = 1000;

      try { // for releasing fetch latch
        while(retryNum < maxRetryNum) {
          if (retryNum > 0) {
            try {
              Thread.sleep(retryWaitTime);
            } catch (InterruptedException e) {
              LOG.error(e);
            }
            LOG.info("Retry on the fetch: " + fetcher.getURI() + " (" + retryNum + ")");
          }
          try {
            File fetched = fetcher.get();
            if (fetched != null) {
              break;
            }
          } catch (IOException e) {
            LOG.error("Fetch failed: " + fetcher.getURI(), e);
          }
          retryNum++;
        }
      } finally {
        ctx.getFetchLatch().countDown();
      }

      if (retryNum == maxRetryNum) {
        LOG.error("ERROR: the maximum retry (" + retryNum + ") on the fetch exceeded (" + fetcher.getURI() + ")");
      }
    }
  }
  
  private class QueryLauncher extends Thread {
    private final BlockingQueue<Task> blockingQueue
      = new ArrayBlockingQueue<Task>(coreNum);
    private final ExecutorService executor
      = Executors.newFixedThreadPool(coreNum);
    private boolean stopped = false;    
    
    public void schedule(Task task) throws InterruptedException {
      this.blockingQueue.put(task);
      task.setStatus(QueryStatus.QUERY_PENDING);
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
    private boolean aborted = false;

    // TODO - to be refactored
    private ScheduleUnit.PARTITION_TYPE partitionType = null;
    private Schema finalSchema = null;
    private TupleComparator sortComp = null;

    public Task(QueryUnitRequest request) throws IOException {
      File localQueryTmpDir = createWorkDir(request.getId());
      this.ctx = ctxFactory.create(request, localQueryTmpDir);
      plan = GsonCreator.getInstance().fromJson(request.getSerializedData(),
          LogicalNode.class);      
      interQuery = request.getProto().getInterQuery();
      if (interQuery) {
        StoreTableNode store = (StoreTableNode) plan;
        this.partitionType = store.getPartitionType();
        if (store.getSubNode().getType() == ExprType.SORT) {
          SortNode sortNode = (SortNode) store.getSubNode();
          this.finalSchema = PlannerUtil.sortSpecsToSchema(sortNode.getSortKeys());
          this.sortComp = new TupleComparator(finalSchema, sortNode.getSortKeys());
        }
      }
      fetcherRunners = getFetchRunners(ctx, request.getFetches());      
      ctx.setStatus(QueryStatus.QUERY_INITED);
      LOG.info("==================================");
      LOG.info("* Subquery " + request.getId() + " is initialized");
      LOG.info("* InterQuery: " + interQuery
          + (interQuery ? ", Use " + this.partitionType  + " partitioning":""));

      LOG.info("* Fragments (num: " + request.getFragments().size() + ")");
      for (Fragment f: request.getFragments()) {
        LOG.info("==> Table Id:" + f.getId() + ", path:" + f.getPath() + "(" + f.getMeta().getStoreType() + "), " +
            "(start:" + f.getStartOffset() + ", length: " + f.getLength() + ")");
      }
      LOG.info("* Fetches (total:" + request.getFetches().size() + ") :");
      for (Fetch f : request.getFetches()) {
        LOG.info("==> Table Id: " + f.getName() + ", url: " + f.getUrls());
      }
      LOG.info("* Local task dir: " + localQueryTmpDir.getAbsolutePath());
      LOG.info("* plan:\n");
      LOG.info(plan.toString());
      LOG.info("==================================");
    }

    public void init() throws InternalException {      
    }

    public QueryUnitId getId() {
      return ctx.getQueryId();
    }
    
    public QueryStatus getStatus() {
      return ctx.getStatus();
    }
    
    public String toString() {
      return "queryId: " + this.getId() + " status: " + this.getStatus();
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

    private File createWorkDir(QueryUnitId quid) throws IOException {
      return createLocalDir(getQueryUnitDir(quid).toString());
    }
    
    public void kill() {
      killed = true;
      ctx.stop();
      ctx.setStatus(QueryStatus.QUERY_KILLED);
    }
    
    public void finalize() {
      // remove itself from worker
      // 끝난건지 확인
      if (ctx.getStatus() == QueryStatus.QUERY_FINISHED) {
        try {
          // ctx.getWorkDir() 지우기
          localFS.delete(new Path(ctx.getWorkDir().getAbsolutePath()), true);
          // tasks에서 자기 지우기
          tasks.remove(this.getId());
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        LOG.error(new UnfinishedTaskException("QueryUnitId: " 
            + ctx.getQueryId() + " status: " + ctx.getStatus()));
      }
    }

    public InProgressStatusProto getReport() {
      InProgressStatusProto.Builder builder = InProgressStatusProto.newBuilder();
      builder.setId(ctx.getQueryId().getProto())
          .setProgress(ctx.getProgress())
          .setStatus(ctx.getStatus());

/*      if (ctx.getStatSet(ExprType.STORE.toString()) != null) {
        builder.setStats(ctx.getStatSet(ExprType.STORE.toString()).getProto());
      }*/
      if (ctx.hasResultStats()) {
        builder.setResultStats(ctx.getResultStats().getProto());
      }
      
      
      if (ctx.getStatus() == QueryStatus.QUERY_FINISHED && interQuery) {
        Iterator<Entry<Integer,String>> it = ctx.getRepartitions();
        if (it.hasNext()) {          
          do {
            Partition.Builder part = Partition.newBuilder();
            Entry<Integer,String> entry = it.next();
            part = Partition.newBuilder();
            part.setPartitionKey(entry.getKey());
            if (partitionType == ScheduleUnit.PARTITION_TYPE.HASH) {
              part.setFileName(
                  dataServerURL + "/?qid=" + getId().toString() + "&fn=" +
                  entry.getValue());
            } else {
              part.setFileName(dataServerURL + "/?qid=" + getId().toString());
            }
            builder.addPartitions(part.build());
          } while (it.hasNext());
        }
      }

      return builder.build();
    }

    @Override
    public void run() {
      try {
        ctx.setStatus(QueryStatus.QUERY_INPROGRESS);
        LOG.info("Query status of " + ctx.getQueryId() + " is changed to " + getStatus());
        if (ctx.hasFetchPhase()) {
          // If the fetch is still in progress, the query unit must wait for 
          // complete.
          ctx.getFetchLatch().await();
          Collection<String> inputs = Lists.newArrayList(ctx.getInputTables());
          for (String inputTable: inputs) {
            File tableDir = new File(ctx.getFetchIn(), inputTable);
            Fragment [] frags = list(tableDir, inputTable,
                ctx.getTable(inputTable).getMeta());
            ctx.changeFragment(inputTable, frags);
          }
        }
        
        this.executor = queryEngine.createPlan(ctx, plan);
        while(executor.next() != null && !killed) {
        }
      } catch (Exception e) {
        LOG.error(ExceptionUtils.getStackTrace(e));
        aborted = true;
      } finally {
        if (killed || aborted) {
          ctx.setProgress(0.0f);
          QueryStatus failedStatus = null;
          if (killed) {
            failedStatus = QueryStatus.QUERY_KILLED;
          } else if (aborted) {
            failedStatus = QueryStatus.QUERY_ABORTED;
          }
          ctx.setStatus(failedStatus);
          LOG.info("Query status of " + ctx.getQueryId() + " is changed to "
              + failedStatus);
        } else { // if successful
          ctx.setProgress(1.0f);
          if (interQuery) { // TODO - to be completed
            if (partitionType == null || partitionType != ScheduleUnit.PARTITION_TYPE.RANGE) {
              //PartitionRetrieverHandler partitionHandler =
                  //new PartitionRetrieverHandler(ctx.getWorkDir().getAbsolutePath() + "/out/data");
              PartitionRetrieverHandler partitionHandler =
                  new PartitionRetrieverHandler(ctx.getWorkDir().getAbsolutePath() + "/out/data");
              retriever.register(this.getId(), partitionHandler);
            } else {
              RangeRetrieverHandler rangeHandler = null;
              try {
                rangeHandler =
                    new RangeRetrieverHandler(new File(ctx.getWorkDir() + "/out"), finalSchema, sortComp);
              } catch (IOException e) {
                LOG.error("ERROR: cannot initialize RangeRetrieverHandler");
              }
              retriever.register(this.getId(), rangeHandler);
            }
           LOG.info("LeafServer starts to serve as HTTP data server for " 
               + getId());
          }
          ctx.setStatus(QueryStatus.QUERY_FINISHED);
          LOG.info("Query status of " + ctx.getQueryId() + " is changed to "
              + QueryStatus.QUERY_FINISHED);
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
      Fragment tablet;
      
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

  @Override
  public CommandResponseProto requestCommand(CommandRequestProto request) {
    QueryUnitId uid;
    for (Command cmd : request.getCommandList()) {
      uid = new QueryUnitId(cmd.getId());
      Task task = tasks.get(uid);
      QueryStatus status = task.getStatus();
      switch (cmd.getType()) {
      case FINALIZE:
        if (status == QueryStatus.QUERY_FINISHED
        || status == QueryStatus.QUERY_DATASERVER
        || status == QueryStatus.QUERY_ABORTED
        || status == QueryStatus.QUERY_KILLED) {
          task.finalize();          
          LOG.info("Query unit ( " + uid + ") is finalized");
        } else {
          task.kill();
          LOG.info("Query unit ( " + uid + ") is stopped");
        }
        break;
      case STOP:
        task.kill();
        LOG.info("Query unit ( " + uid + ") is stopped");
        break;
      default:
        break;
      }
    }
    return null;
  }
}
