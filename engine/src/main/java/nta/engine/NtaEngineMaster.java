/**
 * 
 */
package nta.engine;

import nta.catalog.*;
import nta.catalog.exception.AlreadyExistsTableException;
import nta.catalog.exception.NoSuchTableException;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.conf.NtaConf;
import nta.engine.ClientServiceProtos.*;
import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.cluster.*;
import nta.engine.query.GlobalEngine;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;
import nta.rpc.RemoteException;
import nta.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import nta.rpc.protocolrecords.PrimitiveProtos.StringProto;
import nta.storage.StorageManager;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkServer;
import nta.zookeeper.ZkUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;
import tajo.client.ClientService;
import tajo.webapp.StaticHttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Hyunsik Choi
 */
public class NtaEngineMaster extends Thread implements ClientService {
  private static final Log LOG = LogFactory.getLog(NtaEngineMaster.class);

  private final Configuration conf;
  private FileSystem defaultFS;

  private volatile boolean stopped = false;

  private final String clientServiceAddr;
  private final ZkClient zkClient;
  private ZkServer zkServer = null;

  private final Path basePath;
  private final Path dataPath;

  private CatalogService catalog;
  private StorageManager storeManager;
  private GlobalEngine queryEngine;
  private WorkerCommunicator wc;
  private ClusterManager cm;
  private WorkerListener wl;
  private QueryManager qm;

  private final InetSocketAddress clientServiceBindAddr;
  //private RPC.Server clientServiceServer;
  private ProtoParamRpcServer server;

  private List<EngineService> services = new ArrayList<EngineService>();
  
  private LeafServerTracker tracker;
  
  //Web Server
  private StaticHttpServer webServer;
  
  public NtaEngineMaster(final Configuration conf) throws Exception {

    webServer = StaticHttpServer.getInstance(this ,"admin", null, 8080 , 
        true, null, conf, null);
    webServer.start();
    
    this.conf = conf;
    QueryIdFactory.reset();

    // Get the tajo base dir
    this.basePath = new Path(conf.get(NConstants.ENGINE_BASE_DIR));
    LOG.info("Base dir is set " + conf.get(NConstants.ENGINE_BASE_DIR));
    // Get default DFS uri from the base dir
    this.defaultFS = basePath.getFileSystem(conf);
    LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");

    if (!defaultFS.exists(basePath)) {
      defaultFS.mkdirs(basePath);
      LOG.info("Tajo Base dir (" + basePath + ") is created.");
    }

    this.dataPath = new Path(conf.get(NConstants.ENGINE_DATA_DIR));
    LOG.info("Tajo data dir is set " + dataPath);
    if (!defaultFS.exists(dataPath)) {
      defaultFS.mkdirs(dataPath);
      LOG.info("Data dir (" + dataPath + ") is created");
    }

    this.storeManager = new StorageManager(conf);

    // The below is some mode-dependent codes
    // If tajo is local mode
    final String mode = conf.get(NConstants.CLUSTER_DISTRIBUTED);
    if (mode == null || mode.equals(NConstants.CLUSTER_IS_LOCAL)) {
      LOG.info("Enabled Pseudo Distributed Mode");
      this.zkServer = new ZkServer(conf);
      this.zkServer.start();
      conf.set(NConstants.ZOOKEEPER_ADDRESS, "127.0.0.1:2181");

      // TODO - When the RPC framework supports all methods of the catalog
      // server, the below comments should be eliminated.
      // this.catalog = new LocalCatalog(conf);
    } else { // if tajo is distributed mode

      // connect to the catalog server
      // this.catalog = new CatalogClient(conf);
    }
    // This is temporal solution of the above problem.
    this.catalog = new LocalCatalog(conf);
    this.qm = new QueryManager();

    // connect the zkserver
    this.zkClient = new ZkClient(conf);

    this.wl = new WorkerListener(conf, qm);
    this.wl.start();
    // Setup RPC server
    // Get the master address
    LOG.info(NtaEngineMaster.class.getSimpleName() + " is bind to "
        + wl.getAddress());
    this.conf.set(NConstants.MASTER_ADDRESS, wl.getAddress());
    
    String confClientServiceAddr = conf.get(NConstants.CLIENT_SERVICE_ADDRESS, 
        NConstants.DEFAULT_CLIENT_SERVICE_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confClientServiceAddr);
    this.server = 
        NettyRpc
            .getProtoParamRpcServer(this,
                ClientService.class, initIsa);
    
    //this.server = RPC.getServer(this, initIsa.getHostName(), 
        //initIsa.getPort(), conf);
    //this.clientServiceServer.start();
    this.server.start();
    this.clientServiceBindAddr = this.server.getBindAddress();
    this.clientServiceAddr = clientServiceBindAddr.getHostName() + ":" +
        clientServiceBindAddr.getPort();
    LOG.info("Tajo client service master is bind to " + this.clientServiceAddr);
    this.conf.set(NConstants.CLIENT_SERVICE_ADDRESS, this.clientServiceAddr);
    
    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
  }

  private void initMaster() throws Exception {
    
    becomeMaster();
    tracker = new LeafServerTracker(zkClient);
    tracker.start();
    
    this.wc = new WorkerCommunicator(zkClient, tracker);
    this.wc.start();
    this.cm = new ClusterManager(wc, conf, tracker);
    
    this.queryEngine = new GlobalEngine(conf, catalog, storeManager, wc, qm, cm);
    this.queryEngine.init();
    services.add(queryEngine); 
  }

  private void becomeMaster() throws IOException, KeeperException,
      InterruptedException {
    ZkUtil.createPersistentNodeIfNotExist(zkClient, NConstants.ZNODE_BASE);
    ZkUtil.upsertEphemeralNode(zkClient, NConstants.ZNODE_MASTER,
        wl.getAddress().getBytes());
    ZkUtil.createPersistentNodeIfNotExist(zkClient,
        NConstants.ZNODE_LEAFSERVERS);
    ZkUtil.createPersistentNodeIfNotExist(zkClient, NConstants.ZNODE_QUERIES);
    ZkUtil.upsertEphemeralNode(zkClient, NConstants.ZNODE_CLIENTSERVICE,
        clientServiceAddr.getBytes());
  }

  public void run() {
    LOG.info("NtaEngineMaster startup");
    try {
      initMaster();

      if (!this.stopped) {
        while (!this.stopped) {
          Thread.sleep(2000);
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
    } finally {
      // TODO - adds code to stop all services and clean resources
    }

    LOG.info("NtaEngineMaster main thread exiting");
  }

  public String getMasterServerName() {
    return this.wl.getAddress();
  }
  
  public String getClientServiceServerName() {
    return this.clientServiceAddr;
  }

  public InetSocketAddress getRpcServerAddr() {
    return this.clientServiceBindAddr;
  }

  public boolean isMasterRunning() {
    return !this.stopped;
  }

  public void shutdown() {
    try {
      webServer.stop();
    } catch (Exception e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    tracker.close();
    this.stopped = true;
    this.server.shutdown();
    if (wc != null) {
      this.wc.close();
    }
    this.wl.stop();

    for (EngineService service : services) {
      try {
        service.shutdown();
      } catch (Exception e) {
        LOG.error(e);
      }
    }
  }

  public List<String> getOnlineServer() throws KeeperException,
      InterruptedException {
    return zkClient.getChildren(NConstants.ZNODE_LEAFSERVERS);
  }

  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      shutdown();
    }
  }

  public CatalogService getCatalog() {
    return this.catalog;
  }
	
  public WorkerCommunicator getWorkerCommunicator() {
    return wc;
  }

  public ClusterManager getClusterManager() {
    return cm;
  }
  
  public QueryManager getQueryManager() {
    return this.qm;
  }
  
  public GlobalEngine getGlobalEngine() {
    return this.queryEngine;
  }
  
  public StorageManager getStorageManager() {
    return this.storeManager;
  }
  
  public LeafServerTracker getTracker() {
    return tracker;
  }
	
	// TODO - to be improved
	public Collection<InProgressStatus> getProgressQueries() {
	  return this.qm.getAllProgresses().values();
	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new NtaConf();
    
    NtaEngineMaster master = new NtaEngineMaster(conf);

    master.start();
  }

  /////////////////////////////////////////////////////////////////////////////
  // ClientService
  /////////////////////////////////////////////////////////////////////////////
  @Override
  public ExecuteQueryRespose executeQuery(ExecuteQueryRequest query) throws RemoteException {
    try {
      cm.updateAllFragmentServingInfo(cm.getOnlineWorker());
    } catch (IOException ioe) {
      LOG.error(ioe);
      throw new RemoteException(ioe);
    }
    String path;
    long elapsed;
    try {
      long start = System.currentTimeMillis();
      path = queryEngine.executeQuery(query.getQuery());
      long end = System.currentTimeMillis();
      elapsed = end - start;
    } catch (Exception e) {
      throw new RemoteException(e);
    }
    
    ExecuteQueryRespose.Builder build = ExecuteQueryRespose.newBuilder();
    build.setPath(path);
    build.setResponseTime(elapsed);
    return build.build();
  }

  @Override
  public AttachTableResponse attachTable(AttachTableRequest request)
      throws RemoteException {    
    if (catalog.existsTable(request.getName()))
      throw new AlreadyExistsTableException(request.getName());

    Path path = new Path(request.getPath());

    LOG.info(path.toUri());

    TableMeta meta;
    try {
      meta = TableUtil.getTableMeta(conf, path);
    } catch (IOException e) {
      throw new RemoteException(e);
    }
    TableDesc desc = new TableDescImpl(request.getName(), meta, path);
    catalog.addTable(desc);
    LOG.info("Table " + desc.getId() + " is attached.");
    
    return AttachTableResponse.newBuilder().
        setDesc((TableDescProto) desc.getProto()).build();
  }

  @Override
  public void detachTable(StringProto name) throws RemoteException {
    if (!catalog.existsTable(name.getValue())) {
      throw new NoSuchTableException(name.getValue());
    }

    catalog.deleteTable(name.getValue());
    LOG.info("Table " + name + " is detached.");
  }

  @Override
  public CreateTableResponse createTable(CreateTableRequest request)
      throws RemoteException {    
    if (catalog.existsTable(request.getName()))
      throw new AlreadyExistsTableException(request.getName());

    Path path = new Path(request.getPath());
    LOG.info(path.toUri());
    
    TableDesc desc = new TableDescImpl(request.getName(), 
        new TableMetaImpl(request.getMeta()), path);
    catalog.addTable(desc);
    LOG.info("Table " + desc.getId() + " is attached.");
    
    return CreateTableResponse.newBuilder().
        setDesc((TableDescProto) desc.getProto()).build();
  }
  
  @Override
  public BoolProto existTable(StringProto name) throws RemoteException {    
    BoolProto.Builder res = BoolProto.newBuilder();
    return res.setValue(catalog.existsTable(name.getValue())).build();
  }

  @Override
  public void dropTable(StringProto name) throws RemoteException {
    if (!catalog.existsTable(name.getValue())) {
      throw new NoSuchTableException(name.getValue());
    }

    Path path = catalog.getTableDesc(name.getValue()).getPath();
    catalog.deleteTable(name.getValue());
    try {
      this.storeManager.delete(path);
    } catch (IOException e) {
      throw new RemoteException(e);
    }
    LOG.info("Drop Table " + name);
  }
  
  @Override
  public GetClusterInfoResponse getClusterInfo(GetClusterInfoRequest request) throws RemoteException {
    List<String> onlineServers;
    try {
      onlineServers = getOnlineServer();
      if (onlineServers == null) {
       throw new NullPointerException(); 
      }
    } catch (Exception e) {
      throw new RemoteException(e);
    }
    GetClusterInfoResponse.Builder builder = GetClusterInfoResponse.newBuilder();
    builder.addAllServerName(onlineServers);
    return builder.build();
  }
  
  @Override
  public GetTableListResponse getTableList(GetTableListRequest request) {    
    Collection<String> tableNames = catalog.getAllTableNames(); 
    GetTableListResponse.Builder builder = GetTableListResponse.newBuilder();
    builder.addAllTables(tableNames);
    return builder.build();
  }

  @Override
  public TableDescProto getTableDesc(StringProto proto)
      throws RemoteException {
    String name = proto.getValue();
    if (!catalog.existsTable(name)) {
      throw new NoSuchTableException(name);
    }

    return (TableDescProto) catalog.getTableDesc(name).getProto();
  }
}