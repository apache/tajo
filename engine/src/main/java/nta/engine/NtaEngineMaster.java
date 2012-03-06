/**
 * 
 */
package nta.engine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nta.catalog.CatalogService;
import nta.catalog.LocalCatalog;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableUtil;
import nta.catalog.exception.AlreadyExistsTableException;
import nta.catalog.exception.NoSuchTableException;
import nta.conf.NtaConf;
import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.cluster.ClusterManager;
import nta.engine.cluster.LeafServerTracker;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.cluster.WorkerListener;
import nta.engine.ipc.QueryEngineInterface;
import nta.engine.json.GsonCreator;
import nta.engine.query.GlobalEngine;
import nta.storage.StorageManager;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;

/**
 * @author Hyunsik Choi
 * 
 */
public class NtaEngineMaster extends Thread implements QueryEngineInterface {
  private static final Log LOG = LogFactory.getLog(NtaEngineMaster.class);

  private final Configuration conf;
  private FileSystem defaultFS;

  private volatile boolean stopped = false;

  private final String clientServiceAddr;
  private final ZkClient zkClient;

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
  private RPC.Server clientServiceServer;

  private List<EngineService> services = new ArrayList<EngineService>();
  
  private LeafServerTracker tracker;
  
  public NtaEngineMaster(final Configuration conf) throws Exception {
    this.conf = conf;
    QueryIdFactory.reset();

    // Get the tajo base dir
    this.basePath = new Path(conf.get(NConstants.ENGINE_BASE_DIR));
    LOG.info("Base dir is set " + conf.get(NConstants.ENGINE_BASE_DIR));
    // Get default DFS uri from the base dir
    this.defaultFS = basePath.getFileSystem(conf);
    LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");

    if (defaultFS.exists(basePath) == false) {
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
    if (conf.get(NConstants.CLUSTER_DISTRIBUTED, NConstants.CLUSTER_IS_LOCAL)
        .equals("false")) {
      LOG.info("Enabled Pseudo Distributed Mode");
      /*
       * this.zkServer = new ZkServer(conf); this.zkServer.start();
       */

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
    this.clientServiceServer = RPC.getServer(this, initIsa.getHostName(), 
        initIsa.getPort(), conf);
    this.clientServiceServer.start();
    this.clientServiceBindAddr = this.clientServiceServer.getListenerAddress();
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
    tracker.close();
    this.stopped = true;
    this.clientServiceServer.stop();
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
  
  @Override
  public String executeQuery(String query) throws Exception {
    cm.updateAllFragmentServingInfo(cm.getOnlineWorker());
    String rs = queryEngine.executeQuery(query);
    if (rs == null) {
      return "";
    } else {
      return rs.toString();
    }
  }

  @Override
  public String executeQueryAsync(String query) {
    // TODO Auto-generated method stub
    return "Path String should be returned(Async)";
  }

  @Override
  public void attachTable(String name, String strPath) throws Exception {

    if (catalog.existsTable(name))
      throw new AlreadyExistsTableException(name);

    Path path = new Path(strPath);

    LOG.info(path.toUri());

    TableMeta meta = TableUtil.getTableMeta(conf, path);
    TableDesc desc = new TableDescImpl(name, meta, path);
    catalog.addTable(desc);
    LOG.info("Table " + desc.getId() + " is attached.");
  }

  @Override
  public void detachTable(String name) throws Exception {
    if (!catalog.existsTable(name)) {
      throw new NoSuchTableException(name);
    }

    catalog.deleteTable(name);
    LOG.info("Table " + name + " is detached.");
  }

  @Override
  public boolean existsTable(String name) {
    return catalog.existsTable(name);
  }

  public String getTableDesc(String name) throws NoSuchTableException {
    if (!catalog.existsTable(name)) {
      throw new NoSuchTableException(name);
    }

    return catalog.getTableDesc(name).toJSON();
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

	public String getClusterInfo() throws KeeperException, InterruptedException {
		List<String> onlineServers = getOnlineServer();
		String json = GsonCreator.getInstance().toJson(onlineServers);
		return json;
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		return 0l;
	}

	@Override
	public String getTableList() {		
		Collection<String> tableNames = catalog.getAllTableNames();		
		return GsonCreator.getInstance().toJson(tableNames);
	}
	
  public WorkerCommunicator getWorkerCommunicator() {
    return wc;
  }

  public ClusterManager getClusterManager() {
    return cm;
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
}