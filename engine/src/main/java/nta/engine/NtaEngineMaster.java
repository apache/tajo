/**
 * 
 */
package nta.engine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import nta.catalog.CatalogServer;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableUtil;
import nta.catalog.exception.AlreadyExistsTableException;
import nta.catalog.exception.NoSuchTableException;
import nta.conf.NtaConf;
import nta.engine.exception.NTAQueryException;
import nta.engine.ipc.QueryEngineInterface;
import nta.engine.query.GlobalEngine;
import nta.storage.StorageManager;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkServer;

import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;

/**
 * @author hyunsik
 * 
 */
public class NtaEngineMaster extends Thread implements QueryEngineInterface {
  private static final Log LOG = LogFactory.getLog(NtaEngineMaster.class);

  private final Configuration conf;
  private FileSystem defaultFS;

  private final InetSocketAddress isa;

  private volatile boolean stopped = false;

  private final String serverName;
  private final ZkClient zkClient;
  ZkServer zkServer = null;

  private CatalogServer catalog;
  private StorageManager storeManager;
  private GlobalEngine queryEngine;

  private final Path basePath;
  private final Path catalogPath;
  private final Path dataPath;

  private final InetSocketAddress bindAddr;
  private ProtoParamRpcServer server; // RPC between master and client

  private List<EngineService> services = new ArrayList<EngineService>();

  public NtaEngineMaster(final Configuration conf) throws Exception {
    this.conf = conf;

    // Server to handle client requests.
    String hostname =
        DNS.getDefaultHost(conf.get("nta.master.dns.interface", "default"),
            conf.get("nta.master.dns.nameserver", "default"));
    int port =
        conf.getInt(NConstants.MASTER_PORT, NConstants.DEFAULT_MASTER_PORT);
    // Creation of a HSA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);

    this.isa = initialIsa;

    this.serverName = this.isa.getHostName() + ":" + this.isa.getPort();

    if (conf.get(NConstants.ZOOKEEPER_HOST).equals("local")) {
      conf.set(NConstants.ZOOKEEPER_HOST, "localhost");
      this.zkServer = new ZkServer(conf);
      this.zkServer.start();
    }

    this.zkClient = new ZkClient(conf);

    String master = conf.get(NConstants.MASTER_HOST, "local");
    if ("local".equals(master)) {
      // local mode
      this.defaultFS = LocalFileSystem.get(conf);
      LOG.info("LocalFileSystem is initialized.");
    } else {
      // remote mode
      this.defaultFS = FileSystem.get(conf);
      LOG.info("FileSystem is initialized.");
    }

    this.basePath = new Path(conf.get(NConstants.ENGINE_BASE_DIR));
    LOG.info("Base dir is set " + conf.get(NConstants.ENGINE_BASE_DIR));
    File baseDir = new File(this.basePath.toString());
    if (baseDir.exists() == false) {
      baseDir.mkdir();
      LOG.info("Base dir (" + baseDir.getAbsolutePath() + ") is created.");
    }

    this.dataPath = new Path(conf.get(NConstants.ENGINE_DATA_DIR));
    LOG.info("Data dir is set " + dataPath);
    if (!defaultFS.exists(dataPath)) {
      defaultFS.mkdirs(dataPath);
      LOG.info("Data dir (" + dataPath + ") is created");
    }
    this.storeManager = new StorageManager(conf);

    this.catalogPath = new Path(conf.get(NConstants.ENGINE_CATALOG_DIR));
    LOG.info("Catalog dir is set to " + this.catalogPath);
    File catalogDir = new File(this.catalogPath.toString());
    if (catalogDir.exists() == false) {
      catalogDir.mkdir();
      LOG.info("Catalog dir (" + catalogDir.getAbsolutePath() + ") is created.");
    }
    this.catalog = new CatalogServer(conf);
    this.catalog.init();
    /*
     * File catalogFile = new
     * File(catalogPath+"/"+NConstants.ENGINE_CATALOG_FILENAME);
     * if(catalogFile.exists()) loadCatalog(catalogFile);
     */
    services.add(catalog);

    this.queryEngine = new GlobalEngine(conf, catalog, storeManager);
    this.queryEngine.init();
    services.add(queryEngine);

    hostname =
        DNS.getDefaultHost(conf.get("engine.master.dns.interface", "default"),
            conf.get("engine.master.dns.nameserver", "default"));
    port = conf.getInt(NConstants.MASTER_PORT, NConstants.DEFAULT_MASTER_PORT);
    // Creation of a HSA will force a resolve.
    initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + this.bindAddr);
    }

    this.server = NettyRpc.getProtoParamRpcServer(this, initialIsa);
    this.bindAddr = this.server.getBindAddress();
    LOG.info(NtaEngine.class.getSimpleName() + " is bind to "
        + bindAddr.getAddress() + ":" + bindAddr.getPort());

    this.server.start();

    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
  }

  private void loadCatalog(File catalogFile) throws Exception {
    FileInputStream fis = new FileInputStream(catalogFile);
    LineNumberReader reader = new LineNumberReader(new InputStreamReader(fis));
    String line = null;

    while ((line = reader.readLine()) != null) {
      String[] cols = line.split("\t");
      attachTable(cols[1], new Path(cols[2]));
    }
  }

  public void run() {
    LOG.info("NtaEngineMaster startup");
    try {
      becomeMaster();
      if (!this.stopped) {
        while (!this.stopped) {
          Thread.sleep(1000);
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
    } finally {
      // TODO - adds code to stop all services and clean resources
    }

    LOG.info("NtaEngineMaster main thread exiting");
  }

  public void becomeMaster() throws IOException, KeeperException,
      InterruptedException {
    zkClient.createPersistent(NConstants.ZNODE_BASE);
    zkClient.createEphemeral(NConstants.ZNODE_MASTER, serverName.getBytes());
    zkClient.createPersistent(NConstants.ZNODE_LEAFSERVERS);
    zkClient.createPersistent(NConstants.ZNODE_QUERIES);
  }

  public String getServerName() {
    return this.serverName;
  }

  public InetSocketAddress getRpcServerAddr() {
    return this.bindAddr;
  }

  public boolean isMasterRunning() {
    return !this.stopped;
  }

  public void init() {

  }

  public void start() {
  }

  public void shutdown() {
    this.stopped = true;
    this.server.shutdown();

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

  public static void main(String[] args) throws Exception {
    NtaConf conf = new NtaConf();
    NtaEngineMaster master = new NtaEngineMaster(conf);

    master.start();
  }

  @Override
  public String executeQuery(String query) {
    // TODO Auto-generated method stub
    return "Path String should be returned";
  }

  @Override
  public String executeQueryAsync(String query) {
    // TODO Auto-generated method stub
    return "Path String should be returned(Async)";
  }

  @Override
  @Deprecated
  public void createTable(TableDescImpl meta) {
    // TODO Auto-generated method stub

  }

  @Override
  @Deprecated
  public void dropTable(String name) {

  }

  @Deprecated
  @Override
  public void attachTable(String name, Path path) throws Exception {
    if (catalog.existsTable(name))
      throw new AlreadyExistsTableException(name);

    LOG.info(path.toUri());

    TableMeta meta = TableUtil.getTableMeta(conf, path);
    TableDesc desc = new TableDescImpl(name, meta);
    desc.setPath(path);
    catalog.addTable(desc);
    LOG.info("Table " + desc.getId() + " is attached.");
  }

  @Override
  public void attachTable(String name, String strPath) throws Exception {

    if (catalog.existsTable(name))
      throw new AlreadyExistsTableException(name);

    Path path = new Path(strPath);

    LOG.info(path.toUri());

    TableMeta meta = TableUtil.getTableMeta(conf, path);
    TableDesc desc = new TableDescImpl(name, meta);
    desc.setPath(path);
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

  public String executeQueryC(String query) throws Exception {
    catalog.updateAllTabletServingInfo(getOnlineServer());
    ResultSetOld rs = queryEngine.executeQuery(query);
    if (rs == null) {
      return "";
    } else {
      return rs.toString();
    }
  }

  public void updateQuery(String query) throws NTAQueryException {
    // TODO Auto-generated method stub

  }

  public TableDesc getTableDesc(String name) throws NoSuchTableException {
    if (!catalog.existsTable(name)) {
      throw new NoSuchTableException(name);
    }

    return catalog.getTableDesc(name);
  }

  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      shutdown();
    }
  }

  public CatalogServer getCatalog() {
    return this.catalog;
  }

}