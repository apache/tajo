/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tajo.catalog.*;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.global.rewriter.GlobalPlanTestRuleProvider;
import org.apache.tajo.exception.UnsupportedCatalogStore;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.plan.rewrite.LogicalPlanTestRuleProvider;
import org.apache.tajo.querymaster.Query;
import org.apache.tajo.querymaster.QueryMasterTask;
import org.apache.tajo.querymaster.Stage;
import org.apache.tajo.querymaster.StageState;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.storage.FileTablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.history.QueryHistory;
import org.apache.tajo.worker.TajoWorker;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TajoTestingCluster {
	private static Log LOG = LogFactory.getLog(TajoTestingCluster.class);
	private TajoConf conf;
  private FileSystem defaultFS;
  private MiniDFSCluster dfsCluster;
	private CatalogServer catalogServer;
  private HBaseTestClusterUtil hbaseUtil;

  private TajoMaster tajoMaster;
  private List<TajoWorker> tajoWorkers = new ArrayList<>();
  private boolean isDFSRunning = false;
  private boolean isTajoClusterRunning = false;
  private boolean isCatalogServerRunning = false;

	private File clusterTestBuildDir = null;

	/**
	 * Default parent directory for test output.
	 */
	public static final String DEFAULT_TEST_DIRECTORY = "target/" + 
	    System.getProperty("tajo.test.data.dir", "test-data");

  /**
   * True If HiveCatalogStore is used. Otherwise, it is FALSE.
   */
  public Boolean isHiveCatalogStoreUse = false;

  private static final String LOG_LEVEL;

  static {
    LOG_LEVEL = System.getProperty("LOG_LEVEL");
  }

  public TajoTestingCluster() {
    this(false);
  }

  public TajoTestingCluster(boolean masterHaEMode) {
    this.conf = new TajoConf();
    this.conf.setBoolVar(ConfVars.TAJO_MASTER_HA_ENABLE, masterHaEMode);

    initTestDir();
    setTestingFlagProperties();
    initPropertiesAndConfigs();
  }

  void setTestingFlagProperties() {
    System.setProperty(TajoConstants.TEST_KEY, Boolean.TRUE.toString());
    conf.set(TajoConstants.TEST_KEY, Boolean.TRUE.toString());
  }

  void initPropertiesAndConfigs() {

    // Injection of equality testing code of logical plan (de)serialization
    conf.setClassVar(ConfVars.LOGICAL_PLAN_REWRITE_RULE_PROVIDER_CLASS, LogicalPlanTestRuleProvider.class);
    conf.setClassVar(ConfVars.GLOBAL_PLAN_REWRITE_RULE_PROVIDER_CLASS, GlobalPlanTestRuleProvider.class);
    conf.setLongVar(ConfVars.$DIST_QUERY_BROADCAST_CROSS_JOIN_THRESHOLD, 1024 * 1024); // 1GB

    conf.setInt(ConfVars.WORKER_RESOURCE_AVAILABLE_CPU_CORES.varname, 4);
    conf.setInt(ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB.varname, 2000);
    conf.setInt(ConfVars.WORKER_RESOURCE_AVAILABLE_DISK_PARALLEL_NUM.varname, 3);
    conf.setInt(ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM.varname, 1);

    // Client API RPC
    conf.setIntVar(ConfVars.RPC_CLIENT_WORKER_THREAD_NUM, 2);

    //Client API service RPC Server
    conf.setIntVar(ConfVars.MASTER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(ConfVars.WORKER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(ConfVars.REST_SERVICE_RPC_SERVER_WORKER_THREAD_NUM, 2);

    // Internal RPC Client
    conf.setIntVar(ConfVars.INTERNAL_RPC_CLIENT_WORKER_THREAD_NUM, 2);
    conf.setIntVar(ConfVars.SHUFFLE_RPC_CLIENT_WORKER_THREAD_NUM, 2);

    // Internal RPC Server
    conf.setIntVar(ConfVars.MASTER_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(ConfVars.QUERY_MASTER_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(ConfVars.WORKER_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(ConfVars.CATALOG_RPC_SERVER_WORKER_THREAD_NUM, 2);
    conf.setIntVar(ConfVars.SHUFFLE_RPC_SERVER_WORKER_THREAD_NUM, 2);

    // Memory cache termination
    conf.setIntVar(ConfVars.HISTORY_QUERY_CACHE_SIZE, 10);

    // Python function path
    URL pythonUdfURL = getClass().getResource("/pyudf");
    if (pythonUdfURL != null) {
      conf.setStrings(ConfVars.PYTHON_CODE_DIR.varname, pythonUdfURL.toString());
    }

    // Buffer size
    conf.setInt(ConfVars.$EXECUTOR_EXTERNAL_SORT_BUFFER_SIZE.varname, 1);
    conf.setInt(ConfVars.$EXECUTOR_HASH_SHUFFLE_BUFFER_SIZE.varname, 1);

    /* decrease Hbase thread and memory cache for testing */
    //server handler
    conf.setInt("hbase.regionserver.handler.count", 5);
    //client handler
    conf.setInt("hbase.hconnection.threads.core", 5);
    conf.setInt("hbase.hconnection.threads.max", 10);
    conf.setInt("hbase.hconnection.meta.lookup.threads.core", 5);
    conf.setInt("hbase.hconnection.meta.lookup.threads.max", 10);

    //memory cache
    conf.setFloat("hfile.block.cache.size", 0.0f);  //disable cache
    conf.setBoolean("hbase.bucketcache.combinedcache.enabled", false);

    /* Since Travis CI limits the size of standard output log up to 4MB */
    if (!StringUtils.isEmpty(LOG_LEVEL)) {
      Level defaultLevel = Logger.getRootLogger().getLevel();
      Logger.getLogger("org.apache.tajo").setLevel(Level.toLevel(LOG_LEVEL.toUpperCase(), defaultLevel));
      Logger.getLogger("org.apache.hadoop").setLevel(Level.toLevel(LOG_LEVEL.toUpperCase(), defaultLevel));
      Logger.getLogger("org.apache.zookeeper").setLevel(Level.toLevel(LOG_LEVEL.toUpperCase(), defaultLevel));
      Logger.getLogger("BlockStateChange").setLevel(Level.toLevel(LOG_LEVEL.toUpperCase(), defaultLevel));
      Logger.getLogger("org.mortbay.log").setLevel(Level.toLevel(LOG_LEVEL.toUpperCase(), defaultLevel));
    }
  }

	public TajoConf getConfiguration() {
		return this.conf;
	}

  public void initTestDir() {
    if (clusterTestBuildDir == null) {
      clusterTestBuildDir = setupClusterTestBuildDir();
    }
  }

	/**
	 * @return Where to write test data on local filesystem; usually
	 * {@link #DEFAULT_TEST_DIRECTORY}
	 * @see #setupClusterTestBuildDir()
	 */
	public File getTestDir() {
		return clusterTestBuildDir;
	}

	/**
	 * @param subdirName
	 * @return Path to a subdirectory named <code>subdirName</code> under
	 * {@link #getTestDir()}.
	 * @see #setupClusterTestBuildDir()
	 */
	public static File getTestDir(final String subdirName) {
		return new File(new File(DEFAULT_TEST_DIRECTORY), subdirName);
  }

	public static File setupClusterTestBuildDir() {
		String randomStr = UUID.randomUUID().toString();
		String dirStr = getTestDir(randomStr).toString();
		File dir = new File(dirStr).getAbsoluteFile();
		// Have it cleaned up on exit
		dir.deleteOnExit();
		return dir;
	}

  ////////////////////////////////////////////////////////
  // HDFS Section
  ////////////////////////////////////////////////////////
  /**
   * Start a minidfscluster.
   * @param servers How many DNs to start.
   * @throws Exception
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(int servers) throws Exception {
    return startMiniDFSCluster(servers, null, null);
  }

  /**
   * Start a minidfscluster.
   * Can only create one.
   * @param servers How many DNs to start.
   * @param dir Where to home your dfs cluster.
   * @param hosts hostnames DNs to run on.
   * @throws Exception
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   * @throws java.io.IOException
   */
  public MiniDFSCluster startMiniDFSCluster(int servers,
                                            File dir,
                                            final String hosts[])
      throws IOException {

    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dir.getAbsolutePath());
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, false);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, 0);

    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(new HdfsConfiguration(conf));
    builder.hosts(hosts);
    builder.numDataNodes(servers);
    builder.format(true);
    builder.waitSafeMode(true);
    this.dfsCluster = builder.build();

    // Set this just-started cluster as our filesystem.
    this.defaultFS = this.dfsCluster.getFileSystem();
    this.conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultFS.getUri().toString());
    this.conf.setVar(TajoConf.ConfVars.ROOT_DIR, defaultFS.getUri() + "/tajo");
    isDFSRunning = true;
    return this.dfsCluster;
  }

  public void shutdownMiniDFSCluster() throws Exception {
    if (this.dfsCluster != null) {
      try {
        FileSystem fs = this.dfsCluster.getFileSystem();
        if (fs != null) fs.close();
      } catch (IOException e) {
        System.err.println("error closing file system: " + e);
      }
      // The below throws an exception per dn, AsynchronousCloseException.
      this.dfsCluster.shutdown();
    }
  }

  public boolean isRunningDFSCluster() {
    return this.defaultFS != null;
  }

  public MiniDFSCluster getMiniDFSCluster() {
    return this.dfsCluster;
  }

  public FileSystem getDefaultFileSystem() {
    return this.defaultFS;
  }

  public HBaseTestClusterUtil getHBaseUtil() {
    return hbaseUtil;
  }

  ////////////////////////////////////////////////////////
  // Catalog Section
  ////////////////////////////////////////////////////////
  public CatalogServer startCatalogCluster() throws Exception {
    if(isCatalogServerRunning) throw new IOException("Catalog Cluster already running");

    CatalogTestingUtil.configureCatalog(conf, clusterTestBuildDir.getAbsolutePath());
    LOG.info("Apache Derby repository is set to " + conf.get(CatalogConstants.CATALOG_URI));
    conf.setVar(ConfVars.CATALOG_ADDRESS, "localhost:0");

    catalogServer = new CatalogServer();
    catalogServer.init(conf);
    catalogServer.start();
    isCatalogServerRunning = true;
    return this.catalogServer;
  }

  public void shutdownCatalogCluster() {

    try {
      CatalogTestingUtil.shutdownCatalogStore(conf);
    } catch (Exception e) {
      //ignore
    }

    if (catalogServer != null) {
      this.catalogServer.stop();
    }
    isCatalogServerRunning = false;
  }

  public CatalogServer getMiniCatalogCluster() {
    return this.catalogServer;
  }

  public CatalogService getCatalogService() {
    if (catalogServer != null) {
      return new LocalCatalogWrapper(catalogServer);
    } else {
      return tajoMaster.getCatalog();
    }
  }

  public boolean isHiveCatalogStoreRunning() {
    return isHiveCatalogStoreUse;
  }

  ////////////////////////////////////////////////////////
  // Tajo Cluster Section
  ////////////////////////////////////////////////////////
  private void startMiniTajoCluster(File testBuildDir,
                                    final int numSlaves,
                                    boolean local) throws Exception {
    TajoConf c = getConfiguration();
    c.setVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, "localhost:0");
    c.setVar(ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS, "localhost:0");
    c.setVar(ConfVars.RESOURCE_TRACKER_RPC_ADDRESS, "localhost:0");
    c.setVar(ConfVars.WORKER_PEER_RPC_ADDRESS, "localhost:0");
    c.setVar(ConfVars.WORKER_TEMPORAL_DIR, "file://" + testBuildDir.getAbsolutePath() + "/tajo-localdir");
    c.setVar(ConfVars.REST_SERVICE_ADDRESS, "localhost:0");

    if (!local) {
      String tajoRootDir = getMiniDFSCluster().getFileSystem().getUri().toString() + "/tajo";
      c.setVar(ConfVars.ROOT_DIR, tajoRootDir);

      URI defaultTsUri = TajoConf.getWarehouseDir(c).toUri();
      FileTablespace defaultTableSpace =
          new FileTablespace(TablespaceManager.DEFAULT_TABLESPACE_NAME, defaultTsUri, null);
      defaultTableSpace.init(conf);
      TablespaceManager.addTableSpaceForTest(defaultTableSpace);

    } else {
      c.setVar(ConfVars.ROOT_DIR, "file://" + testBuildDir.getAbsolutePath() + "/tajo");
    }

    setupCatalogForTesting(c, testBuildDir);

    LOG.info("derby repository is set to " + conf.get(CatalogConstants.CATALOG_URI));

    tajoMaster = new TajoMaster();
    tajoMaster.init(c);
    tajoMaster.start();

    this.conf.setVar(ConfVars.WORKER_PEER_RPC_ADDRESS, c.getVar(ConfVars.WORKER_PEER_RPC_ADDRESS));
    this.conf.setVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, c.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS));

    InetSocketAddress tajoMasterAddress = tajoMaster.getContext().getTajoMasterService().getBindAddress();

    this.conf.setVar(ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS, NetUtils.getHostPortString(tajoMasterAddress));
    this.conf.setVar(ConfVars.RESOURCE_TRACKER_RPC_ADDRESS, c.getVar(ConfVars.RESOURCE_TRACKER_RPC_ADDRESS));
    this.conf.setVar(ConfVars.CATALOG_ADDRESS, c.getVar(ConfVars.CATALOG_ADDRESS));
    
    InetSocketAddress tajoRestAddress = tajoMaster.getContext().getRestServer().getBindAddress();

    this.conf.setVar(ConfVars.REST_SERVICE_ADDRESS, NetUtils.getHostPortString(tajoRestAddress));

    startTajoWorkers(numSlaves);

    isTajoClusterRunning = true;
    LOG.info("Mini Tajo cluster is up");
    LOG.info("====================================================================================");
    LOG.info("=                           MiniTajoCluster starts up                              =");
    LOG.info("====================================================================================");
    LOG.info("= * Master Address: " + tajoMaster.getMasterName());
    LOG.info("= * CatalogStore: " + tajoMaster.getCatalogServer().getStoreClassName());
    LOG.info("------------------------------------------------------------------------------------");
    LOG.info("= * Warehouse Dir: " + TajoConf.getWarehouseDir(c));
    LOG.info("= * Worker Tmp Dir: " + c.getVar(ConfVars.WORKER_TEMPORAL_DIR));
    LOG.info("====================================================================================");
  }

  private void setupCatalogForTesting(TajoConf c, File testBuildDir) throws IOException, UnsupportedCatalogStore {
    final String HIVE_CATALOG_CLASS_NAME = "org.apache.tajo.catalog.store.HiveCatalogStore";
    boolean hiveCatalogClassExists = false;
    try {
      getClass().getClassLoader().loadClass(HIVE_CATALOG_CLASS_NAME);
      hiveCatalogClassExists = true;
    } catch (ClassNotFoundException e) {
      LOG.info("HiveCatalogStore is not available.");
    }
    String driverClass = System.getProperty(CatalogConstants.STORE_CLASS);

    if (hiveCatalogClassExists &&
        driverClass != null && driverClass.equals(HIVE_CATALOG_CLASS_NAME)) {
      try {
        getClass().getClassLoader().loadClass(HIVE_CATALOG_CLASS_NAME);
        String jdbcUri = "jdbc:derby:;databaseName="+ testBuildDir.toURI().getPath()  + "/metastore_db;create=true";
        c.set("hive.metastore.warehouse.dir", TajoConf.getWarehouseDir(c).toString() + "/default");
        c.set("javax.jdo.option.ConnectionURL", jdbcUri);
        c.set(TajoConf.ConfVars.WAREHOUSE_DIR.varname, conf.getVar(ConfVars.WAREHOUSE_DIR));
        c.set(CatalogConstants.STORE_CLASS, HIVE_CATALOG_CLASS_NAME);
        Path defaultDatabasePath = new Path(TajoConf.getWarehouseDir(c).toString() + "/default");
        FileSystem fs = defaultDatabasePath.getFileSystem(c);
        if (!fs.exists(defaultDatabasePath)) {
          fs.mkdirs(defaultDatabasePath);
        }
        isHiveCatalogStoreUse = true;
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
    } else { // for derby
      CatalogTestingUtil.configureCatalog(conf, testBuildDir.getAbsolutePath());
    }
    c.setVar(ConfVars.CATALOG_ADDRESS, "localhost:0");
  }

  private void startTajoWorkers(int numSlaves) throws Exception {
    for(int i = 0; i < 1; i++) {
      TajoWorker tajoWorker = new TajoWorker();

      TajoConf workerConf  = new TajoConf(this.conf);

      workerConf.setVar(ConfVars.WORKER_INFO_ADDRESS, "localhost:0");
      workerConf.setVar(ConfVars.WORKER_CLIENT_RPC_ADDRESS, "localhost:0");
      workerConf.setVar(ConfVars.WORKER_PEER_RPC_ADDRESS, "localhost:0");

      workerConf.setVar(ConfVars.WORKER_QM_RPC_ADDRESS, "localhost:0");
      
      tajoWorker.startWorker(workerConf, new String[0]);

      LOG.info("MiniTajoCluster Worker #" + (i + 1) + " started.");
      tajoWorkers.add(tajoWorker);
    }
  }

  public TajoMaster getMaster() {
    return this.tajoMaster;
  }

  public List<TajoWorker> getTajoWorkers() {
    return this.tajoWorkers;
  }

  public void shutdownMiniTajoCluster() {
    if(this.tajoMaster != null) {
      this.tajoMaster.stop();
    }
    for(TajoWorker eachWorker: tajoWorkers) {
      eachWorker.stopWorkerForce();
    }
    tajoWorkers.clear();
    this.tajoMaster= null;
  }

  ////////////////////////////////////////////////////////
  // Meta Cluster Section
  ////////////////////////////////////////////////////////
  /**
   * @throws java.io.IOException If a cluster -- dfs or engine -- already running.
   */
  void isRunningCluster() throws IOException {
    if (!isTajoClusterRunning && !isCatalogServerRunning && !isDFSRunning) return;
    throw new IOException("Cluster already running at " +
        this.clusterTestBuildDir);
  }

  /**
   * This method starts up a tajo cluster with a given number of clusters in
   * distributed mode.
   *
   * @param numSlaves the number of tajo cluster to start up
   * @throws Exception
   */
  public void startMiniCluster(final int numSlaves)
      throws Exception {
    startMiniCluster(numSlaves, null);
  }

  public void startMiniCluster(final int numSlaves, final String [] dataNodeHosts) throws Exception {

    int numDataNodes = numSlaves;
    if (dataNodeHosts != null && dataNodeHosts.length != 0) {
      numDataNodes = dataNodeHosts.length;
    }

    LOG.info("Starting up minicluster with 1 master(s) and " +
        numSlaves + " worker(s) and " + numDataNodes + " datanode(s)");

    // If we already bring up the cluster, fail.
    isRunningCluster();
    if (clusterTestBuildDir != null) {
      LOG.info("Using passed path: " + clusterTestBuildDir);
    }

    startMiniDFSCluster(numDataNodes, clusterTestBuildDir, dataNodeHosts);
    this.dfsCluster.waitClusterUp();
    hbaseUtil = new HBaseTestClusterUtil(conf, clusterTestBuildDir);

    startMiniTajoCluster(this.clusterTestBuildDir, numSlaves, false);
  }

  public void startMiniClusterInLocal(final int numSlaves) throws Exception {
    isRunningCluster();

    if (clusterTestBuildDir != null) {
      LOG.info("Using passed path: " + clusterTestBuildDir);
    }

    startMiniTajoCluster(this.clusterTestBuildDir, numSlaves, true);
  }

  public void shutdownMiniCluster() throws IOException {
    LOG.info("========================================");
    LOG.info("Minicluster is stopping");
    LOG.info("========================================");

    shutdownMiniTajoCluster();

    if(this.catalogServer != null) {
      shutdownCatalogCluster();
      isCatalogServerRunning = false;
    }

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    if(this.dfsCluster != null) {
      try {
        FileSystem fs = this.dfsCluster.getFileSystem();
        if (fs != null) fs.close();
        this.dfsCluster.shutdown();
      } catch (IOException e) {
        System.err.println("error closing file system: " + e);
      }
      isDFSRunning = false;
    }

    if(this.clusterTestBuildDir != null && this.clusterTestBuildDir.exists()) {
      if(!ShutdownHookManager.get().isShutdownInProgress()) {
        //TODO clean test dir when ShutdownInProgress
        LocalFileSystem localFS = LocalFileSystem.getLocal(conf);
        localFS.delete(new Path(clusterTestBuildDir.toString()), true);
        localFS.close();
      }
      this.clusterTestBuildDir = null;
    }

    if(hbaseUtil != null) {
      hbaseUtil.stopZooKeeperCluster();
      hbaseUtil.stopHBaseCluster();
    }

    LOG.info("Minicluster is down");
    isTajoClusterRunning = false;
  }

  public TajoClient newTajoClient() throws Exception {
    return new TajoClientImpl(ServiceTrackerFactory.get(getConfiguration()));
  }

  public static ResultSet run(String[] names,
                              Schema[] schemas,
                              String[][] tables,
                              String query,
                              TajoClient client) throws Exception {
    TajoTestingCluster util = TpchTestBase.getInstance().getTestingCluster();

    FileSystem fs = util.getDefaultFileSystem();
    Path rootDir = TajoConf.getWarehouseDir(util.getConfiguration());
    fs.mkdirs(rootDir);
    for (int i = 0; i < names.length; i++) {
      createTable(util.conf, names[i], schemas[i], tables[i]);
    }

    ResultSet res = client.executeQueryAndGetResult(query);
    return res;
  }

  public static ResultSet run(String[] names,
                              Schema[] schemas,
                              String[][] tables,
                              String query) throws Exception {
    TpchTestBase instance = TpchTestBase.getInstance();
    TajoTestingCluster util = instance.getTestingCluster();
    while(true) {
      if(util.getMaster().isMasterRunning()) {
        break;
      }
      Thread.sleep(1000);
    }
    TajoConf conf = util.getConfiguration();

    try (TajoClient client = new TajoClientImpl(ServiceTrackerFactory.get(conf))) {
      return run(names, schemas, tables, query, client);
    }
  }

  public static TajoClient newTajoClient(TajoTestingCluster util) throws SQLException, InterruptedException {
    while(true) {
      if(util.getMaster().isMasterRunning()) {
        break;
      }
      Thread.sleep(1000);
    }
    TajoConf conf = util.getConfiguration();
    return new TajoClientImpl(ServiceTrackerFactory.get(conf));
  }

  public static void createTable(TajoConf conf, String tableName, Schema schema,
                                 String[] tableDatas) throws Exception {
    createTable(conf, tableName, schema, tableDatas, 1);
  }

  public static void createTable(TajoConf conf, String tableName, Schema schema,
                                 String[] tableDatas, int numDataFiles) throws Exception {
    TpchTestBase instance = TpchTestBase.getInstance();
    TajoTestingCluster util = instance.getTestingCluster();
    try (TajoClient client = newTajoClient(util)) {
      FileSystem fs = util.getDefaultFileSystem();
      Path rootDir = TajoConf.getWarehouseDir(util.getConfiguration());
      if (!fs.exists(rootDir)) {
        fs.mkdirs(rootDir);
      }
      Path tablePath;
      if (IdentifierUtil.isFQTableName(tableName)) {
        Pair<String, String> name = IdentifierUtil.separateQualifierAndName(tableName);
        tablePath = new Path(rootDir, new Path(name.getFirst(), name.getSecond()));
      } else {
        tablePath = new Path(rootDir, tableName);
      }

      fs.mkdirs(tablePath);
      if (tableDatas.length > 0) {
        int recordPerFile = tableDatas.length / numDataFiles;
        if (recordPerFile == 0) {
          recordPerFile = 1;
        }
        FSDataOutputStream out = null;
        for (int j = 0; j < tableDatas.length; j++) {
          if (out == null || j % recordPerFile == 0) {
            if (out != null) {
              out.close();
            }
            Path dfsPath = new Path(tablePath, tableName + j + ".tbl");
            out = fs.create(dfsPath);
          }
          out.write((tableDatas[j] + "\n").getBytes());
        }
        if (out != null) {
          out.close();
        }
      }
      TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, conf);
      client.createExternalTable(tableName, schema, tablePath.toUri(), meta);
    }
  }

    /**
    * Write lines to a file.
    *
    * @param file File to write lines to
    * @param lines Strings written to the file
    * @throws java.io.IOException
    */
  private static void writeLines(File file, String... lines)
      throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      for (String line : lines) {
        writer.write(line);
        writer.write('\n');
      }
    } finally {
      Closeables.closeQuietly(writer);
    }
  }

  public void setAllTajoDaemonConfValue(String key, String value) {
    tajoMaster.getContext().getConf().set(key, value);
    setAllWorkersConfValue(key, value);
  }

  public void setAllWorkersConfValue(String key, String value) {
    for (TajoWorker eachWorker: tajoWorkers) {
      eachWorker.getConfig().set(key, value);
    }
  }

  public void waitForQuerySubmitted(QueryId queryId) throws Exception {
    waitForQuerySubmitted(queryId, 50);
  }

  public void waitForQuerySubmitted(QueryId queryId, int delay) throws Exception {
    QueryMasterTask qmt = null;

    int i = 0;
    while (qmt == null || TajoClientUtil.isQueryWaitingForSchedule(qmt.getState())) {
      try {
        Thread.sleep(delay);

        if (qmt == null) {
          qmt = getQueryMasterTask(queryId);
        }
      } catch (InterruptedException e) {
      }
      if (++i > 200) {
        throw new IOException("Timed out waiting for query to start");
      }
    }
  }

  public void waitForQueryState(Query query, TajoProtos.QueryState expected, int delay) throws Exception {
    int i = 0;
    while (query == null || query.getSynchronizedState() != expected) {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
      }
      if (++i > 200) {
        throw new IOException("Timed out waiting. expected: " + expected +
            ", actual: " + query != null ? String.valueOf(query.getSynchronizedState()) : String.valueOf(query));
      }
    }
  }

  public void waitForStageState(Stage stage, StageState expected, int delay) throws Exception {

    int i = 0;
    while (stage == null || stage.getSynchronizedState() != expected) {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
      }
      if (++i > 200) {
        throw new IOException("Timed out waiting");
      }
    }
  }

  public QueryMasterTask getQueryMasterTask(QueryId queryId) {
    QueryMasterTask qmt = null;
    for (TajoWorker worker : getTajoWorkers()) {
      qmt = worker.getWorkerContext().getQueryMaster().getQueryMasterTask(queryId, true);
      if (qmt != null && queryId.equals(qmt.getQueryId())) {
        break;
      }
    }
    return qmt;
  }

  public QueryHistory getQueryHistory(QueryId queryId) throws IOException {
    QueryHistory queryHistory = null;
    for (TajoWorker worker : getTajoWorkers()) {
      queryHistory = worker.getWorkerContext().getQueryMaster().getQueryHistory(queryId);
      if (queryHistory != null) {
        break;
      }
    }
    return queryHistory;
  }
}
