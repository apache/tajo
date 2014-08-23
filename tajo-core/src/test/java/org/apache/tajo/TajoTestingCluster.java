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
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.rm.TajoWorkerResourceManager;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.worker.TajoWorker;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URL;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TajoTestingCluster {
	private static Log LOG = LogFactory.getLog(TajoTestingCluster.class);
	private TajoConf conf;

  protected MiniTajoYarnCluster yarnCluster;
  private FileSystem defaultFS;
  private MiniDFSCluster dfsCluster;
	private MiniCatalogServer catalogServer;

  private TajoMaster tajoMaster;
  private List<TajoWorker> tajoWorkers = new ArrayList<TajoWorker>();
  private boolean standbyWorkerMode = false;

	// If non-null, then already a cluster running.
	private File clusterTestBuildDir = null;

	/**
	 * System property key to get test directory value.
	 * Name is as it is because mini dfs has hard-codings to put test data here.
	 */
	public static final String TEST_DIRECTORY_KEY = MiniDFSCluster.PROP_TEST_BUILD_DATA;

	/**
	 * Default parent directory for test output.
	 */
	public static final String DEFAULT_TEST_DIRECTORY = "target/test-data";

  /**
   * True If HCatalogStore is used. Otherwise, it is FALSE.
   */
  public Boolean isHCatalogStoreUse = false;

  public TajoTestingCluster() {
    this(false);
  }

  public TajoTestingCluster(boolean masterHaEMode) {
    this.conf = new TajoConf();
    this.conf.setBoolVar(ConfVars.TAJO_MASTER_HA_ENABLE, masterHaEMode);

    setTestingFlagProperties();
    initPropertiesAndConfigs();
  }

  void setTestingFlagProperties() {
    System.setProperty(CommonTestingUtil.TAJO_TEST_KEY, CommonTestingUtil.TAJO_TEST_TRUE);
    conf.set(CommonTestingUtil.TAJO_TEST_KEY, CommonTestingUtil.TAJO_TEST_TRUE);
  }

  void initPropertiesAndConfigs() {
    if (System.getProperty(ConfVars.RESOURCE_MANAGER_CLASS.varname) != null) {
      String testResourceManager = System.getProperty(ConfVars.RESOURCE_MANAGER_CLASS.varname);
      Preconditions.checkState(testResourceManager.equals(TajoWorkerResourceManager.class.getCanonicalName()));
      conf.set(ConfVars.RESOURCE_MANAGER_CLASS.varname, System.getProperty(ConfVars.RESOURCE_MANAGER_CLASS.varname));
    }
    conf.setInt(ConfVars.WORKER_RESOURCE_AVAILABLE_MEMORY_MB.varname, 1024);
    conf.setFloat(ConfVars.WORKER_RESOURCE_AVAILABLE_DISKS.varname, 2.0f);

    this.standbyWorkerMode = conf.getVar(ConfVars.RESOURCE_MANAGER_CLASS)
        .indexOf(TajoWorkerResourceManager.class.getName()) >= 0;
  }

	public TajoConf getConfiguration() {
		return this.conf;
	}

	public void initTestDir() {
		if (System.getProperty(TEST_DIRECTORY_KEY) == null) {
			clusterTestBuildDir = setupClusterTestBuildDir();
			System.setProperty(TEST_DIRECTORY_KEY,
          clusterTestBuildDir.getAbsolutePath());
		}
	}

	/**
	 * @return Where to write test data on local filesystem; usually
	 * {@link #DEFAULT_TEST_DIRECTORY}
	 * @see #setupClusterTestBuildDir()
	 */
	public static File getTestDir() {
		return new File(System.getProperty(TEST_DIRECTORY_KEY,
			DEFAULT_TEST_DIRECTORY));
	}

	/**
	 * @param subdirName
	 * @return Path to a subdirectory named <code>subdirName</code> under
	 * {@link #getTestDir()}.
	 * @see #setupClusterTestBuildDir()
	 */
	public static File getTestDir(final String subdirName) {
		return new File(getTestDir(), subdirName);
  }

	public File setupClusterTestBuildDir() {
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
                                            final File dir,
                                            final String hosts[])
      throws IOException {
    if (dir == null) {
      this.clusterTestBuildDir = setupClusterTestBuildDir();
    } else {
      this.clusterTestBuildDir = dir;
    }

    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA,
        this.clusterTestBuildDir.toString());

    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, false);
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(new HdfsConfiguration(conf));
    builder.hosts(hosts);
    builder.numDataNodes(servers);
    builder.format(true);
    builder.manageNameDfsDirs(true);
    builder.manageDataDfsDirs(true);
    builder.waitSafeMode(true);
    this.dfsCluster = builder.build();

    // Set this just-started cluser as our filesystem.
    this.defaultFS = this.dfsCluster.getFileSystem();
    this.conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultFS.getUri().toString());
    this.conf.setVar(TajoConf.ConfVars.ROOT_DIR, defaultFS.getUri() + "/tajo");

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

  ////////////////////////////////////////////////////////
  // Catalog Section
  ////////////////////////////////////////////////////////
  public MiniCatalogServer startCatalogCluster() throws Exception {
    TajoConf c = getConfiguration();

    if(clusterTestBuildDir == null) {
      clusterTestBuildDir = setupClusterTestBuildDir();
    }

    conf.set(CatalogConstants.STORE_CLASS, "org.apache.tajo.catalog.store.MemStore");
    conf.set(CatalogConstants.CATALOG_URI, "jdbc:derby:" + clusterTestBuildDir.getAbsolutePath() + "/db");
    LOG.info("Apache Derby repository is set to "+conf.get(CatalogConstants.CATALOG_URI));
    conf.setVar(ConfVars.CATALOG_ADDRESS, "localhost:0");

    catalogServer = new MiniCatalogServer(conf);
    CatalogServer catServer = catalogServer.getCatalogServer();
    InetSocketAddress sockAddr = catServer.getBindAddress();
    c.setVar(ConfVars.CATALOG_ADDRESS, NetUtils.normalizeInetSocketAddress(sockAddr));

    return this.catalogServer;
  }

  public void shutdownCatalogCluster() {
    if (catalogServer != null) {
      this.catalogServer.shutdown();
    }
  }

  public MiniCatalogServer getMiniCatalogCluster() {
    return this.catalogServer;
  }

  public boolean isHCatalogStoreRunning() {
    return isHCatalogStoreUse;
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

    LOG.info("derby repository is set to "+conf.get(CatalogConstants.CATALOG_URI));

    if (!local) {
      c.setVar(ConfVars.ROOT_DIR,
          getMiniDFSCluster().getFileSystem().getUri() + "/tajo");
    } else {
      c.setVar(ConfVars.ROOT_DIR, clusterTestBuildDir.getAbsolutePath() + "/tajo");
    }

    setupCatalogForTesting(c, clusterTestBuildDir);

    tajoMaster = new TajoMaster();
    tajoMaster.init(c);
    tajoMaster.start();

    this.conf.setVar(ConfVars.WORKER_PEER_RPC_ADDRESS, c.getVar(ConfVars.WORKER_PEER_RPC_ADDRESS));
    this.conf.setVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, c.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS));

    InetSocketAddress tajoMasterAddress = tajoMaster.getContext().getTajoMasterService().getBindAddress();

    this.conf.setVar(ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS,
        tajoMasterAddress.getHostName() + ":" + tajoMasterAddress.getPort());
    this.conf.setVar(ConfVars.RESOURCE_TRACKER_RPC_ADDRESS, c.getVar(ConfVars.RESOURCE_TRACKER_RPC_ADDRESS));
    this.conf.setVar(ConfVars.CATALOG_ADDRESS, c.getVar(ConfVars.CATALOG_ADDRESS));

    if(standbyWorkerMode) {
      startTajoWorkers(numSlaves);
    }
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

  private void setupCatalogForTesting(TajoConf c, File testBuildDir) throws IOException {
    final String HCATALOG_CLASS_NAME = "org.apache.tajo.catalog.store.HCatalogStore";
    boolean hcatalogClassExists = false;
    try {
      getClass().getClassLoader().loadClass(HCATALOG_CLASS_NAME);
      hcatalogClassExists = true;
    } catch (ClassNotFoundException e) {
      LOG.info("HCatalogStore is not available.");
    }
    String driverClass = System.getProperty(CatalogConstants.STORE_CLASS);

    if (hcatalogClassExists &&
        driverClass != null && driverClass.equals(HCATALOG_CLASS_NAME)) {
      try {
        getClass().getClassLoader().loadClass(HCATALOG_CLASS_NAME);
        String jdbcUri = "jdbc:derby:;databaseName="+ testBuildDir.toURI().getPath()  + "/metastore_db;create=true";
        c.set("hive.metastore.warehouse.dir", TajoConf.getWarehouseDir(c).toString() + "/default");
        c.set("javax.jdo.option.ConnectionURL", jdbcUri);
        c.set(TajoConf.ConfVars.WAREHOUSE_DIR.varname, conf.getVar(ConfVars.WAREHOUSE_DIR));
        c.set(CatalogConstants.STORE_CLASS, HCATALOG_CLASS_NAME);
        Path defaultDatabasePath = new Path(TajoConf.getWarehouseDir(c).toString() + "/default");
        FileSystem fs = defaultDatabasePath.getFileSystem(c);
        if (!fs.exists(defaultDatabasePath)) {
          fs.mkdirs(defaultDatabasePath);
        }
        isHCatalogStoreUse = true;
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
    } else { // for derby
      c.set(CatalogConstants.STORE_CLASS, "org.apache.tajo.catalog.store.MemStore");
      c.set(CatalogConstants.CATALOG_URI, "jdbc:derby:" + testBuildDir.getAbsolutePath() + "/db");
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
      
      tajoWorker.startWorker(workerConf, new String[]{"standby"});

      LOG.info("MiniTajoCluster Worker #" + (i + 1) + " started.");
      tajoWorkers.add(tajoWorker);
    }
  }

  public void restartTajoCluster(int numSlaves) throws Exception {
    tajoMaster.stop();
    tajoMaster.start();

    LOG.info("Minicluster has been restarted");
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
  void isRunningCluster(String passedBuildPath) throws IOException {
    if (this.clusterTestBuildDir == null || passedBuildPath != null) return;
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
    if(dataNodeHosts != null && dataNodeHosts.length != 0) {
      numDataNodes = dataNodeHosts.length;
    }

    LOG.info("Starting up minicluster with 1 master(s) and " +
        numSlaves + " worker(s) and " + numDataNodes + " datanode(s)");

    // If we already put up a cluster, fail.
    String testBuildPath = conf.get(TEST_DIRECTORY_KEY, null);
    isRunningCluster(testBuildPath);
    if (testBuildPath != null) {
      LOG.info("Using passed path: " + testBuildPath);
    }

    // Make a new random dir to home everything in.  Set it as system property.
    // minidfs reads home from system property.
    this.clusterTestBuildDir = testBuildPath == null?
        setupClusterTestBuildDir() : new File(testBuildPath);

    System.setProperty(TEST_DIRECTORY_KEY,
        this.clusterTestBuildDir.getAbsolutePath());

    startMiniDFSCluster(numDataNodes, this.clusterTestBuildDir, dataNodeHosts);
    this.dfsCluster.waitClusterUp();

    if(!standbyWorkerMode) {
      startMiniYarnCluster();
    }

    startMiniTajoCluster(this.clusterTestBuildDir, numSlaves, false);
  }

  private void startMiniYarnCluster() throws Exception {
    LOG.info("Starting up YARN cluster");
    // Scheduler properties required for YARN to work
    conf.set("yarn.scheduler.capacity.root.queues", "default");
    conf.set("yarn.scheduler.capacity.root.default.capacity", "100");

    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 384);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 3000);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 1);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 2);

    if (yarnCluster == null) {
      yarnCluster = new MiniTajoYarnCluster(TajoTestingCluster.class.getName(), 3);
      yarnCluster.init(conf);
      yarnCluster.start();

      ResourceManager resourceManager = yarnCluster.getResourceManager();
      InetSocketAddress rmAddr = resourceManager.getClientRMService().getBindAddress();
      InetSocketAddress rmSchedulerAddr = resourceManager.getApplicationMasterService().getBindAddress();
      conf.set(YarnConfiguration.RM_ADDRESS, NetUtils.normalizeInetSocketAddress(rmAddr));
      conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, NetUtils.normalizeInetSocketAddress(rmSchedulerAddr));

      URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
      if (url == null) {
        throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");
      }
      yarnCluster.getConfig().set("yarn.application.classpath", new File(url.getPath()).getParent());
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      yarnCluster.getConfig().writeXml(os);
      os.close();
    }
  }

  public void startMiniClusterInLocal(final int numSlaves) throws Exception {
    // If we already put up a cluster, fail.
    String testBuildPath = conf.get(TEST_DIRECTORY_KEY, null);
    isRunningCluster(testBuildPath);
    if (testBuildPath != null) {
      LOG.info("Using passed path: " + testBuildPath);
    }

    // Make a new random dir to home everything in.  Set it as system property.
    // minidfs reads home from system property.
    this.clusterTestBuildDir = testBuildPath == null?
        setupClusterTestBuildDir() : new File(testBuildPath);

    System.setProperty(TEST_DIRECTORY_KEY,
        this.clusterTestBuildDir.getAbsolutePath());

    startMiniTajoCluster(this.clusterTestBuildDir, numSlaves, true);
  }

  public void shutdownMiniCluster() throws IOException {
    LOG.info("========================================");
    LOG.info("Minicluster is stopping");
    LOG.info("========================================");

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    shutdownMiniTajoCluster();

    if(this.catalogServer != null) {
      shutdownCatalogCluster();
    }

    if(this.yarnCluster != null) {
      this.yarnCluster.stop();
    }

    try {
      Thread.sleep(3000);
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
    LOG.info("Minicluster is down");
  }

  public static ResultSet run(String[] names,
                              Schema[] schemas,
                              KeyValueSet tableOption,
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
    TajoClient client = new TajoClient(conf);
    try {
      FileSystem fs = util.getDefaultFileSystem();
      Path rootDir = util.getMaster().
          getStorageManager().getWarehouseDir();
      fs.mkdirs(rootDir);
      for (int i = 0; i < names.length; i++) {
        createTable(names[i], schemas[i], tableOption, tables[i]);
      }
      Thread.sleep(1000);
      ResultSet res = client.executeQueryAndGetResult(query);
      return res;
    } finally {
      client.close();
    }
  }

  public static void createTable(String tableName, Schema schema,
                                 KeyValueSet tableOption, String[] tableDatas) throws Exception {
    createTable(tableName, schema, tableOption, tableDatas, 1);
  }

  public static void createTable(String tableName, Schema schema,
                                 KeyValueSet tableOption, String[] tableDatas, int numDataFiles) throws Exception {
    TpchTestBase instance = TpchTestBase.getInstance();
    TajoTestingCluster util = instance.getTestingCluster();
    while(true) {
      if(util.getMaster().isMasterRunning()) {
        break;
      }
      Thread.sleep(1000);
    }
    TajoConf conf = util.getConfiguration();
    TajoClient client = new TajoClient(conf);
    try {
      FileSystem fs = util.getDefaultFileSystem();
      Path rootDir = util.getMaster().
          getStorageManager().getWarehouseDir();
      if (!fs.exists(rootDir)) {
        fs.mkdirs(rootDir);
      }
      Path tablePath = new Path(rootDir, tableName);
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
      TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV, tableOption);
      client.createExternalTable(tableName, schema, tablePath, meta);
    } finally {
      client.close();
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

  public void waitForQueryRunning(QueryId queryId) throws Exception {
    QueryMasterTask qmt = null;

    int i = 0;
    while (qmt == null || TajoClient.isInPreNewState(qmt.getState())) {
      try {
        Thread.sleep(100);
        if(qmt == null){
          qmt = getQueryMasterTask(queryId);
        }
      } catch (InterruptedException e) {
      }
      if (++i > 100) {
        throw new IOException("Timed out waiting for query to start");
      }
    }
  }

  public QueryMasterTask getQueryMasterTask(QueryId queryId) {
    QueryMasterTask qmt = null;
    for (TajoWorker worker : getTajoWorkers()) {
      qmt = worker.getWorkerContext().getQueryMaster().getQueryMasterTask(queryId);
      if (qmt != null) {
        break;
      }
    }

    return qmt;
  }
}
