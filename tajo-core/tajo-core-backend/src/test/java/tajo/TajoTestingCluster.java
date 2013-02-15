/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos;
import tajo.client.TajoClient;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.master.TajoMaster;
import tajo.util.NetUtils;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.sql.ResultSet;
import java.util.UUID;

public class TajoTestingCluster {
	private static Log LOG = LogFactory.getLog(TajoTestingCluster.class);
	private TajoConf conf;

  protected MiniTajoYarnCluster yarnCluster;
  private FileSystem defaultFS;
  private MiniDFSCluster dfsCluster;
	private MiniCatalogServer catalogServer;


  private TajoMaster tajoMaster;

	// If non-null, then already a cluster running.
	private File clusterTestBuildDir = null;

	/**
	 * System property key to get test directory value.
	 * Name is as it is because mini dfs has hard-codings to put test data here.
	 */
	public static final String TEST_DIRECTORY_KEY =
      MiniDFSCluster.PROP_TEST_BUILD_DATA;

	/**
	 * Default parent directory for test output.
	 */
	public static final String DEFAULT_TEST_DIRECTORY = "target/test-data";

	public TajoTestingCluster() {
		this.conf = new TajoConf();
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

    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.hosts(hosts);
    builder.numDataNodes(servers);
    builder.format(true);
    builder.manageNameDfsDirs(true);
    builder.manageDataDfsDirs(true);
    this.dfsCluster = builder.build();

    // Set this just-started cluser as our filesystem.
    this.defaultFS = this.dfsCluster.getFileSystem();
    this.conf.set("fs.defaultFS", defaultFS.getUri().toString());
    // Do old style too just to be safe.
    this.conf.set("fs.default.name", defaultFS.getUri().toString());

    return this.dfsCluster;
  }

  public void shutdownMiniDFSCluster() throws Exception {
    if (this.dfsCluster != null) {
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

    conf.set(CatalogConstants.STORE_CLASS, "tajo.catalog.store.MemStore");
    conf.set(CatalogConstants.JDBC_URI, "jdbc:derby:target/test-data/tcat/db");
    LOG.info("Apache Derby repository is set to "+conf.get(CatalogConstants.JDBC_URI));
    conf.setVar(ConfVars.CATALOG_ADDRESS, "localhost:0");

    catalogServer = new MiniCatalogServer(conf);
    CatalogServer catServer = catalogServer.getCatalogServer();
    InetSocketAddress sockAddr = catServer.getBindAddress();
    c.setVar(ConfVars.CATALOG_ADDRESS, NetUtils.getIpPortString(sockAddr));

    return this.catalogServer;
  }

  public void shutdownCatalogCluster() {
    this.catalogServer.shutdown();
  }

  public MiniCatalogServer getMiniCatalogCluster() {
    return this.catalogServer;
  }

  ////////////////////////////////////////////////////////
  // Tajo Cluster Section
  ////////////////////////////////////////////////////////
  private void startMiniTajoCluster(File testBuildDir,
                                               final int numSlaves,
                                               boolean local) throws Exception {
    TajoConf c = getConfiguration();
    c.setVar(ConfVars.TASKRUNNER_LISTENER_ADDRESS, "localhost:0");
    c.setVar(ConfVars.CLIENT_SERVICE_ADDRESS, "localhost:0");
    c.setVar(ConfVars.CATALOG_ADDRESS, "localhost:0");
    c.set(CatalogConstants.STORE_CLASS, "tajo.catalog.store.MemStore");
    c.set(CatalogConstants.JDBC_URI, "jdbc:derby:target/test-data/tcat/db");
    LOG.info("derby repository is set to "+conf.get(CatalogConstants.JDBC_URI));

    if (!local) {
      c.setVar(ConfVars.ROOT_DIR,
          getMiniDFSCluster().getFileSystem().getUri() + "/tajo");
    } else {
      c.setVar(ConfVars.ROOT_DIR,
          clusterTestBuildDir.getAbsolutePath() + "/tajo");
    }

    tajoMaster = new TajoMaster();
    tajoMaster.init(c);
    tajoMaster.start();

    this.conf.setVar(ConfVars.TASKRUNNER_LISTENER_ADDRESS, c.getVar(ConfVars.TASKRUNNER_LISTENER_ADDRESS));
    this.conf.setVar(ConfVars.CLIENT_SERVICE_ADDRESS, c.getVar(ConfVars.CLIENT_SERVICE_ADDRESS));
    this.conf.setVar(ConfVars.CATALOG_ADDRESS, c.getVar(ConfVars.CATALOG_ADDRESS));

    LOG.info("Mini Tajo cluster is up");
  }

  public void restartTajoCluster(int numSlaves) throws Exception {
    tajoMaster.stop();
    tajoMaster.start();

    LOG.info("Minicluster has been restarted");
  }

  public TajoMaster getMaster() {
    return this.tajoMaster;
  }

  public void shutdownMiniTajoCluster() {
    if(this.tajoMaster != null) {
      this.tajoMaster.stop();
    }
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
    String localHostName = InetAddress.getLocalHost().getHostName();
    startMiniCluster(numSlaves, new String[] {localHostName});
  }

  public void startMiniCluster(final int numSlaves,
                                          final String [] dataNodeHosts) throws Exception {
    // the conf is set to the distributed mode.
    this.conf.setBoolVar(ConfVars.CLUSTER_DISTRIBUTED, true);

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

    LOG.info("Starting up YARN cluster");
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 384);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 3000);
    if (yarnCluster == null) {
      yarnCluster = new MiniTajoYarnCluster(TajoTestingCluster.class.getName(), 3);
      yarnCluster.init(conf);
      yarnCluster.start();

      conf.set(YarnConfiguration.RM_ADDRESS,
          NetUtils.getIpPortString(yarnCluster.getResourceManager().
              getClientRMService().getBindAddress()));
      conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
          NetUtils.getIpPortString(yarnCluster.getResourceManager().
              getApplicationMasterService().getBindAddress()));

      URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
      if (url == null) {
        throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");
      }
      yarnCluster.getConfig().set("yarn.application.classpath", new File(url.getPath()).getParent());
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      yarnCluster.getConfig().writeXml(os);
      os.close();
    }

    startMiniTajoCluster(this.clusterTestBuildDir, numSlaves, false);
  }

  public void startMiniClusterInLocal(final int numSlaves) throws Exception {
    // the conf is set to the distributed mode.
    this.conf.setBoolVar(ConfVars.CLUSTER_DISTRIBUTED, true);

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
    LOG.info("Shutting down minicluster");
    shutdownMiniTajoCluster();

    if(this.catalogServer != null) {
      shutdownCatalogCluster();
    }

    if(this.dfsCluster != null) {
      this.dfsCluster.shutdown();
    }

    if(this.clusterTestBuildDir != null && this.clusterTestBuildDir.exists()) {
      LocalFileSystem localFS = LocalFileSystem.getLocal(conf);
      localFS.delete(
          new Path(clusterTestBuildDir.toString()), true);
      this.clusterTestBuildDir = null;
    }

    LOG.info("Minicluster is down");
  }

  public static ResultSet runInLocal(String[] tableNames,
                                     Schema[] schemas,
                                     Options option,
                                     String[][] tables,
                                     String query) throws Exception {
    TajoTestingCluster util = new TajoTestingCluster();
    util.startMiniClusterInLocal(1);
    TajoConf conf = util.getConfiguration();
    TajoClient client = new TajoClient(conf);

    File tmpDir = util.setupClusterTestBuildDir();
    for (int i = 0; i < tableNames.length; i++) {
      File tableDir = new File(tmpDir,tableNames[i]);
      tableDir.mkdirs();
      File dataDir = new File(tableDir, "data");
      dataDir.mkdirs();
      File tableFile = new File(dataDir, tableNames[i]);
      writeLines(tableFile, tables[i]);
      TableMeta meta = TCatUtil
          .newTableMeta(schemas[i], CatalogProtos.StoreType.CSV, option);
      client.createTable(tableNames[i], new Path(tableDir.getAbsolutePath()), meta);
    }
    Thread.sleep(1000);
    ResultSet res = client.executeQueryAndGetResult(query);
    util.shutdownMiniCluster();
    return res;
  }

  public static ResultSet run(String[] names,
                              String[] tablepaths,
                              Schema[] schemas,
                              Options option,
                              String query) throws Exception {
    TajoTestingCluster util = new TajoTestingCluster();
    util.startMiniCluster(1);
    TajoConf conf = util.getConfiguration();
    TajoClient client = new TajoClient(conf);

    FileSystem fs = util.getDefaultFileSystem();
    Path rootDir = util.getMaster().
        getStorageManager().getBaseDir();
    fs.mkdirs(rootDir);
    for (int i = 0; i < tablepaths.length; i++) {
      Path localPath = new Path(tablepaths[i]);
      Path tablePath = new Path(rootDir, names[i]);
      fs.mkdirs(tablePath);
      Path dataPath = new Path(tablePath, "data");
      fs.mkdirs(dataPath);
      Path dfsPath = new Path(dataPath, localPath.getName());
      fs.copyFromLocalFile(localPath, dfsPath);
      TableMeta meta = TCatUtil.newTableMeta(schemas[i],
          CatalogProtos.StoreType.CSV, option);
      client.createTable(names[i], tablePath, meta);
    }
    Thread.sleep(1000);
    ResultSet res = client.executeQueryAndGetResult(query);
    util.shutdownMiniCluster();
    return res;
  }

  public static ResultSet run(String[] names,
                              Schema[] schemas,
                              Options option,
                              String[][] tables,
                              String query) throws Exception {
    TajoTestingCluster util = new TajoTestingCluster();
    util.startMiniCluster(1);
    TajoConf conf = util.getConfiguration();
    TajoClient client = new TajoClient(conf);

    FileSystem fs = util.getDefaultFileSystem();
    Path rootDir = util.getMaster().
        getStorageManager().getBaseDir();
    fs.mkdirs(rootDir);
    for (int i = 0; i < names.length; i++) {
      Path tablePath = new Path(rootDir, names[i]);
      fs.mkdirs(tablePath);
      Path dataPath = new Path(tablePath, "data");
      fs.mkdirs(dataPath);
      Path dfsPath = new Path(dataPath, names[i] + ".tbl");
      FSDataOutputStream out = fs.create(dfsPath);
      for (int j = 0; j < tables[i].length; j++) {
        out.write((tables[i][j]+"\n").getBytes());
      }
      out.close();
      TableMeta meta = TCatUtil.newTableMeta(schemas[i],
          CatalogProtos.StoreType.CSV, option);
      client.createTable(names[i], tablePath, meta);
    }
    Thread.sleep(1000);
    ResultSet res = client.executeQueryAndGetResult(query);
    util.shutdownMiniCluster();
    return res;
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


	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		TajoTestingCluster cluster = new TajoTestingCluster();
    File f = cluster.setupClusterTestBuildDir();
    System.out.println("first setupClusterTestBuildDir: " + f);
    f = cluster.setupClusterTestBuildDir();
    System.out.println("second setupClusterTestBuildDir: " + f);
    f = cluster.getTestDir();
    System.out.println("getTestDir() after second: " + f);
    f = cluster.getTestDir("abc");
    System.out.println("getTestDir(\"abc\") after second: " + f);

    cluster.initTestDir();
    f = cluster.getTestDir();
    System.out.println("getTestDir() after initTestDir: " + f);
    f = cluster.getTestDir("abc");
    System.out.println("getTestDir(\"abc\") after initTestDir: " + f);
    f = cluster.setupClusterTestBuildDir();
    System.out.println("setupClusterTestBuildDir() after initTestDir: " + f);

    TajoTestingCluster cluster2 = new TajoTestingCluster();
    File f2 = cluster2.setupClusterTestBuildDir();
    System.out.println("first setupClusterTestBuildDir of cluster2: " + f2);
    /*
    String [] names = {"table1"};
    String [][] tables = new String[1][];
    tables[0] = new String[] {"a,b,c", "b,c,d"};

    Schema [] schemas = new Schema[1];
    schemas[0] = new Schema()
          .addColumn("f1", CatalogProtos.DataType.STRING)
          .addColumn("f2", CatalogProtos.DataType.STRING)
          .addColumn("f3", CatalogProtos.DataType.STRING);

    ResultSet res = runInLocal(names, schemas, tables, "select f1 from table1");
    res.next();
    System.out.println(res.getString(0));
    res.next();
    System.out.println(res.getString(0));
    System.exit(0);
    */
	}
}
