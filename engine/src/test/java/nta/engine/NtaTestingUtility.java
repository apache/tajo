/**
 * 
 */
package nta.engine;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import nta.catalog.CatalogServer;
import nta.catalog.MiniCatalogServer;
import nta.catalog.TConstants;
import nta.conf.NtaConf;
import nta.zookeeper.MiniZooKeeperCluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * @author Hyunsik Choi
 *
 */
public class NtaTestingUtility {
	private static Log LOG = LogFactory.getLog(NtaTestingUtility.class);
	private NtaConf conf;

	/**
	 * Set if we were passed a zkCluster.  If so, we won't shutdown zk as
	 * part of general shutdown.
	 */
	private boolean passedZkCluster = false;
	
	private FileSystem defaultFS = null;
	private MiniDFSCluster dfsCluster;
	private MiniZooKeeperCluster zkCluster = null;
	private MiniNtaEngineCluster engineCluster;
	private MiniCatalogServer catalogCluster;

	// If non-null, then already a cluster running.
	private File clusterTestBuildDir = null;

	/**
	 * System property key to get test directory value.
	 * Name is as it is because mini dfs has hard-codings to put test data here.
	 */
	public static final String TEST_DIRECTORY_KEY = "test.build.data";

	/**
	 * Default parent directory for test output.
	 */
	public static final String DEFAULT_TEST_DIRECTORY = "target/test-data";

	public NtaTestingUtility() {
		this.conf = new NtaConf();
	}

	public Configuration getConfiguration() {
		return this.conf;
	}
	
	public void initTestDir() {
		if (System.getProperty(TEST_DIRECTORY_KEY) == null) {
			clusterTestBuildDir = setupClusterTestBuildDir();
			System.setProperty(TEST_DIRECTORY_KEY, clusterTestBuildDir.getPath());
		}
	}

	/**
	 * @return Where to write test data on local filesystem; usually
	 * {@link #DEFAULT_TEST_DIRECTORY}
	 * @see #setupClusterTestBuildDir()
	 * @see #clusterTestBuildDir()
	 * @see #getTestFileSystem()
	 */
	public static Path getTestDir() {
		return new Path(System.getProperty(TEST_DIRECTORY_KEY,
			DEFAULT_TEST_DIRECTORY));
	}

	/**
	 * @param subdirName
	 * @return Path to a subdirectory named <code>subdirName</code> under
	 * {@link #getTestDir()}.
	 * @see #setupClusterTestBuildDir()
	 * @see #clusterTestBuildDir(String)
	 * @see #getTestFileSystem()
	 */
	public static Path getTestDir(final String subdirName) {
		return new Path(getTestDir(), subdirName);
	}

	public File setupClusterTestBuildDir() {
		String randomStr = UUID.randomUUID().toString();
		String dirStr = getTestDir(randomStr).toString();
		File dir = new File(dirStr).getAbsoluteFile();
		// Have it cleaned up on exit
		dir.deleteOnExit();
		return dir;
	}

	/**
	 * @throws IOException If a cluster -- dfs or engine -- already running.
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
	 * @return a mini tajo cluster
	 * @throws Exception
	 */
	public MiniNtaEngineCluster startMiniCluster(final int numSlaves) 
	    throws Exception {
		return startMiniCluster(numSlaves, null);
	}

	public MiniNtaEngineCluster startMiniCluster(final int numSlaves, 
	    final String [] dataNodeHosts) throws Exception {
	  Configuration c = getConfiguration();
	  // the conf is set to the distributed mode.
	  c.set(NConstants.CLUSTER_DISTRIBUTED, "true");
	  
		int numDataNodes = numSlaves;
		if(dataNodeHosts != null && dataNodeHosts.length != 0) {
			numDataNodes = dataNodeHosts.length;
		}

		LOG.info("Starting up minicluster with 1 master(s) and " +
			numSlaves + " leafserver(s) and " + numDataNodes + " datanode(s)");

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

		System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestBuildDir.getPath());

		startMiniDFSCluster(numDataNodes, this.clusterTestBuildDir, dataNodeHosts);
		this.dfsCluster.waitClusterUp();
		
		// Start up a zk cluster.
		if (this.zkCluster == null) {
		  startMiniZKCluster(this.clusterTestBuildDir);
		}
		// TODO: to be fixed
		/*
		if (this.catalogCluster == null) {
		  startCatalogCluster();
		}
		*/
		
		return startMiniTajoCluster(this.clusterTestBuildDir, numSlaves);
	}
	
	private MiniNtaEngineCluster startMiniTajoCluster(File testBuildDir,
	    final int numSlaves) throws Exception {
	  Configuration c = getConfiguration();
	  c.set(NConstants.MASTER_ADDRESS, "localhost:0");
	  c.set(NConstants.CATALOG_ADDRESS, "localhost:0");
	  conf.set(TConstants.JDBC_URI, 
        "jdbc:derby:"+clusterTestBuildDir.getAbsolutePath()+"/db");
    LOG.info("derby repository is set to "+conf.get(TConstants.JDBC_URI));
		c.set(NConstants.ENGINE_BASE_DIR, 
		    getMiniDFSCluster().getFileSystem().getUri()+"/tajo");
		c.set(NConstants.WORKER_BASE_DIR, 
		    clusterTestBuildDir.getAbsolutePath()+"/worker");
		c.set(NConstants.WORKER_TMP_DIR, 
        clusterTestBuildDir.getAbsolutePath()+"/worker/tmp");
		this.engineCluster = new MiniNtaEngineCluster(c, numSlaves);
		
		LOG.info("Mini Tajo cluster is up");
		return this.engineCluster;
	}
	
	public void restartNtaEngineCluster(int numSlaves) throws Exception {
	  this.engineCluster.shutdown();
		this.engineCluster = 
		    new MiniNtaEngineCluster(new Configuration(this.conf), numSlaves);
		
		LOG.info("Minicluster has been restarted");
	}
	
	public MiniNtaEngineCluster getMiniNtaEngineCluster() {
		return this.engineCluster;
	}
	
	public void shuwdownMiniNtaEngineCluster() throws IOException {
		if(this.engineCluster != null) {
			this.engineCluster.shutdown();
			this.engineCluster.join();
		}
		this.engineCluster = null;
	}
	
	public void flush() {
		this.engineCluster.flushcache();
	}
	
	public void flush(String tableName) {
		this.engineCluster.flushcache(tableName);
	}
	
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
	 * @throws IOException 
	 */
	public MiniDFSCluster startMiniDFSCluster(int servers, 
	    final File dir, final String hosts[]) throws IOException {
		// This does the following to home the minidfscluster
		// base_dir = new File(System.getProperty("test.build.data", "build/test/data"), "dfs/");
		// Some tests also do this:
		//  System.getProperty("test.cache.data", "build/test/cache");
		if (dir == null) {
			this.clusterTestBuildDir = setupClusterTestBuildDir();
		} else {
			this.clusterTestBuildDir = dir;
		}

		System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestBuildDir.toString());
		System.setProperty("test.cache.data", this.clusterTestBuildDir.toString());
		this.dfsCluster = new MiniDFSCluster(0, this.conf, servers, true, true,
			true, null, null, hosts, null);
		// Set this just-started cluser as our filesystem.
		this.defaultFS = this.dfsCluster.getFileSystem();
		this.conf.set("fs.defaultFS", defaultFS.getUri().toString());
		// Do old style too just to be safe.
		this.conf.set("fs.default.name", defaultFS.getUri().toString());		
		
		return this.dfsCluster;
	}
	
	/**
	 * Shuts down instance created by call to {@link #startMiniDFSCluster(int, File)}
	 * or does nothing.
	 * @throws Exception
	 */
	public void shutdownMiniDFSCluster() throws Exception {
		if (this.dfsCluster != null) {
			// The below throws an exception per dn, AsynchronousCloseException.
			this.dfsCluster.shutdown();
		}
	}
	
	public MiniZooKeeperCluster startMiniZKCluster() throws Exception {
		return startMiniZKCluster(1);
	}
	
	public MiniZooKeeperCluster startMiniZKCluster(int zookeeperServerNum) 
	    throws Exception {
		return startMiniZKCluster(setupClusterTestBuildDir(), zookeeperServerNum);
	}
	
	private MiniZooKeeperCluster startMiniZKCluster(final File dir) 
	    throws Exception {
		return startMiniZKCluster(dir, 1);
	}
	
	private MiniZooKeeperCluster startMiniZKCluster(final File dir,
		int zookeeperServerNum) throws Exception {
		this.passedZkCluster = false;
		if(this.zkCluster != null) {
			throw new IOException("Zoookeeper Cluster already running at "+dir);
		}
		this.zkCluster = new MiniZooKeeperCluster();
		int clientPort = this.zkCluster.startup(dir, zookeeperServerNum);
		this.conf.set(NConstants.ZOOKEEPER_ADDRESS, "127.0.0.1:"+clientPort);
		
		return this.zkCluster;
	}
	
	public MiniZooKeeperCluster getZKCluster() {
		return this.zkCluster;
	}
	
	public void shutdownMiniZKCluster() throws IOException {
		if(this.zkCluster != null) {
			this.zkCluster.shutdown();
			this.zkCluster = null;
		}
	}
	
	public boolean isRunningDFSCluster() {
		return this.defaultFS != null;
	}

	public MiniDFSCluster getMiniDFSCluster() {
		return this.dfsCluster;
	}
	
	public void shutdownMiniNtaEngineCluster() {
		if(engineCluster != null) {
			engineCluster.shutdown();
			
			this.engineCluster.join();
		}
	}
	
	public void shutdownMiniCluster() throws IOException {
		LOG.info("Shutting down minicluster");
		shutdownMiniNtaEngineCluster();
		
		if(!this.passedZkCluster) shutdownMiniZKCluster();
		if(this.dfsCluster != null) {
			this.dfsCluster.shutdown();
		}
		
		if(this.clusterTestBuildDir != null && this.clusterTestBuildDir.exists()) {
			LocalFileSystem localFS = LocalFileSystem.getLocal(conf);			
			localFS.delete(
				new Path(clusterTestBuildDir.toString()), true);
			this.clusterTestBuildDir = null;
		}
		if(this.catalogCluster != null) {
		  shutdownCatalogCluster();
		}
		
		LOG.info("Minicluster is down");
	}
	
	public MiniCatalogServer startCatalogCluster() throws Exception {
	  Configuration c = getConfiguration();
	  c.set(NConstants.CATALOG_ADDRESS, "localhost:0");
	  
	  if(clusterTestBuildDir == null) {
	    clusterTestBuildDir = setupClusterTestBuildDir();
	  }
	  String testDir = clusterTestBuildDir.getAbsolutePath();
	  
	  conf.set(TConstants.JDBC_URI, 
        "jdbc:derby:"+testDir+"/db");
    LOG.info("derby repository is set to "+conf.get(TConstants.JDBC_URI));
    
	  this.catalogCluster = new MiniCatalogServer(conf);
	  CatalogServer catServer = this.catalogCluster.getCatalogServer();
	  InetSocketAddress sockAddr = catServer.getBindAddress();
	  c.set(NConstants.CATALOG_ADDRESS, 
	      sockAddr.getHostName()+":"+sockAddr.getPort());
	  
	  return this.catalogCluster;
	}
	
	public void shutdownCatalogCluster() {
	  this.catalogCluster.shutdown();
	}
	
  public MiniCatalogServer getMiniCatalogCluster() {
    return this.catalogCluster;
  }

	/**
	 * @param args
	 * @throws Exception 
	 * @throws MetaException 
	 */
	public static void main(String[] args) throws Exception {
		NtaTestingUtility cluster = new NtaTestingUtility();
		cluster.startMiniCluster(4);
		
		Thread.sleep(3000);
	}
}
