/**
 * 
 */
package nta.engine;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import nta.conf.NtaConf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;

/**
 * @author hyunsik
 *
 */
public class NtaTestingUtility {
	private static Log LOG = LogFactory.getLog(NtaTestingUtility.class);
	private NtaConf conf;

	private FileSystem defaultFS = null;
	private MiniDFSCluster dfsCluster;
	private MiniMRCluster mrCluster;
	private MiniNtaEngineCluster engineCluster;

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
	
	public MiniNtaEngineCluster startMiniCluster(final int numSlaves) throws IOException {
		return startMiniCluster(numSlaves, null);
	}

	public MiniNtaEngineCluster startMiniCluster(final int numSlaves, final String [] dataNodeHosts) 
		throws IOException {
		int numDataNodes = numSlaves;
		if(dataNodeHosts != null && dataNodeHosts.length != 0) {
			numDataNodes = dataNodeHosts.length;
		}

		LOG.info("Starting up minicluster with 1 master(s) and " +
			numSlaves + " regionserver(s) and " + numDataNodes + " datanode(s)");

		// If we already put up a cluster, fail.
		String testBuildPath = conf.get(TEST_DIRECTORY_KEY, null);
		isRunningCluster(testBuildPath);
		if (testBuildPath != null) {
			LOG.info("Using passed path: " + testBuildPath);
		}

		// Make a new random dir to home everything in.  Set it as system property.
		// minidfs reads home from system property.
		this.clusterTestBuildDir = testBuildPath == null?setupClusterTestBuildDir() : new File(testBuildPath);

		System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestBuildDir.getPath());

		startMiniDFSCluster(numDataNodes, this.clusterTestBuildDir, dataNodeHosts);
		this.dfsCluster.waitClusterUp();
		
		return startMiniNtaEngineCluster(numSlaves);
	}
	
	public MiniNtaEngineCluster startMiniNtaEngineCluster(final int numSlaves) {
		Configuration c = new Configuration(this.conf);
		this.engineCluster = new MiniNtaEngineCluster(c, numSlaves);
		
		LOG.info("Minicluster is up");		
		return this.engineCluster;
	}
	
	public void restartNtaEngineCluster(int numSlaves) {
		this.engineCluster = new MiniNtaEngineCluster(new Configuration(this.conf), numSlaves);
		
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
	public MiniDFSCluster startMiniDFSCluster(int servers, final File dir, final String hosts[]) throws IOException {
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
	
	public boolean isRunningDFSCluster() {
		return this.defaultFS != null;
	}

	public MiniDFSCluster getMiniDFSCluster() {
		return this.dfsCluster;
	}

	/**
	 * Starts a <code>MiniMRCluster</code>.
	 *
	 * @param servers  The number of <code>TaskTracker</code>'s to start.
	 * @throws IOException When starting the cluster fails.
	 */
	public void startMiniMapReduceCluster(final int servers) throws IOException {		
		LOG.info("Starting mini mapreduce cluster...");		
		// These are needed for the new and improved Map/Reduce framework
		Configuration c = getConfiguration();
		
		c.set("hadoop.log.dir","target/test-data/logs");
		c.set("hadoop.tmp.dir","target/test-data/mapred");
		c.set("mapred.job.tracker", "localhost:21987");
		
		System.setProperty("hadoop.log.dir", c.get("hadoop.log.dir"));
		c.set("mapred.output.dir", c.get("hadoop.tmp.dir"));
		mrCluster = new MiniMRCluster(servers,
			FileSystem.get(c).getUri().toString(), 1);
		LOG.info("Mini mapreduce cluster started");
		c.set("mapred.job.tracker",
			mrCluster.createJobConf().get("mapred.job.tracker"));
	}

	/**
	 * Stops the previously started <code>MiniMRCluster</code>.
	 */
	public void shutdownMiniMapReduceCluster() {
		LOG.info("Stopping mini mapreduce cluster...");
		if (mrCluster != null) {
			mrCluster.shutdown();
		}
		// Restore configuration to point to local jobtracker
		conf.set("mapred.job.tracker", "local");
		LOG.info("Mini mapreduce cluster stopped");
	}

	/**
	 * @param args
	 * @throws Exception 
	 * @throws MetaException 
	 */
	public static void main(String[] args) throws Exception {
		NtaTestingUtility cluster = new NtaTestingUtility();
		cluster.startMiniCluster(2);		
	}
}
