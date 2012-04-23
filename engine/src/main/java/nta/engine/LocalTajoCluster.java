package nta.engine;

import nta.conf.NtaConf;
import nta.engine.utils.JVMClusterUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class LocalTajoCluster {
	static final Log LOG = LogFactory.getLog(LocalTajoCluster.class);
	private JVMClusterUtil.MasterThread masterThread;
	private final List<JVMClusterUtil.LeafServerThread> leafThreads
	= new CopyOnWriteArrayList<JVMClusterUtil.LeafServerThread>();
	private final static int DEFAULT_NO = 1;
	private final Configuration conf;

	public LocalTajoCluster(final Configuration conf) throws Exception {
		this(conf, DEFAULT_NO);		
	}

	public LocalTajoCluster(final Configuration conf, final int numLeafServers) throws Exception {
		this.conf = conf;
		this.conf.set(NConstants.LEAFSERVER_PORT, "0");

		addMaster(conf, 0);

    Configuration c = null;
		for(int i=0; i < numLeafServers; i++) {
      c = new Configuration(conf);

      // if LocalTajoCluster is executed by NtaTestingUtility
      // each leaf server should have its own tmp directory.
      if (System.getProperty("test.build.data") != null) {
        String clusterTestBuildDir =
            System.getProperty("test.build.data");
        c.set(NConstants.WORKER_TMP_DIR,
            clusterTestBuildDir + "/worker_" + i + "/tmp");
      }
			addLeafServer(c, i);
		}
	}

	public JVMClusterUtil.MasterThread addMaster(Configuration c, final int index)
		throws Exception {
		// Create each master with its own Configuration instance so each has
		// its HConnection instance rather than share (see HBASE_INSTANCES down in
		// the guts of HConnectionManager.
		JVMClusterUtil.MasterThread mt =
			JVMClusterUtil.createMasterThread(c, index);
		this.masterThread = mt;
		return mt;
	}

	public JVMClusterUtil.LeafServerThread addLeafServer(
      Configuration c, final int index)
			throws IOException {
		// Create each regionserver with its own Configuration instance so each has
		// its HConnection instance rather than share (see HBASE_INSTANCES down in
		// the guts of HConnectionManager.
		JVMClusterUtil.LeafServerThread rst =
			JVMClusterUtil.createLeafServerThread(c, index);
		this.leafThreads.add(rst);
		return rst;
	}

	public LeafServer getLeafServer(int index) {
		return leafThreads.get(index).getLeafServer();
	}

	public List<JVMClusterUtil.LeafServerThread> getLeafServers() {
		return Collections.unmodifiableList(this.leafThreads);
	}
	
	public int getClusterSize() {
	  return this.leafThreads.size();
	}

	public List<JVMClusterUtil.LeafServerThread> getLiveLeafServers() {
		List<JVMClusterUtil.LeafServerThread> liveServers =
			new ArrayList<JVMClusterUtil.LeafServerThread>();
		List<JVMClusterUtil.LeafServerThread> list = getLeafServers();
		for(JVMClusterUtil.LeafServerThread lst: list) {
			if(lst.isAlive()) liveServers.add(lst);
		}

		return liveServers;
	}

	public NtaEngineMaster getMaster() {
		return this.masterThread.getMaster();
	}
	
	public String waitOnLeafServer(int index) {
		JVMClusterUtil.LeafServerThread leafServerThread =
			this.leafThreads.remove(index);
		while(leafServerThread.isAlive()) {
			try {
				LOG.info("Waiting on " +
					leafServerThread.getLeafServer().toString());
				leafServerThread.join();			
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return leafServerThread.getName();
	}
	
	public String waitOnLeafServer(JVMClusterUtil.LeafServerThread lst) {
		while(lst.isAlive()) {
			try {
				LOG.info("Waiting on " +
					lst.getLeafServer().toString());
				lst.join();			
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		for(int i=0; i<leafThreads.size(); i++) {
			if(leafThreads.get(i) == lst) {
				leafThreads.remove(i);
				break;
			}
		}
		
		return lst.getName();
	}
	
	public String waitOnMaster() {
		try {
			masterThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return masterThread.getName();
	}

	/**
	 * Wait for Mini HBase Cluster to shut down.
	 * Presumes you've already called {@link #shutdown()}.
	 */
	public void join() {
		if (this.leafThreads != null) {
			for(Thread t: this.leafThreads) {
				if (t.isAlive()) {
					try {
						t.join();
					} catch (InterruptedException e) {
						// continue
					}
				}
			}
		}

		if(masterThread.isAlive()) {
			try {
				masterThread.join();
			} catch (InterruptedException e) {
				// continue
			}
		}
	}
	
	public void startup() {
		JVMClusterUtil.startup(masterThread, leafThreads);
	}
	
	public void shutdown() {
		JVMClusterUtil.shutdown(masterThread, leafThreads);
	}
	
	/**
	 * @param c Configuration to check.
	 * @return True if a 'local' address in hbase.master value.
	 */
	public static boolean isLocal(final Configuration c) {
		final String mode = c.get(NConstants.CLUSTER_DISTRIBUTED); 
		return mode == null || mode.equals(NConstants.CLUSTER_IS_LOCAL);
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new NtaConf();
	    LocalTajoCluster cluster = new LocalTajoCluster(conf,2);
	    cluster.startup();
	    Thread.sleep(1000);
	    cluster.shutdown();
	}
}
