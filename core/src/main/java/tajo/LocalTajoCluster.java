package tajo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.utils.JVMClusterUtil;
import tajo.master.TajoMaster;
import tajo.worker.LeafServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Hyunsik Choi
 */
public class LocalTajoCluster {
	static final Log LOG = LogFactory.getLog(LocalTajoCluster.class);
	private JVMClusterUtil.MasterThread masterThread;
	private final List<JVMClusterUtil.LeafServerThread> leafThreads
	  = new CopyOnWriteArrayList<JVMClusterUtil.LeafServerThread>();
	private final static int DEFAULT_NO = 2;
	private final TajoConf conf;

	public LocalTajoCluster(final TajoConf conf) throws Exception {
		this(conf, DEFAULT_NO);		
	}

	public LocalTajoCluster(final TajoConf conf, final int numLeafServers) throws Exception {
		this.conf = conf;
    // all workers ports are set to 0, leading to random port.
		this.conf.setIntVar(TajoConf.ConfVars.LEAFSERVER_PORT, 0);

		addMaster(conf, 0);

    TajoConf c;
		for(int i=0; i < numLeafServers; i++) {
      c = new TajoConf(conf);

      // TODO - if non-testing local cluster, how do worker's temporal directories created?

      // if LocalTajoCluster is executed by TajoTestingUtility
      // each leaf server should have its own tmp directory.
      if (System.getProperty("test.build.data") != null) {
        String clusterTestBuildDir =
            System.getProperty("test.build.data");
        c.setVar(ConfVars.WORKER_TMP_DIR,
            clusterTestBuildDir + "/worker_" + i + "/tmp");
      }
			addLeafServer(c, i);
		}
	}

	public JVMClusterUtil.MasterThread addMaster(TajoConf c, final int index)
		throws Exception {
		JVMClusterUtil.MasterThread mt =
			JVMClusterUtil.createMasterThread(c, index);
		this.masterThread = mt;
		return mt;
	}

	public JVMClusterUtil.LeafServerThread addLeafServer(
      TajoConf c, final int index)
			throws IOException {
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

	public TajoMaster getMaster() {
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
	 * Wait for workers to shut down.
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
	 * @return True if "nta.cluster.distributed" is false or null
	 */
	public static boolean isLocal(final TajoConf c) {
		return c.getBoolVar(ConfVars.CLUSTER_DISTRIBUTED);
	}
}
