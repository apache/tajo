package nta.engine;

import java.io.IOException;
import java.util.List;

import nta.conf.NtaConf;
import nta.engine.utils.JVMClusterUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class MiniNtaEngineCluster {
	static final Log LOG = LogFactory.getLog(MiniNtaEngineCluster.class);
	private Configuration conf;
	public LocalNtaEngineCluster engineCluster;
	private static int index;
	
	public MiniNtaEngineCluster(Configuration conf, int numLeafServers) throws IOException {
		this.conf = conf;
		conf.set(NConstants.MASTER_PORT, "0");
		init(numLeafServers);
	}
	
	private void init(int numLeafServers) throws IOException {
		try {
		engineCluster = new LocalNtaEngineCluster(conf,numLeafServers);
		
		engineCluster.startup();
		} catch (IOException e) {
			shutdown();
			throw e;
		}
	}
	
	public JVMClusterUtil.LeafServerThread startLeafServer() throws IOException {
		final Configuration newConf = new NtaConf(conf);
		
		JVMClusterUtil.LeafServerThread t = null;
		
		t = engineCluster.addRegionServer(newConf,engineCluster.getLeafServers().size());
		t.start();
		t.waitForServerOnline();
		
		return t;
	}
	
	public String abortLeafServer(int index) {
		LeafServer server = getLeafServer(index);
		LOG.info("Aborting " + server.toString());
		server.abort("Aborting for tests", new Exception("Trace info"));
		return server.toString();
	}
	
	public JVMClusterUtil.LeafServerThread stopLeafServer(int index) {
		return stopLeafServer(index);
	}
	
	public JVMClusterUtil.LeafServerThread stopLeafServer(int index, final boolean shutdownFS) {
		JVMClusterUtil.LeafServerThread server = engineCluster.getLeafServers().get(index);
		LOG.info("Stopping " +  server.toString());
		server.getLeafServer().stop("Stopping ls " + index);
		return server;
	}
	
	public JVMClusterUtil.MasterThread startMaster() throws IOException {
		Configuration c = new NtaConf(conf); 
		
		JVMClusterUtil.MasterThread t = null;
		
		
		t = engineCluster.addMaster(c, 0);
		t.start();
		t.waitForServerOnline();
		
		return t;		
	}
	
	public NtaEngineMaster getMaster() {
		return this.engineCluster.getMaster();
	}
	
	public void join() {
		this.engineCluster.join();
	}
	
	public void shutdown() {
		if(this.engineCluster != null) {
			this.engineCluster.shutdown();
		}
	}
	
	public void flushcache() {
		// TODO - to be implemented, but it is necessary?
	}
	
	public void flushcache(String tableName) {
		// TODO - to be implemented, but it is necessary?
	}
	
	public List<JVMClusterUtil.LeafServerThread> getLeafServerThreads() {
		return this.engineCluster.getLeafServers();
	}
	
	public List<JVMClusterUtil.LeafServerThread> getLiveLeafServerThreads() {
		return this.engineCluster.getLiveLeafServers();
	}
	
	public LeafServer getLeafServer(int index) {
		return engineCluster.getLeafServer(index);
	}
	
	public List<LeafServer> getLeafServers(String tableName) {
		// TODO - to be implemented
		return null;
	}
}
