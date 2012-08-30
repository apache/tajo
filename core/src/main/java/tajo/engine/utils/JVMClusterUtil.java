package tajo.engine.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.conf.TajoConf;
import tajo.worker.LeafServer;
import tajo.master.TajoMaster;

import java.io.IOException;
import java.util.List;

public class JVMClusterUtil {
	private static final Log LOG = LogFactory.getLog(JVMClusterUtil.class);

	public static class LeafServerThread extends Thread {
		private final LeafServer leafServer;

		public LeafServerThread(final LeafServer s, final int index) {
			super(s, "LeafServer: "+index+";"+s.getServerName());
			this.leafServer = s;
		}

		public LeafServer getLeafServer() {
			return this.leafServer;
		}

		public void waitForServerOnline() {

			while(!this.leafServer.isOnline() &&
				!this.leafServer.isStopped()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// continue waiting
				}
			}
		}
	}

	public static JVMClusterUtil.LeafServerThread createLeafServerThread(
		final TajoConf c, final int index) throws IOException {
		LeafServer server;

		server = new LeafServer(c);

		return new JVMClusterUtil.LeafServerThread(server, index);
	}

	public static class MasterThread extends Thread {
		private final TajoMaster master;

		public MasterThread(final TajoMaster m, final int index) {
			super(m, "Master:" + index + ";" + m.getMasterServerName());
			this.master = m;
		}

		public TajoMaster getMaster() {
			return this.master;
		}

		public void waitForServerOnline() {
			while(!this.master.isMasterRunning()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// continue waiting
				}
			}
		}
	}

	public static JVMClusterUtil.MasterThread createMasterThread(
		final TajoConf c, final int index) throws Exception {
		TajoMaster server;

		server = new TajoMaster(c);

		return new JVMClusterUtil.MasterThread(server, index);
	}

	public static String startup(final JVMClusterUtil.MasterThread masters,
		final List<JVMClusterUtil.LeafServerThread> leafservers) {
    if (masters == null) {
			return null;
		}

		masters.start();	

		if(leafservers != null) {
			for(JVMClusterUtil.LeafServerThread t: leafservers) {
				t.start();
			}
		}

		while(true) {
			if(masters.master.isMasterRunning()) {
				return masters.master.getMasterServerName();
			}
		}
	}
	
	public static void shutdown(final MasterThread master, final List<LeafServerThread> leafServers) {
		LOG.debug("Shutting down NtaEngine Cluster");		
		
		for(LeafServerThread t: leafServers) {
			if(t.isAlive()) {
				try {
					t.getLeafServer().shutdown("Shutdown");
					t.join();
				} catch (InterruptedException e) {
					// continue
				}
			}
		}

    master.master.shutdown();

		LOG.info("Shutdown of "+
			((master != null) ? "1" : "0") + " master and "+
			((leafServers != null) ? leafServers.size() : "0") +
			" leafserver(s) complete");
	}
}