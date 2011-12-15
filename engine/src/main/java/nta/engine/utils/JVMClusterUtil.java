package nta.engine.utils;

import java.io.IOException;
import java.util.List;

import nta.engine.LeafServer;
import nta.engine.NtaEngineMaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

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
		final Configuration c, final int index) throws IOException {
		LeafServer server;

		server = new LeafServer(c);

		return new JVMClusterUtil.LeafServerThread(server, index);
	}

	public static class MasterThread extends Thread {
		private final NtaEngineMaster master;

		public MasterThread(final NtaEngineMaster m, final int index) {
			super(m, "Master:" + index + ";" + m.getServerName());
			this.master = m;
		}

		public NtaEngineMaster getMaster() {
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
		final Configuration c, final int index) throws IOException {
		NtaEngineMaster server;

		server = new NtaEngineMaster(c);

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
				return masters.master.getServerName();
			}
		}
	}
	
	public static void shutdown(final MasterThread master, final List<LeafServerThread> leafServers) {
		LOG.debug("Shutting down NtaEngine Cluster");
		
		
		for(Thread t: leafServers) {
			if(t.isAlive()) {
				try {
					t.join();
				} catch (InterruptedException e) {
					// continue
				}
			}
		}
		
		LOG.info("Shutdown of "+
			((master != null) ? "1" : "0") + " master and "+
			((leafServers != null) ? leafServers.size() : "0") +
			" leafserver(s) complete");
	}
}