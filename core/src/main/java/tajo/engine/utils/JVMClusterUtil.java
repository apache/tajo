package tajo.engine.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.conf.TajoConf;
import tajo.master.TajoMaster;
import tajo.worker.Worker;

import java.io.IOException;
import java.util.List;

public class JVMClusterUtil {
	private static final Log LOG = LogFactory.getLog(JVMClusterUtil.class);

	public static class WorkerThread extends Thread {
		private final Worker worker;

		public WorkerThread(final Worker s, final int index) {
			super(s, "Worker: "+index+";"+s.getServerName());
			this.worker = s;
		}

		public Worker getWorker() {
			return this.worker;
		}

		public void waitForServerOnline() {

			while(!this.worker.isOnline() &&
				!this.worker.isStopped()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// continue waiting
				}
			}
		}
	}

	public static WorkerThread createLeafServerThread(
		final TajoConf c, final int index) throws IOException {
		Worker server;

		server = new Worker(c);

		return new WorkerThread(server, index);
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
		final List<WorkerThread> leafservers) {
    if (masters == null) {
			return null;
		}

		masters.start();	

		if(leafservers != null) {
			for(WorkerThread t: leafservers) {
				t.start();
			}
		}

		while(true) {
			if(masters.master.isMasterRunning()) {
				return masters.master.getMasterServerName();
			}
		}
	}
	
	public static void shutdown(final MasterThread master, final List<WorkerThread> workers) {
		LOG.debug("Shutting down NtaEngine Cluster");		
		
		for(WorkerThread t: workers) {
			if(t.isAlive()) {
				try {
					t.getWorker().shutdown("Shutdown");
					t.join();
				} catch (InterruptedException e) {
					// continue
				}
			}
		}

    master.master.shutdown();

		LOG.info("Shutdown of "+
			((master != null) ? "1" : "0") + " master and "+
			((workers != null) ? workers.size() : "0") +
			" leafserver(s) complete");
	}
}