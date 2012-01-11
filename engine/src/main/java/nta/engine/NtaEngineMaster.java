/**
 * 
 */
package nta.engine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import nta.conf.NtaConf;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;

/**
 * @author hyunsik
 *
 */
public class NtaEngineMaster extends Thread {
	private static final Log LOG = LogFactory.getLog(NtaEngineMaster.class);	

	private final Configuration conf;

	private final InetSocketAddress isa;

	private volatile boolean stopped = false;

	private final String serverName;
	private final ZkClient zkClient;
	ZkServer zkServer = null;

	public NtaEngineMaster(final Configuration conf) throws IOException {
		this.conf = conf;

		// Server to handle client requests.
		String hostname = DNS.getDefaultHost(
				conf.get("nta.master.dns.interface", "default"),
				conf.get("nta.master.dns.nameserver", "default"));
		int port = conf.getInt(NConstants.MASTER_PORT, NConstants.DEFAULT_MASTER_PORT);
		// Creation of a HSA will force a resolve.
		InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);

		this.isa = initialIsa;

		this.serverName = this.isa.getHostName()+":"+this.isa.getPort();

		if(conf.get(NConstants.ZOOKEEPER_HOST).equals("local")) {
			conf.set(NConstants.ZOOKEEPER_HOST, "localhost");
			this.zkServer = new ZkServer(conf);
			this.zkServer.start();
		}

		this.zkClient = new ZkClient(conf);
	}

	public void run() {
		LOG.info("NtaEngineMaster startup");
		try {
			becomeMaster();
			if(!this.stopped) {
				while(!this.stopped) {					
					Thread.sleep(1000);
				}
			}	
		} catch (Throwable t) {
			LOG.fatal("Unhandled exception. Starting shutdown.", t);
		} finally {
			// TODO - adds code to stop all services and clean resources 
		}

		LOG.info("NtaEngineMaster main thread exiting");
	}

	public void becomeMaster() throws IOException, KeeperException, InterruptedException {		
		zkClient.createPersistent(NConstants.ZNODE_BASE);		
		zkClient.createEphemeral(NConstants.ZNODE_MASTER, serverName.getBytes());		
		zkClient.createPersistent(NConstants.ZNODE_LEAFSERVERS);
		zkClient.createPersistent(NConstants.ZNODE_QUERIES);
	}

	public String getServerName() {
		return this.serverName;
	}	

	public boolean isMasterRunning() {
		return !this.stopped;
	}

	public void shutdown() {
		this.stopped = true;
	}
	
	List<String> getOnlineServer() throws KeeperException, InterruptedException {
		return zkClient.getChildren(NConstants.ZNODE_LEAFSERVERS);
	}

	public static void main(String [] args) throws IOException {
		NtaConf conf = new NtaConf();
		NtaEngineMaster master = new NtaEngineMaster(conf);

		master.start();
	}
}