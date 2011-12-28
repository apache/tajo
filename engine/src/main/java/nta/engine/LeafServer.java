/**
 * 
 */
package nta.engine;

import java.io.IOException;
import java.net.InetSocketAddress;

import nta.conf.NtaConf;
import nta.engine.cluster.MasterAddressTracker;
import nta.engine.ipc.LeafServerInterface;
import nta.engine.ipc.protocolrecords.AssignTabletRequest;
import nta.engine.ipc.protocolrecords.ReleaseTableRequest;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.ipc.protocolrecords.SubQueryResponse;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;

/**
 * @author hyunsik
 *
 */
public class LeafServer extends Thread implements LeafServerInterface {
	private static final Log LOG = LogFactory.getLog(LeafServer.class);	

	private final Configuration conf;

	private final Server rpcServer;
	private final InetSocketAddress isa;

	private volatile boolean stopped = false;
	private volatile boolean isOnline = false;

	private final String serverName;
	
	private ZkClient zookeeper;
	private MasterAddressTracker masterAddrTracker;

	public LeafServer(final Configuration conf) throws IOException {
		this.conf = conf;

		// Server to handle client requests.
		String hostname = DNS.getDefaultHost(
			conf.get("nta.master.dns.interface", "default"),
			conf.get("nta.master.dns.nameserver", "default"));
		int port = conf.getInt(NConstants.LEAFSERVER_PORT, NConstants.DEFAULT_LEAFSERVER_PORT);
		// Creation of a HSA will force a resolve.
		InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
		if (initialIsa.getAddress() == null) {
			throw new IllegalArgumentException("Failed resolve of " + this.isa);
		}
		int numHandlers = conf.getInt("nta.master.handler.count",
			conf.getInt("nta.leafserver.handler.count", 25));
		this.rpcServer = RPC.getServer(this,
			initialIsa.getHostName(), // BindAddress is IP we got for this server.
			initialIsa.getPort(),		        
			numHandlers,
			conf.getBoolean("nta.rpc.verbose", false), 
			conf);

		// Set our address.
	    this.isa = this.rpcServer.getListenerAddress();
		this.serverName = this.isa.getHostName()+":"+this.isa.getPort();
	}

	public void run() {
		LOG.info("NtaLeafServer startup");
		
		try {
			initializeZookeeper();
			
			if(!this.stopped) {
				this.isOnline = true;
				while(!this.stopped) {					
					Thread.sleep(1000);

				}
			}	
		} catch (Throwable t) {
			LOG.fatal("Unhandled exception. Starting shutdown.", t);
		} finally {
			// TODO - adds code to stop all services and clean resources 
		}

		LOG.info("NtaLeafServer main thread exiting");
	}
	
	private void initializeZookeeper() throws IOException, InterruptedException, KeeperException {
		this.zookeeper = new ZkClient(conf);
		this.masterAddrTracker = new MasterAddressTracker(zookeeper);
		do {
			Thread.sleep(200);
		} while(zookeeper.exists(NConstants.ZNODE_LEAFSERVERS) == null);
			zookeeper.createEphemeral(
				ZkUtil.concat(NConstants.ZNODE_LEAFSERVERS, serverName));			
	}
	
	public String getServerName() {
		return this.serverName;
	}
	
	/**
	 * @return true if a stop has been requested.
	 */
	public boolean isStopped() {
		return this.stopped;
	}
	
	public boolean isOnline() {
		return this.isOnline;
	}

	//////////////////////////////////////////////////////////////////////////////
	// LeafServerInterface
	//////////////////////////////////////////////////////////////////////////////
	@Override
	public SubQueryResponse dissminateQuery(SubQueryRequest request) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void assignTablets(AssignTabletRequest request) {
		
	}

	@Override
	public void releaseTablets(ReleaseTableRequest request) {
		// TODO Auto-generated method stub

	}
	
	public void stop(final String msg) {
		this.stopped = true;
		LOG.info("STOPPED: "+msg);
		synchronized (this) {
			notifyAll();
		}
	}
	
	public void abort(String reason, Throwable cause) {
		if(cause != null) {
			LOG.fatal("ABORTING leaf server " + this + ": " + reason, cause);			
		} else {
			LOG.fatal("ABORTING leaf server " + this +": " + reason);
		}
		// TODO - abortRequest : to be implemented
		stop(reason);
	}

	public static void main(String [] args) throws IOException {
		NtaConf conf = new NtaConf();
		LeafServer leafServer = new LeafServer(conf);

		leafServer.start();
	}
}