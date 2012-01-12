/**
 * 
 */
package nta.engine;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import nta.catalog.Catalog;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.conf.NtaConf;
import nta.engine.cluster.MasterAddressTracker;
import nta.engine.ipc.LeafServerInterface;
import nta.engine.ipc.protocolrecords.AssignTabletRequest;
import nta.engine.ipc.protocolrecords.ReleaseTableRequest;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.ipc.protocolrecords.SubQueryResponse;
import nta.engine.ipc.protocolrecords.Tablet;
import nta.engine.query.QueryEngine;
import nta.storage.StorageManager;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
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

	// Server States
	/**
	 * This servers address.
	 */	
	private final Server rpcServer;
	private final InetSocketAddress isa;

	private volatile boolean stopped = false;
	private volatile boolean isOnline = false;

	private final String serverName;
	
	// Cluster Management
	private ZkClient zookeeper;
	private MasterAddressTracker masterAddrTracker;
	
	// Query Processing
	private FileSystem defaultFS;
	
	private Catalog catalog;
	private StorageManager storeManager;
	private QueryEngine queryEngine;
	private List<EngineService> services = new ArrayList<EngineService>();
	
	private final Path basePath;
	private final Path catalogPath;
	private final Path dataPath;

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
		
		
		// FileSystem initialization
		String master = conf.get(NConstants.MASTER_HOST,"local");
		if("local".equals(master)) {
			// local mode
			this.defaultFS = LocalFileSystem.get(conf);
			LOG.info("LocalFileSystem is initialized.");
		} else {
			// remote mode
			this.defaultFS = FileSystem.get(conf);	
			LOG.info("FileSystem is initialized.");
		}
		
		this.basePath = new Path(conf.get(NConstants.ENGINE_BASE_DIR));
		LOG.info("Base dir is set " + conf.get(NConstants.ENGINE_BASE_DIR));
		File baseDir = new File(this.basePath.toString());
		if(baseDir.exists() == false) {
			baseDir.mkdir();
			LOG.info("Base dir ("+baseDir.getAbsolutePath()+") is created.");
		}
		
		this.dataPath = new Path(conf.get(NConstants.ENGINE_DATA_DIR));
		LOG.info("Data dir is set " + dataPath);		
		if(!defaultFS.exists(dataPath)) {
			defaultFS.mkdirs(dataPath);
			LOG.info("Data dir ("+dataPath+") is created");
		}		
		this.storeManager = new StorageManager(conf);
		
		
		this.catalogPath = new Path(conf.get(NConstants.ENGINE_CATALOG_DIR));
		LOG.info("Catalog dir is set to " + this.catalogPath);
		File catalogDir = new File(this.catalogPath.toString());	
		if(catalogDir.exists() == false) {
			catalogDir.mkdir();
			LOG.info("Catalog dir ("+catalogDir.getAbsolutePath()+") is created.");
		}
		this.catalog = new Catalog(conf);
		this.catalog.init();
		//File catalogFile = new File(catalogPath+"/"+NConstants.ENGINE_CATALOG_FILENAME);
//		if(catalogFile.exists())		
//			loadCatalog(catalogFile);
		services.add(catalog);
		
		this.queryEngine = new QueryEngine(conf, catalog, storeManager, null);
		this.queryEngine.init();
		services.add(queryEngine);

		Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
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
			for(EngineService service : services) {
				try {
					service.shutdown();
				} catch (Exception e) {
					LOG.error(e);
				}
			} 
		}

		LOG.info("NtaLeafServer main thread exiting");
	}
	
	private class ShutdownHook implements Runnable {
		@Override
		public void run() {
			shutdown("Shutting Down Normally!");
		}
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
	public SubQueryResponse requestSubQuery(SubQueryRequest request) throws IOException {
	  
	  TableMeta meta = storeManager.getTableMeta(request.getTablets().get(0).getTablePath());
	  TableDesc desc = new TableDescImpl(request.getTableName(), meta);
	  desc.setURI(request.getTablets().get(0).getTablePath());
	  catalog.addTable(desc);
	  
	  for(Tablet tablet : request.getTablets()) {
	    ResultSetOld result = queryEngine.executeQuery(request.getQuery(), tablet);
	  }
	  
	  catalog.deleteTable(request.getTableName());
	  
	  return null;
	}

	@Override
	public void assignTablets(AssignTabletRequest request) {
		// TODO - not implemented yet
	}

	@Override
	public void releaseTablets(ReleaseTableRequest request) {
		// TODO - not implemented yet
	}
	
	public void shutdown(final String msg) {
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
		shutdown(reason);
	}

	public static void main(String [] args) throws IOException {
		NtaConf conf = new NtaConf();
		LeafServer leafServer = new LeafServer(conf);

		leafServer.start();
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		return versionID;
	}
}