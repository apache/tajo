/**
 * 
 */
package nta.engine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import nta.catalog.CatalogClient;
import nta.catalog.CatalogService;
import nta.conf.NtaConf;
import nta.engine.LeafServerProtos.AssignTabletRequestProto;
import nta.engine.LeafServerProtos.ReleaseTabletRequestProto;
import nta.engine.LeafServerProtos.SubQueryRequestProto;
import nta.engine.LeafServerProtos.SubQueryResponseProto;
import nta.engine.cluster.MasterAddressTracker;
import nta.engine.cluster.ServerNodeTracker;
import nta.engine.ipc.LeafServerInterface;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.PhysicalPlanner;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.physical.PhysicalExec;
import nta.engine.query.QueryEngine;
import nta.engine.query.SubQueryRequestImpl;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;

/**
 * @author Hyunsik Choi
 *
 */
public class LeafServer extends Thread implements LeafServerInterface {
	private static final Log LOG = LogFactory.getLog(LeafServer.class);	

	private final Configuration conf;

	// Server States
	/**
	 * This servers address.
	 */	
	//	private final Server rpcServer;
	private final ProtoParamRpcServer rpcServer;
	private final InetSocketAddress isa;

	private volatile boolean stopped = false;
	private volatile boolean isOnline = false;

	private final String serverName;
	
	// Cluster Management
	private ZkClient zookeeper;
	private MasterAddressTracker masterAddrTracker;
	private ServerNodeTracker catalogAddrTracker;
	
	// Query Processing
	private FileSystem defaultFS;
	
	private CatalogClient catalog;
	private StorageManager storeManager;
	private QueryEngine queryEngine;
	private List<EngineService> services = new ArrayList<EngineService>();
	
	private final Path basePath;
	private final Path dataPath;
	
	private final QueryAnalyzer analyzer;
	private final SubqueryContext.Factory ctxFactory;

	public LeafServer(final Configuration conf) throws IOException {
		this.conf = conf;

		// Server to handle client requests.
		String hostname = DNS.getDefaultHost(
			conf.get("nta.master.dns.interface", "default"),
			conf.get("nta.master.dns.nameserver", "default"));
		int port = conf.getInt(NConstants.LEAFSERVER_PORT, 
		    NConstants.DEFAULT_LEAFSERVER_PORT);
		// Creation of a HSA will force a resolve.
		InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
		if (initialIsa.getAddress() == null) {
			throw new IllegalArgumentException("Failed resolve of " + this.isa);
		}
		
		this.rpcServer = NettyRpc.getProtoParamRpcServer(this, initialIsa);
		this.rpcServer.start();

		// Set our address.
	  this.isa = this.rpcServer.getBindAddress();
		this.serverName = this.isa.getHostName()+":"+this.isa.getPort();
		
		 // Get the tajo base dir
    this.basePath = new Path(conf.get(NConstants.ENGINE_BASE_DIR));
    LOG.info("Base dir is set " + conf.get(NConstants.ENGINE_BASE_DIR));
    // Get default DFS uri from the base dir
    this.defaultFS = basePath.getFileSystem(conf);
    LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");    
        
    if (defaultFS.exists(basePath) == false) {
      defaultFS.mkdirs(basePath);
      LOG.info("Tajo Base dir (" + basePath + ") is created.");
    }

    this.dataPath = new Path(conf.get(NConstants.ENGINE_DATA_DIR));
    LOG.info("Tajo data dir is set " + dataPath);
    if (!defaultFS.exists(dataPath)) {
      defaultFS.mkdirs(dataPath);
      LOG.info("Data dir (" + dataPath + ") is created");
    }
		
		this.catalog = new CatalogClient(conf);
		this.analyzer = new QueryAnalyzer(catalog);
		this.storeManager = new StorageManager(conf);
		this.ctxFactory = new SubqueryContext.Factory(catalog);

		Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
	}
	
	private void initLeafServer() throws IOException, InterruptedException, KeeperException {
	  initializeZookeeper();
	}

	public void run() {
		LOG.info("NtaLeafServer startup");
		
		try {
			initLeafServer();
			
      this.queryEngine = new QueryEngine(conf, catalog, storeManager, null);
      this.queryEngine.init();
      services.add(queryEngine);
			
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

		LOG.info("NtaLeafServer ("+serverName+") main thread exiting");
	}
	
	private class ShutdownHook implements Runnable {
		@Override
		public void run() {
		  masterAddrTracker.stop();
		  zookeeper.close();
		  catalog.close();
			shutdown("Shutting Down Normally! ("+serverName+")");
		}
	}
	
	private void initializeZookeeper() throws IOException, InterruptedException, KeeperException {
    this.zookeeper = new ZkClient(conf);
    this.masterAddrTracker = new MasterAddressTracker(zookeeper);
    this.masterAddrTracker.start();
    
    byte [] master = null;
    while(master == null) {
      master = masterAddrTracker.blockUntilAvailable(1000);
      LOG.info("Waiting for the Tajo master.....");
    }
    
    LOG.info("Got the master address (" + new String(master) + ")");
    // if the znode already exists, it will be updated for notification.
    ZkUtil.upsertEphemeralNode(zookeeper,
        ZkUtil.concat(NConstants.ZNODE_LEAFSERVERS, serverName));
    LOG.info("Write the znode nta/leafservers/" + serverName);
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
	public SubQueryResponseProto requestSubQuery(SubQueryRequestProto requestProto) throws IOException {
	  SubQueryRequest request = new SubQueryRequestImpl(requestProto);
	  SubqueryContext ctx = ctxFactory.create(request);	  
	  QueryBlock query = analyzer.parse(ctx, request.getQuery());
	  LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
	  LogicalOptimizer.optimize(ctx, plan);
    
    PhysicalPlanner phyPlanner = new PhysicalPlanner(storeManager);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);
    
    Tuple tuple = null;
    int i=0;
    long start = System.currentTimeMillis();
    while((tuple = exec.next()) != null) {
      System.out.println(tuple);
      i++;
    }
    long end = System.currentTimeMillis();
    System.out.println((end - start)+" msc");
	  
	  return null;
	}

	@Override
	public void assignTablets(AssignTabletRequestProto request) {
		// TODO - not implemented yet
	}

	@Override
	public void releaseTablets(ReleaseTabletRequestProto request) {
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
}