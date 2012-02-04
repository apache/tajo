package nta.catalog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import nta.catalog.exception.AlreadyExistsFunction;
import nta.catalog.exception.AlreadyExistsTableException;
import nta.catalog.exception.NoSuchFunctionException;
import nta.catalog.exception.NoSuchTableException;
import nta.catalog.proto.CatalogProtos.ColumnProto;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionDescProto;
import nta.catalog.proto.CatalogProtos.SchemaProto;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.catalog.proto.CatalogProtos.TableProto;
import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Preconditions;

/**
 * This class provides the catalog service. 
 * The catalog service enables clients to register, unregister and access 
 * information about tables, functions, and cluster information.  
 * 
 * @author Hyunsik Choi
 */
public class CatalogServer extends Thread implements CatalogServiceProtocol {
	private static Log LOG = LogFactory.getLog(CatalogServer.class);
	private Configuration conf;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private Lock rlock = lock.readLock();
	private Lock wlock = lock.writeLock();

	private Map<String, TableDescProto> tables = 
	    new HashMap<String, TableDescProto>();	
	private Map<String, FunctionDescProto> functions = 
	    new HashMap<String, FunctionDescProto>();
  private Map<String, List<TabletServInfo>> tabletServingInfo 
  = new HashMap<String, List<TabletServInfo>>();
	
  // RPC variables
	private final ProtoParamRpcServer rpcServer;
  private final InetSocketAddress isa;
  private final String serverName;  
  private ZkClient zkClient;

  // Server status variables
  private volatile boolean stopped = false;
  private volatile boolean isOnline = false;

	public CatalogServer(Configuration conf) throws IOException {
		this.conf = conf;
		
		// Server to handle client requests.
    String serverAddr = conf.get(NConstants.CATALOG_ADDRESS, 
        NConstants.DEFAULT_CATALOG_ADDRESS);
    // Creation of a HSA will force a resolve.
    InetSocketAddress initIsa = NetUtils.createSocketAddr(serverAddr);
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this, initIsa);
    this.isa = this.rpcServer.getBindAddress();
    this.serverName = this.isa.getHostName() + ":" + this.isa.getPort();
	}
	
  public void shutdown() throws IOException {
    this.rpcServer.shutdown();
    this.zkClient.close();
  }

	private void initCatalogServer() throws IOException, 
	    KeeperException, InterruptedException {
    initializeZookeeper();
    this.rpcServer.start();
	}
	
	public InetSocketAddress getBindAddress() {
	  return this.isa;
	}
	
	public void run() {
    try {
      LOG.info("Catalog Server startup ("+serverName+")");
      
      initCatalogServer();
      
      // loop area
      if(!this.stopped) {
        this.isOnline = true;
        while(!this.stopped) {          
          Thread.sleep(1000);

        }
      } 
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
    } finally {
      // finalize area regardless of either normal or abnormal shutdown
      try {
        shutdown();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    LOG.info("Catalog Server ("+serverName+") main thread exiting");
  }
	
	public void stop(String message) {
	  this.stopped = true;
	}
	
	private void initializeZookeeper() throws KeeperException, 
	    InterruptedException, IOException {
	  this.zkClient = new ZkClient(conf);
    ZkUtil.upsertEphemeralNode(zkClient, NConstants.ZNODE_CATALOG, 
        serverName.getBytes());
	}

	public TableDescProto getTableDesc(String tableId) throws NoSuchTableException {
		rlock.lock();
		try {
			if (!this.tables.containsKey(tableId)) {
				throw new NoSuchTableException(tableId);
			}
			return this.tables.get(tableId);
		} finally {
			rlock.unlock();
		}
	}

	public Collection<TableDescProto> getAllTableDescs() {
		wlock.lock();
		try {
			return tables.values();			
		} finally {
			wlock.unlock();
		}
	}
	
	public void resetHostsByTable() {
		this.tabletServingInfo.clear();
	}
	
	public List<TabletServInfo> getHostByTable(String tableId) {
		return tabletServingInfo.get(tableId);
	}
	
	public void updateAllTabletServingInfo(List<String> onlineServers) throws IOException {
		tabletServingInfo.clear();
		Collection<TableDescProto> tbs = tables.values();
		Iterator<TableDescProto> it = tbs.iterator();
		List<TabletServInfo> locInfos;
		List<TabletServInfo> servInfos;
		int index = 0;
		StringTokenizer tokenizer;
		while (it.hasNext()) {
			TableDescProto td = it.next();
			locInfos = getTabletLocInfo(td);
			servInfos = new ArrayList<TabletServInfo>();
			// TODO: select the proper online server
			for (TabletServInfo servInfo : locInfos) {
				// round robin
				if (index == onlineServers.size()) {
					index = 0;
				}
				tokenizer = new StringTokenizer(onlineServers.get(index++), ":");
				servInfo.setHost(tokenizer.nextToken(), Integer.valueOf(tokenizer.nextToken()));
				servInfos.add(servInfo);
			}
			tabletServingInfo.put(td.getId(), servInfos);
		}
	}
	
	private List<TabletServInfo> getTabletLocInfo(TableDescProto desc) throws IOException {
		int fileIdx, blockIdx;
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(desc.getPath());
		
		FileStatus[] files = fs.listStatus(new Path(path+"/data"));
		BlockLocation[] blocks;
		String[] hosts;
		List<TabletServInfo> tabletInfoList = new ArrayList<TabletServInfo>();
//		if (tabletServingInfo.containsKey(tid)) {
//			tabletInfoList = tabletServingInfo.get(tid);
//		} else {
//			tabletInfoList = new ArrayList<TabletServInfo>();
//		}
		
		int i=0;
		for (fileIdx = 0; fileIdx < files.length; fileIdx++) {
			blocks = fs.getFileBlockLocations(files[fileIdx], 0, files[fileIdx].getLen());
			for (blockIdx = 0; blockIdx < blocks.length; blockIdx++) {
				hosts = blocks[blockIdx].getHosts();
//				if (tabletServingInfo.containsKey(tid)) {
//					tabletInfoList = tabletServingInfo.get(tid);
//				} else {
//					tabletInfoList = new ArrayList<TabletServInfo>();
//					tabletServingInfo.put(tid, tabletInfoList);
//				}
				// TODO: select the proper serving node for block
				tabletInfoList.add(new TabletServInfo(hosts[0], -1, new Fragment(desc.getId()+"_"+i, 
            files[fileIdx].getPath(), new TableMetaImpl(desc.getMeta()), 
						blocks[blockIdx].getOffset(), blocks[blockIdx].getLength())));
				i++;
			}
		}
		return tabletInfoList;
	}
	
	public void addTable(String tableId, TableMeta info) throws AlreadyExistsTableException {	  
	  addTable(new TableDescImpl(tableId, info).proto);
	}

	public void addTable(final TableDescProto proto) throws AlreadyExistsTableException {
	  Preconditions.checkArgument(proto.hasId() == true, 
	      "Must be set to the table name");
		Preconditions.checkArgument(proto.hasPath() == true, 
		    "Must be set to the table URI");
	  wlock.lock();
		
		try {
			if (tables.containsKey(proto.getId())) {
				throw new AlreadyExistsTableException(proto.getId());
			}

			// rewrite schema
			SchemaProto revisedSchema = 
			    CatalogUtil.getQualfiedSchema(proto.getId(), 
			        proto.getMeta().getSchema());
			
			TableProto.Builder metaBuilder = TableProto.newBuilder(proto.getMeta());
			metaBuilder.setSchema(revisedSchema);
			TableDescProto.Builder descBuilder = TableDescProto.newBuilder(proto);
			descBuilder.setMeta(metaBuilder.build());
			
	    this.tables.put(proto.getId(), descBuilder.build());
	    
		} finally {
			wlock.unlock();
		}
	}

	public void deleteTable(String tableId) throws NoSuchTableException {
		wlock.lock();
		try {
			if (!tables.containsKey(tableId)) {
				throw new NoSuchTableException(tableId);
			}
			tables.remove(tableId);
		}
		finally {
			wlock.unlock();
		}
	}

	public boolean existsTable(String tableId) {
		rlock.lock();
		try {
			return tables.containsKey(tableId);
		} finally {
			rlock.unlock();
		}
	}

	@Override
	public void registerFunction(FunctionDescProto funcDesc) {
	  String canonicalName = CatalogUtil.getCanonicalName(funcDesc.getSignature(), 
	      funcDesc.getParameterTypesList());
		if (functions.containsKey(canonicalName)) {
			throw new AlreadyExistsFunction(canonicalName);
		}
		
		functions.put(canonicalName, funcDesc);
	}

	public void unregisterFunction(String signature, DataType...paramTypes) {
	  String canonicalName = CatalogUtil.getCanonicalName(signature, paramTypes);
		if (!functions.containsKey(canonicalName)) {
			throw new NoSuchFunctionException(canonicalName);
		}
		
		functions.remove(canonicalName);
	}

	public FunctionDescProto getFunctionMeta(String signature, 
	    DataType...paramTypes) {
		return this.functions.get(CatalogUtil.getCanonicalName(signature, 
		    paramTypes));
	}

	public boolean containFunction(String signature, DataType...paramTypes) {
		return this.functions.containsKey(CatalogUtil.getCanonicalName(signature, 
		    paramTypes));
	}

	public Collection<FunctionDescProto> getFunctions() {
		return functions.values();
	}
	
	public static void main(String [] args) throws IOException {
	  NtaConf conf = new NtaConf();
	  CatalogServer catalog = new CatalogServer(conf);
	  catalog.start();
	}
}
