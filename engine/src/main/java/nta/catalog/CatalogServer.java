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
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionDescProto;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.SchemaProto;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.catalog.proto.CatalogProtos.TableProto;
import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.engine.function.CountRows;
import nta.engine.function.CountValue;
import nta.engine.function.MaxDouble;
import nta.engine.function.MaxFloat;
import nta.engine.function.MaxInt;
import nta.engine.function.MaxLong;
import nta.engine.function.MinDouble;
import nta.engine.function.MinFloat;
import nta.engine.function.MinInt;
import nta.engine.function.MinLong;
import nta.engine.function.SumDouble;
import nta.engine.function.SumFloat;
import nta.engine.function.SumInt;
import nta.engine.function.SumLong;
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
  private Map<String, List<HostInfo>> tabletServingInfo
  = new HashMap<String, List<HostInfo>>();
	
  // RPC variables
	private final ProtoParamRpcServer rpcServer;
  private final InetSocketAddress isa;
  private final String serverName;  
  private ZkClient zkClient;

  // Server status variables
  private volatile boolean stopped = false;
  @SuppressWarnings("unused")
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
    this.zkClient = new ZkClient(conf);
    
    initBuiltinFunctions();
	}
	
  private void prepareServing() throws IOException, KeeperException,
      InterruptedException {    
    this.rpcServer.start();
  }
	
  private void cleanUp() throws IOException {
    this.rpcServer.shutdown();
    this.zkClient.close();
  }
	
	public InetSocketAddress getBindAddress() {
	  return this.isa;
	}
	
	public void run() {
    try {
      LOG.info("Catalog Server startup ("+serverName+")");
      try {
        prepareServing();
        participateCluster();
      } catch (Exception e) {
        abort(e.getMessage(), e);
      }
      
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
        cleanUp();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    LOG.info("Catalog Server ("+serverName+") main thread exiting");
  }
	
	public void shutdown(final String msg) {
    this.stopped = true;
    LOG.info("STOPPED: " + msg);
    synchronized (this) {
      notifyAll();
    }
  }
	
	public void abort(String reason, Throwable cause) {
    if (cause != null) {
      LOG.fatal("ABORTING leaf server " + this + ": " + reason, cause);
    } else {
      LOG.fatal("ABORTING leaf server " + this + ": " + reason);
    }
    // TODO - abortRequest : to be implemented
    shutdown(reason);
  }
	
	private void participateCluster() throws KeeperException, 
	    InterruptedException, IOException {
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
	
	public List<HostInfo> getHostByTable(String tableId) {
		return tabletServingInfo.get(tableId);
	}
	
	public void updateAllTabletServingInfo(List<String> onlineServers) throws IOException {
		tabletServingInfo.clear();
		Collection<TableDescProto> tbs = tables.values();
		Iterator<TableDescProto> it = tbs.iterator();
		List<HostInfo> locInfos;
		List<HostInfo> servInfos;
		int index = 0;
		StringTokenizer tokenizer;
		while (it.hasNext()) {
			TableDescProto td = it.next();
			locInfos = getTabletLocInfo(td);
			servInfos = new ArrayList<HostInfo>();
			// TODO: select the proper online server
			for (HostInfo servInfo : locInfos) {
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
	
	private List<HostInfo> getTabletLocInfo(TableDescProto desc) throws IOException {
		int fileIdx, blockIdx;
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(desc.getPath());
		
		FileStatus[] files = fs.listStatus(new Path(path+"/data"));
		BlockLocation[] blocks;
		String[] hosts;
		List<HostInfo> tabletInfoList = new ArrayList<HostInfo>();
//		if (tabletServingInfo.containsKey(tid)) {
//			tabletInfoList = tabletServingInfo.get(tid);
//		} else {
//			tabletInfoList = new ArrayList<HostInfo>();
//		}
		
		int i=0;
		for (fileIdx = 0; fileIdx < files.length; fileIdx++) {
			blocks = fs.getFileBlockLocations(files[fileIdx], 0, files[fileIdx].getLen());
			for (blockIdx = 0; blockIdx < blocks.length; blockIdx++) {
				hosts = blocks[blockIdx].getHosts();
//				if (tabletServingInfo.containsKey(tid)) {
//					tabletInfoList = tabletServingInfo.get(tid);
//				} else {
//					tabletInfoList = new ArrayList<HostInfo>();
//					tabletServingInfo.put(tid, tabletInfoList);
//				}
				// TODO: select the proper serving node for block
				tabletInfoList.add(new HostInfo(hosts[0], -1, new Fragment(desc.getId()+"_"+i, 
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
			LOG.info("Table " + proto.getId() + " is added to the catalog (" + serverName +")");
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
		LOG.info("Function " + canonicalName + " is registered.");
	}

	public void unregisterFunction(String signature, DataType...paramTypes) {
	  String canonicalName = CatalogUtil.getCanonicalName(signature, paramTypes);
		if (!functions.containsKey(canonicalName)) {
			throw new NoSuchFunctionException(canonicalName);
		}
		
		functions.remove(canonicalName);
		LOG.info("Function " + canonicalName + " is unregistered.");
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
	
  private void initBuiltinFunctions() {
    List<FunctionDesc> sqlFuncs = new ArrayList<FunctionDesc>();
    
    // Sum
    sqlFuncs.add(new FunctionDesc("sum", SumInt.class,
            FunctionType.AGGREGATION, DataType.INT,
            new DataType[] { DataType.INT }));
    sqlFuncs.add(new FunctionDesc("sum", SumLong.class,
        FunctionType.AGGREGATION, DataType.LONG,
        new DataType[] { DataType.LONG }));
    sqlFuncs.add(new FunctionDesc("sum", SumFloat.class,
        FunctionType.AGGREGATION, DataType.FLOAT,
        new DataType[] { DataType.FLOAT }));
    sqlFuncs.add(new FunctionDesc("sum", SumDouble.class,
        FunctionType.AGGREGATION, DataType.DOUBLE,
        new DataType[] { DataType.DOUBLE }));
    
    // Max
    sqlFuncs.add(new FunctionDesc("max", MaxInt.class,
            FunctionType.AGGREGATION, DataType.INT,
            new DataType[] { DataType.INT }));
    sqlFuncs.add(new FunctionDesc("max", MaxLong.class,
        FunctionType.AGGREGATION, DataType.LONG,
        new DataType[] { DataType.LONG }));
    sqlFuncs.add(new FunctionDesc("max", MaxFloat.class,
        FunctionType.AGGREGATION, DataType.FLOAT,
        new DataType[] { DataType.FLOAT }));
    sqlFuncs.add(new FunctionDesc("max", MaxDouble.class,
        FunctionType.AGGREGATION, DataType.DOUBLE,
        new DataType[] { DataType.DOUBLE }));
    
    // Min
    sqlFuncs.add(new FunctionDesc("min", MinInt.class,
            FunctionType.AGGREGATION, DataType.INT,
            new DataType[] { DataType.INT }));
    sqlFuncs.add(new FunctionDesc("min", MinLong.class,
        FunctionType.AGGREGATION, DataType.LONG,
        new DataType[] { DataType.LONG }));
    sqlFuncs.add(new FunctionDesc("min", MinFloat.class,
        FunctionType.AGGREGATION, DataType.FLOAT,
        new DataType[] { DataType.FLOAT }));
    sqlFuncs.add(new FunctionDesc("min", MinDouble.class,
        FunctionType.AGGREGATION, DataType.DOUBLE,
        new DataType[] { DataType.DOUBLE }));

    // Count
    sqlFuncs.add(new FunctionDesc("count", CountValue.class,
        FunctionType.AGGREGATION, DataType.LONG,
        new DataType[] {DataType.ANY}));
    sqlFuncs.add(new FunctionDesc("count", CountRows.class,
        FunctionType.AGGREGATION, DataType.LONG,
        new DataType[] {}));

    for (FunctionDesc func : sqlFuncs) {
      registerFunction(func.getProto());
    }
  }
	
	public static void main(String [] args) throws IOException {
	  NtaConf conf = new NtaConf();
	  CatalogServer catalog = new CatalogServer(conf);
	  catalog.start();
	}
}
