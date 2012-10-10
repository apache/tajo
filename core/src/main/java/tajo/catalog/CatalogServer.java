package tajo.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;
import tajo.NConstants;
import tajo.catalog.exception.*;
import tajo.catalog.proto.CatalogProtos.*;
import tajo.catalog.store.CatalogStore;
import tajo.catalog.store.DBStore;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.function.Country;
import tajo.engine.function.InCountry;
import tajo.engine.function.builtin.*;
import tajo.ipc.protocolrecords.Fragment;
import tajo.rpc.NettyRpc;
import tajo.rpc.NettyRpcServer;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkUtil;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class provides the catalog service. The catalog service enables clients
 * to register, unregister and access information about tables, functions, and
 * cluster information.
 * 
 * @author Hyunsik Choi
 */
public class CatalogServer extends Thread implements CatalogServiceProtocol {

	private final static Log LOG = LogFactory.getLog(CatalogServer.class);
	private final TajoConf conf;
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock rlock = lock.readLock();
	private final Lock wlock = lock.writeLock();

	private final CatalogStore store;
	  
	private Map<String, FunctionDescProto> functions = 
	    new HashMap<String, FunctionDescProto>();
  
  // RPC variables
  private final NettyRpcServer rpcServer;
  private final InetSocketAddress isa;
  private final String serverName;
  private final ZkClient zkClient;

  // Server status variables
  private volatile boolean stopped = false;
  @SuppressWarnings("unused")
  private volatile boolean isOnline = false;
  
  public CatalogServer(final TajoConf conf) throws IOException {
    this.conf = conf;

    Constructor<?> cons = null;
    try {
      Class<?> storeClass =
          this.conf.getClass(TConstants.STORE_CLASS, DBStore.class);
      LOG.info("Catalog Store Class: " + storeClass.getCanonicalName());
      
      cons = storeClass.
          getConstructor(new Class [] {Configuration.class});
    } catch (Exception e) {
      throw new IOException("cannot initialize catalog store");
    }
    try {
      this.store = (CatalogStore) cons.newInstance(this.conf);
    } catch (Exception e) {
      throw new IOException("cannot initialize catalog store");
    }
    initBuiltinFunctions();

    // Server to handle client requests.
    String serverAddr = conf.getVar(ConfVars.CATALOG_ADDRESS);
    // Creation of a HSA will force a resolve.
    InetSocketAddress initIsa = NetUtils.createSocketAddr(serverAddr);
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this, CatalogServiceProtocol.class, initIsa);
    this.rpcServer.start();
    this.isa = this.rpcServer.getBindAddress();
    this.serverName = this.isa.getHostName() + ":" + this.isa.getPort();
    this.zkClient = new ZkClient(conf);
  }

  private void prepareServing() throws IOException, KeeperException,
      InterruptedException {
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
      try {
        prepareServing();
        participateCluster();
      } catch (Exception e) {
        abort(e.getMessage(), e);
      }

      LOG.info("Catalog Server startup (" + serverName + ")");

      // loop area
      if (!this.stopped) {
        this.isOnline = true;
        while (!this.stopped) {
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

    LOG.info("Catalog Server (" + serverName + ") main thread exiting");
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
    LOG.info("Created the znode " + NConstants.ZNODE_CATALOG + " with "
        + serverName);
  }

  @Override
  public TableDescProto getTableDesc(StringProto name)
      throws NoSuchTableException {
    rlock.lock();
    try {
      String tableId = name.getValue().toLowerCase();
      if (!this.store.existTable(tableId)) {
        throw new NoSuchTableException(tableId);
      }
      return (TableDescProto) this.store.getTable(tableId).getProto();
    } catch (IOException ioe) {
      // TODO - handle exception
      LOG.error(ioe);
      return null;
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public GetAllTableNamesResponse getAllTableNames(NullProto request) {
    try {
      Iterator<String> iterator = store.getAllTableNames().iterator();
      GetAllTableNamesResponse.Builder builder =
          GetAllTableNamesResponse.newBuilder();
      while (iterator.hasNext()) {
        builder.addTableName(iterator.next());
      }
      return builder.build();
    } catch (IOException ioe) {
      // TODO - handle exception
      return null;
    }
  }

  @Override
  public void addTable(final TableDescProto proto)
      throws AlreadyExistsTableException {
    Preconditions.checkArgument(proto.hasId(),
        "Must be set to the table name");
    Preconditions.checkArgument(proto.hasPath(),
        "Must be set to the table URI");

    wlock.lock();
    try {
      if (store.existTable(proto.getId())) {
        throw new AlreadyExistsTableException(proto.getId());
      }

      // rewrite schema
      SchemaProto revisedSchema =
          TCatUtil.getQualfiedSchema(proto.getId(), proto.getMeta()
              .getSchema());

      TableProto.Builder metaBuilder = TableProto.newBuilder(proto.getMeta());
      metaBuilder.setSchema(revisedSchema);
      TableDescProto.Builder descBuilder = TableDescProto.newBuilder(proto);
      descBuilder.setMeta(metaBuilder.build());

      store.addTable(new TableDescImpl(descBuilder.build()));

    } catch (IOException ioe) {
      LOG.error(ioe);
    } finally {
      wlock.unlock();
      LOG.info("Table " + proto.getId() + " is added to the catalog ("
          + serverName + ")");
    }
  }

  @Override
  public void deleteTable(StringProto name) throws NoSuchTableException {
    wlock.lock();
    try {
      String tableId = name.getValue().toLowerCase();
      if (!store.existTable(tableId)) {
        throw new NoSuchTableException(tableId);
      }
      store.deleteTable(tableId);
    } catch (IOException ioe) {
      LOG.error(ioe);
    } finally {
      wlock.unlock();
    }
  }

  @Override
  public BoolProto existsTable(StringProto name) {
    try {
      String tableId = name.getValue().toLowerCase();
      return BoolProto.newBuilder().setValue(store.existTable(tableId)).build();
    } catch (IOException e) {
      LOG.error(e);
      return BoolProto.newBuilder().setValue(false).build();
    }
  }
  
  private List<FragmentServInfo> getFragmentLocInfo(TableDescProto desc) throws IOException {
    long before = System.currentTimeMillis();
    int fileIdx, blockIdx;
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(desc.getPath());
    
    FileStatus[] files = fs.listStatus(new Path(path+"/data"));
    BlockLocation[] blocks;
    String[] hosts;
    List<FragmentServInfo> tabletInfoList = new ArrayList<FragmentServInfo>();
    
    for (fileIdx = 0; fileIdx < files.length; fileIdx++) {
      blocks = fs.getFileBlockLocations(files[fileIdx], 0, files[fileIdx].getLen());
      for (blockIdx = 0; blockIdx < blocks.length; blockIdx++) {
        hosts = blocks[blockIdx].getHosts();

        // TODO: select the proper serving node for block
        tabletInfoList.add(new FragmentServInfo(hosts[0], -1, new Fragment(desc.getId(),
            files[fileIdx].getPath(), new TableMetaImpl(desc.getMeta()), 
            blocks[blockIdx].getOffset(), blocks[blockIdx].getLength())));
      }
    }
    long after = System.currentTimeMillis();
    LOG.info("getTabletLocInfo processing time: " + (after-before) + "msc");
    return tabletInfoList;
  }
	
  @Override
  public void addIndex(IndexDescProto index) {
    rlock.lock();
    try {
      if (store.existIndex(index.getName())) {
        throw new AlreadyExistsIndexException(index.getName());
      }
      store.addIndex(index);
    } catch (IOException ioe) {
      LOG.error("ERROR : cannot add index " + index.getName(), ioe);
      LOG.error(index);
    } finally {
      rlock.unlock();
    }
  }
  
  @Override
  public BoolProto existIndex(StringProto indexName) {
    rlock.lock();
    try {      
      return BoolProto.newBuilder().setValue(
          store.existIndex(indexName.getValue())).build();
    } catch (IOException e) {
      LOG.error(e);
      return BoolProto.newBuilder().setValue(false).build();
    } finally {
      rlock.unlock();
    }
  }
  
  @Override
  public BoolProto existIndex(GetIndexRequest req) {
    rlock.lock();
    try {      
      return BoolProto.newBuilder().setValue(
          store.existIndex(req.getTableName(), req.getColumnName())).build();
    } catch (IOException e) {
      LOG.error(e);
      return BoolProto.newBuilder().setValue(false).build();
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public void delIndex(StringProto indexName) {
    wlock.lock();
    try {
      if (!store.existIndex(indexName.getValue())) {
        throw new NoSuchIndexException(indexName.getValue());
      }
      store.delIndex(indexName.getValue());
    } catch (IOException e) {
      LOG.error(e);
    } finally {
      wlock.unlock();
    }
  }

  @Override
  public IndexDescProto getIndex(StringProto indexName) {
    rlock.lock();
    try {
      if (!store.existIndex(indexName.getValue())) {
        throw new NoSuchIndexException(indexName.getValue());
      }
      return store.getIndex(indexName.getValue());
    } catch (IOException ioe) {
      LOG.error("ERROR : cannot get index " + indexName, ioe);
      return null;
    } finally {    
      rlock.unlock();
    }
  }
  
  @Override
  public IndexDescProto getIndex(GetIndexRequest req) {    
    rlock.lock();
    try {
      if (!store.existIndex(req.getTableName())) {
        throw new NoSuchIndexException(req.getTableName() + "." 
            + req.getColumnName());
      }
      return store.getIndex(req.getTableName(), req.getColumnName());
    } catch (IOException ioe) {
      LOG.error("ERROR : cannot get index " + req.getTableName() + "." 
          + req.getColumnName(), ioe);
      return null;
    } finally {    
      rlock.unlock();
    }
  }

  @Override
  public void registerFunction(FunctionDescProto funcDesc) {
    String canonicalName =
        TCatUtil.getCanonicalName(funcDesc.getSignature(),
            funcDesc.getParameterTypesList());
    if (functions.containsKey(canonicalName)) {
      throw new AlreadyExistsFunctionException(canonicalName);
    }

    functions.put(canonicalName, funcDesc);
    if (LOG.isDebugEnabled()) {
      LOG.info("Function " + canonicalName + " is registered.");
    }
  }

  @Override
  public void unregisterFunction(UnregisterFunctionRequest request) {
    String signature = request.getSignature();
    List<DataType> paramTypes = new ArrayList<DataType>();
    int size = request.getParameterTypesCount();
    for (int i = 0; i < size; i++) {
      paramTypes.add(request.getParameterTypes(i));
    }
    String canonicalName = TCatUtil.getCanonicalName(signature, paramTypes);
    if (!functions.containsKey(canonicalName)) {
      throw new NoSuchFunctionException(canonicalName);
    }

    functions.remove(canonicalName);
    LOG.info("GeneralFunction " + canonicalName + " is unregistered.");
  }

  @Override
  public FunctionDescProto getFunctionMeta(GetFunctionMetaRequest request) {
    List<DataType> paramTypes = new ArrayList<DataType>();
    int size = request.getParameterTypesCount();
    for (int i = 0; i < size; i++) {
      paramTypes.add(request.getParameterTypes(i));
    }
    return this.functions.get(TCatUtil.getCanonicalName(
        request.getSignature().toLowerCase(), paramTypes));
  }

  @Override
  public BoolProto containFunction(ContainFunctionRequest request) {
    List<DataType> paramTypes = new ArrayList<DataType>();
    int size = request.getParameterTypesCount();
    for (int i = 0; i < size; i++) {
      paramTypes.add(request.getParameterTypes(i));
    }
    boolean returnValue =
        this.functions.containsKey(TCatUtil.getCanonicalName(
            request.getSignature().toLowerCase(), paramTypes));
    return BoolProto.newBuilder().setValue(returnValue).build();
  }

  @Override
  public GetFunctionsResponse getFunctions(NullProto request) {
    Iterator<FunctionDescProto> iterator = functions.values().iterator();
    GetFunctionsResponse.Builder builder = GetFunctionsResponse.newBuilder();
    while (iterator.hasNext()) {
      builder.addFunctionDesc(iterator.next());
    }
    return builder.build();
  }

  private void initBuiltinFunctions() {
    List<FunctionDesc> sqlFuncs = new ArrayList<FunctionDesc>();

    // Sum
    sqlFuncs.add(new FunctionDesc("sum", NewSumInt.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.INT},
        new DataType[] {DataType.INT}));
    sqlFuncs.add(new FunctionDesc("sum", NewSumLong.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.LONG},
        new DataType[] {DataType.LONG}));
    sqlFuncs.add(new FunctionDesc("sum", NewSumFloat.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.FLOAT},
        new DataType[] {DataType.FLOAT}));
    sqlFuncs.add(new FunctionDesc("sum", NewSumDouble.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.DOUBLE},
        new DataType[] {DataType.DOUBLE}));

    // Max
    sqlFuncs.add(new FunctionDesc("max", NewMaxInt.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.INT},
        new DataType[] {DataType.INT}));
    sqlFuncs.add(new FunctionDesc("max", NewMaxLong.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.LONG},
        new DataType[] {DataType.LONG}));
    sqlFuncs.add(new FunctionDesc("max", NewMaxFloat.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.FLOAT},
        new DataType[] {DataType.FLOAT}));
    sqlFuncs.add(new FunctionDesc("max", NewMaxDouble.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.DOUBLE},
        new DataType[] {DataType.DOUBLE}));

    // Min
    sqlFuncs.add(new FunctionDesc("min", NewMinInt.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.INT},
        new DataType[] {DataType.INT}));
    sqlFuncs.add(new FunctionDesc("min", NewMinLong.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.LONG},
        new DataType[] {DataType.LONG}));
    sqlFuncs.add(new FunctionDesc("min", NewMinFloat.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.FLOAT},
        new DataType[] {DataType.FLOAT }));
    sqlFuncs.add(new FunctionDesc("min", NewMinDouble.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.DOUBLE},
        new DataType[] {DataType.DOUBLE}));
    sqlFuncs.add(new FunctionDesc("min", NewMinString.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.STRING},
        new DataType[] {DataType.STRING}));

    // AVG
    sqlFuncs.add(new FunctionDesc("avg", AvgInt.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.FLOAT},
        new DataType[] {DataType.INT}));
    sqlFuncs.add(new FunctionDesc("avg", AvgLong.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.DOUBLE},
        new DataType[] {DataType.LONG}));
    sqlFuncs.add(new FunctionDesc("avg", AvgFloat.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.FLOAT},
        new DataType[] {DataType.FLOAT}));
    sqlFuncs.add(new FunctionDesc("avg", AvgDouble.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.DOUBLE},
        new DataType[] {DataType.DOUBLE}));

    // Count
    sqlFuncs.add(new FunctionDesc("count", NewCountValue.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.LONG},
        new DataType[] {DataType.ANY}));
    sqlFuncs.add(new FunctionDesc("count", NewCountRows.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.LONG},
        new DataType[] {}));

    // GeoIP
    sqlFuncs.add(new FunctionDesc("in_country", InCountry.class, FunctionType.GENERAL,
        new DataType[] {DataType.BOOLEAN},
        new DataType[] {DataType.STRING, DataType.STRING}));
    sqlFuncs.add(new FunctionDesc("country", Country.class, FunctionType.GENERAL,
        new DataType[] {DataType.STRING},
        new DataType[] {DataType.STRING}));

    for (FunctionDesc func : sqlFuncs) {
      registerFunction(func.getProto());
    }

    List<FunctionDesc> extendedFuncs = Lists.newArrayList();
    extendedFuncs.add(new FunctionDesc("random", RandomInt.class, FunctionType.GENERAL,
        new DataType[] {DataType.INT},
        new DataType[] {DataType.INT}));

    for (FunctionDesc func : extendedFuncs) {
      registerFunction(func.getProto());
    }
  }

  public static void main(String[] args) throws IOException {
    TajoConf conf = new TajoConf();
    CatalogServer catalog = new CatalogServer(conf);
    catalog.start();
  }
}
