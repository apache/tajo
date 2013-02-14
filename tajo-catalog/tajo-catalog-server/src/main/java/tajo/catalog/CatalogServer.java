/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.catalog;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.AbstractService;
import tajo.catalog.CatalogProtocol.CatalogProtocolService;
import tajo.catalog.exception.*;
import tajo.catalog.proto.CatalogProtos.*;
import tajo.catalog.store.CatalogStore;
import tajo.catalog.store.DBStore;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.rpc.ProtoBlockingRpcServer;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import tajo.util.NetUtils;

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
public class CatalogServer extends AbstractService {

  private final static Log LOG = LogFactory.getLog(CatalogServer.class);
  private TajoConf conf;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock rlock = lock.readLock();
  private final Lock wlock = lock.writeLock();

  private CatalogStore store;

  private Map<String, FunctionDescProto> functions = new HashMap<>();

  // RPC variables
  private ProtoBlockingRpcServer rpcServer;
  private InetSocketAddress bindAddress;
  private String serverName;
  final CatalogProtocolHandler handler;

  // Server status variables
  private volatile boolean stopped = false;
  @SuppressWarnings("unused")
  private volatile boolean isOnline = false;

  private static BoolProto BOOL_TRUE = BoolProto.newBuilder().
      setValue(true).build();
  private static BoolProto BOOL_FALSE = BoolProto.newBuilder().
      setValue(false).build();

  private List<FunctionDesc> builtingFuncs;

  public CatalogServer() throws IOException {
    super(CatalogServer.class.getName());
    this.handler = new CatalogProtocolHandler();
    this.builtingFuncs = new ArrayList<>();
  }

  public CatalogServer(List<FunctionDesc> sqlFuncs) throws IOException {
    this();
    this.builtingFuncs = sqlFuncs;
  }

  @Override
  public void init(Configuration _conf) {
    this.conf = (TajoConf) _conf;

    Constructor<?> cons;
    try {
      Class<?> storeClass =
          this.conf.getClass(CatalogConstants.STORE_CLASS, DBStore.class);
      LOG.info("Catalog Store Class: " + storeClass.getCanonicalName());

      cons = storeClass.
          getConstructor(new Class [] {Configuration.class});

      this.store = (CatalogStore) cons.newInstance(this.conf);

      initBuiltinFunctions(builtingFuncs);
    } catch (Throwable t) {
      LOG.error("cannot initialize CatalogServer", t);
    }

    super.init(conf);
  }

  private void initBuiltinFunctions(List<FunctionDesc> functions)
      throws ServiceException {
    for (FunctionDesc desc : functions) {
      handler.registerFunction(null, desc.getProto());
    }
  }

  public void start() {
    // Server to handle client requests.
    String serverAddr = conf.getVar(ConfVars.CATALOG_ADDRESS);
    // Creation of a HSA will force a resolve.
    InetSocketAddress initIsa = NetUtils.createSocketAddr(serverAddr);
    try {
      this.rpcServer = new ProtoBlockingRpcServer(
          CatalogProtocol.class,
          handler, initIsa);
      this.rpcServer.start();

      this.bindAddress = this.rpcServer.getBindAddress();
      this.serverName = tajo.util.NetUtils.getIpPortString(bindAddress);
      conf.setVar(ConfVars.CATALOG_ADDRESS, serverName);
    } catch (Exception e) {
      LOG.error("Cannot start RPC Server of CatalogServer", e);
    }

    LOG.info("Catalog Server startup (" + serverName + ")");
    super.start();
  }

  public void stop() {
    this.rpcServer.shutdown();
    LOG.info("Catalog Server (" + serverName + ") shutdown");
    super.stop();
  }

  public CatalogProtocolHandler getHandler() {
    return this.handler;
  }

  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  public class CatalogProtocolHandler
      implements CatalogProtocolService.BlockingInterface {

    @Override
    public TableDescProto getTableDesc(RpcController controller,
                                       StringProto name)
        throws ServiceException {
      rlock.lock();
      try {
        String tableId = name.getValue().toLowerCase();
        if (!store.existTable(tableId)) {
          throw new NoSuchTableException(tableId);
        }
        return (TableDescProto) store.getTable(tableId).getProto();
      } catch (IOException ioe) {
        // TODO - handle exception
        LOG.error(ioe);
        return null;
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public GetAllTableNamesResponse getAllTableNames(RpcController controller,
                                                     NullProto request)
        throws ServiceException {
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
    public GetFunctionsResponse getFunctions(RpcController controller,
                                             NullProto request)
        throws ServiceException {
      Iterator<FunctionDescProto> iterator = functions.values().iterator();
      GetFunctionsResponse.Builder builder = GetFunctionsResponse.newBuilder();
      while (iterator.hasNext()) {
        builder.addFunctionDesc(iterator.next());
      }
      return builder.build();
    }

    @Override
    public BoolProto addTable(RpcController controller, TableDescProto tableDesc)
        throws ServiceException {
      Preconditions.checkArgument(tableDesc.hasId(),
          "Must be set to the table name");
      Preconditions.checkArgument(tableDesc.hasPath(),
          "Must be set to the table URI");

      wlock.lock();
      try {
        if (store.existTable(tableDesc.getId())) {
          throw new AlreadyExistsTableException(tableDesc.getId());
        }

        // rewrite schema
        SchemaProto revisedSchema =
            TCatUtil.getQualfiedSchema(tableDesc.getId(), tableDesc.getMeta()
                .getSchema());

        TableProto.Builder metaBuilder = TableProto.newBuilder(tableDesc.getMeta());
        metaBuilder.setSchema(revisedSchema);
        TableDescProto.Builder descBuilder = TableDescProto.newBuilder(tableDesc);
        descBuilder.setMeta(metaBuilder.build());

        store.addTable(new TableDescImpl(descBuilder.build()));

      } catch (IOException ioe) {
        LOG.error(ioe);
      } finally {
        wlock.unlock();
        LOG.info("Table " + tableDesc.getId() + " is added to the catalog ("
            + serverName + ")");
      }

      return BOOL_TRUE;
    }

    @Override
    public BoolProto deleteTable(RpcController controller, StringProto name)
        throws ServiceException {
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

      return BOOL_TRUE;
    }

    @Override
    public BoolProto existsTable(RpcController controller, StringProto name)
        throws ServiceException {
      try {
        String tableId = name.getValue().toLowerCase();
        if (store.existTable(tableId)) {
          return BOOL_TRUE;
        } else {
          return BOOL_FALSE;
        }
      } catch (IOException e) {
        LOG.error(e);
        throw new ServiceException(e);
      }
    }

    @Override
    public BoolProto addIndex(RpcController controller, IndexDescProto indexDesc)
        throws ServiceException {
      rlock.lock();
      try {
        if (store.existIndex(indexDesc.getName())) {
          throw new AlreadyExistsIndexException(indexDesc.getName());
        }
        store.addIndex(indexDesc);
      } catch (IOException ioe) {
        LOG.error("ERROR : cannot add index " + indexDesc.getName(), ioe);
        LOG.error(indexDesc);
        throw new ServiceException(ioe);
      } finally {
        rlock.unlock();
      }

      return BOOL_TRUE;
    }

    @Override
    public BoolProto existIndexByName(RpcController controller,
                                      StringProto indexName)
        throws ServiceException {
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
    public BoolProto existIndex(RpcController controller,
                                GetIndexRequest request)
        throws ServiceException {
      rlock.lock();
      try {
        return BoolProto.newBuilder().setValue(
            store.existIndex(request.getTableName(),
                request.getColumnName())).build();
      } catch (IOException e) {
        LOG.error(e);
        return BoolProto.newBuilder().setValue(false).build();
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public IndexDescProto getIndexByName(RpcController controller,
                                         StringProto indexName)
        throws ServiceException {
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
    public IndexDescProto getIndex(RpcController controller,
                                   GetIndexRequest request)
        throws ServiceException {
      rlock.lock();
      try {
        if (!store.existIndex(request.getTableName())) {
          throw new NoSuchIndexException(request.getTableName() + "."
              + request.getColumnName());
        }
        return store.getIndex(request.getTableName(), request.getColumnName());
      } catch (IOException ioe) {
        LOG.error("ERROR : cannot get index " + request.getTableName() + "."
            + request.getColumnName(), ioe);
        return null;
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public BoolProto delIndex(RpcController controller, StringProto indexName)
        throws ServiceException {
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

      return BOOL_TRUE;
    }

    @Override
    public BoolProto registerFunction(RpcController controller,
                                      FunctionDescProto funcDesc)
        throws ServiceException {
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

      return BOOL_TRUE;
    }

    @Override
    public BoolProto unregisterFunction(RpcController controller,
                                        UnregisterFunctionRequest request)
        throws ServiceException {
      String signature = request.getSignature();
      List<DataType> paramTypes = new ArrayList<>();
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

      return BOOL_TRUE;
    }

    @Override
    public FunctionDescProto getFunctionMeta(RpcController controller,
                                             GetFunctionMetaRequest request)
        throws ServiceException {
      List<DataType> paramTypes = new ArrayList<>();
      int size = request.getParameterTypesCount();
      for (int i = 0; i < size; i++) {
        paramTypes.add(request.getParameterTypes(i));
      }
      return functions.get(TCatUtil.getCanonicalName(
          request.getSignature().toLowerCase(), paramTypes));
    }

    @Override
    public BoolProto containFunction(RpcController controller,
                                     ContainFunctionRequest request)
        throws ServiceException {
      List<DataType> paramTypes = new ArrayList<>();
      int size = request.getParameterTypesCount();
      for (int i = 0; i < size; i++) {
        paramTypes.add(request.getParameterTypes(i));
      }
      boolean returnValue =
          functions.containsKey(TCatUtil.getCanonicalName(
              request.getSignature().toLowerCase(), paramTypes));
      return BoolProto.newBuilder().setValue(returnValue).build();
    }
  }

  public static void main(String[] args) throws Exception {
    TajoConf conf = new TajoConf();
    CatalogServer catalog = new CatalogServer(new ArrayList<FunctionDesc>());
    catalog.init(conf);
    catalog.start();
  }
}
