/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tajo.catalog.CatalogProtocol.CatalogProtocolService;
import org.apache.tajo.catalog.exception.*;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.catalog.store.CatalogStore;
import org.apache.tajo.catalog.store.DBStore;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.rpc.ProtoBlockingRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionType.*;

/**
 * This class provides the catalog service. The catalog service enables clients
 * to register, unregister and access information about tables, functions, and
 * cluster information.
 */
public class CatalogServer extends AbstractService {
  private final static Log LOG = LogFactory.getLog(CatalogServer.class);
  private TajoConf conf;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock rlock = lock.readLock();
  private final Lock wlock = lock.writeLock();

  private CatalogStore store;
  private Map<String, List<FunctionDescProto>> functions = new ConcurrentHashMap<String,
      List<FunctionDescProto>>();

  // RPC variables
  private ProtoBlockingRpcServer rpcServer;
  private InetSocketAddress bindAddress;
  private String bindAddressStr;
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
    this.builtingFuncs = new ArrayList<FunctionDesc>();
  }

  public CatalogServer(List<FunctionDesc> sqlFuncs) throws IOException {
    this();
    this.builtingFuncs = sqlFuncs;
  }

  @Override
  public void init(Configuration _conf) {
    this.conf = (TajoConf)_conf;

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
      LOG.error("CatalogServer initialization failed", t);
      throw new CatalogException(t);
    }

    super.init(conf);
  }

  private void initBuiltinFunctions(List<FunctionDesc> functions)
      throws ServiceException {
    for (FunctionDesc desc : functions) {
      handler.createFunction(null, desc.getProto());
    }
  }

  public void start() {
    String serverAddr = conf.getVar(ConfVars.CATALOG_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(serverAddr);
    try {
      this.rpcServer = new ProtoBlockingRpcServer(
          CatalogProtocol.class,
          handler, initIsa);
      this.rpcServer.start();

      this.bindAddress = NetUtils.getConnectAddress(this.rpcServer.getListenAddress());
      this.bindAddressStr = NetUtils.normalizeInetSocketAddress(bindAddress);
      conf.set(ConfVars.CATALOG_ADDRESS.varname, bindAddressStr);
    } catch (Exception e) {
      LOG.error("CatalogServer startup failed", e);
      throw new CatalogException(e);
    }

    LOG.info("Catalog Server startup (" + bindAddressStr + ")");
    super.start();
  }

  public void stop() {
    if (rpcServer != null) {
      this.rpcServer.shutdown();
    }
    LOG.info("Catalog Server (" + bindAddressStr + ") shutdown");
    super.stop();
  }

  public CatalogProtocolHandler getHandler() {
    return this.handler;
  }

  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  public class CatalogProtocolHandler implements CatalogProtocolService.BlockingInterface {

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
        TableDesc desc = store.getTable(tableId);
        SchemaProto schemaProto = desc.getMeta().getSchema().getProto();
        SchemaProto qualifiedSchema = CatalogUtil.getQualfiedSchema(tableId, schemaProto);
        desc.getMeta().setSchema(new Schema(qualifiedSchema));
        return (TableDescProto) desc.getProto();
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
      Iterator<List<FunctionDescProto>> iterator = functions.values().iterator();
      GetFunctionsResponse.Builder builder = GetFunctionsResponse.newBuilder();
      while (iterator.hasNext()) {
        builder.addAllFunctionDesc(iterator.next());
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
        TableProto.Builder metaBuilder = TableProto.newBuilder(tableDesc.getMeta());
        metaBuilder.setSchema(tableDesc.getMeta().getSchema());
        TableDescProto.Builder descBuilder = TableDescProto.newBuilder(tableDesc);
        descBuilder.setMeta(metaBuilder.build());

        store.addTable(new TableDescImpl(descBuilder.build()));

      } catch (IOException ioe) {
        LOG.error(ioe.getMessage(), ioe);
        return BOOL_FALSE;
      } finally {
        wlock.unlock();
        LOG.info("Table " + tableDesc.getId() + " is added to the catalog ("
            + bindAddressStr + ")");
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
        LOG.error(ioe.getMessage(), ioe);
        return BOOL_FALSE;
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

    public boolean checkIfBuiltin(FunctionType type) {
      return type == GENERAL || type == AGGREGATION || type == DISTINCT_AGGREGATION;
    }

    private boolean containFunction(String signature) {
      List<FunctionDescProto> found = findFunction(signature);
      return found != null && found.size() > 0;
    }

    private boolean containFunction(String signature, FunctionType type, List<DataType> params) {
      return findFunction(signature, type, params) != null;
    }

    private List<FunctionDescProto> findFunction(String signature) {
      return functions.get(signature);
    }

    private FunctionDescProto findFunction(String signature, List<DataType> params) {
      if (functions.containsKey(signature)) {
        for (FunctionDescProto existing : functions.get(signature)) {
          if (existing.getParameterTypesList().containsAll(params) &&
              params.containsAll(existing.getParameterTypesList())) {
            return existing;
          }
        }
      }
      return null;
    }

    private FunctionDescProto findFunction(String signature, FunctionType type, List<DataType> params) {
      if (functions.containsKey(signature)) {
        for (FunctionDescProto existing : functions.get(signature)) {
          if (existing.getType() == type &&
              existing.getParameterTypesList().containsAll(params)
              && params.containsAll(existing.getParameterTypesList())) {
            return existing;
          }
        }
      }
      return null;
    }

    private FunctionDescProto findFunction(FunctionDescProto target) {
      return findFunction(target.getSignature(), target.getType(), target.getParameterTypesList());
    }

    @Override
    public BoolProto createFunction(RpcController controller, FunctionDescProto funcDesc)
        throws ServiceException {
      FunctionSignature signature = FunctionSignature.create(funcDesc);

      if (functions.containsKey(funcDesc.getSignature())) {
        FunctionDescProto found = findFunction(funcDesc);
        if (found != null) {
          throw new AlreadyExistsFunctionException(signature.toString());
        }
      }

      TUtil.putToNestedList(functions, funcDesc.getSignature(), funcDesc);
      if (LOG.isDebugEnabled()) {
        LOG.info("Function " + signature + " is registered.");
      }

      return BOOL_TRUE;
    }

    @Override
    public BoolProto dropFunction(RpcController controller, UnregisterFunctionRequest request)
        throws ServiceException {

      if (!containFunction(request.getSignature())) {
        throw new NoSuchFunctionException(request.getSignature());
      }

      functions.remove(request.getSignature());
      LOG.info(request.getSignature() + " is dropped.");

      return BOOL_TRUE;
    }

    @Override
    public FunctionDescProto getFunctionMeta(RpcController controller, GetFunctionMetaRequest request)
        throws ServiceException {
      if (request.hasFunctionType()) {
        if (containFunction(request.getSignature(), request.getFunctionType(), request.getParameterTypesList())) {
          FunctionDescProto desc = findFunction(request.getSignature(), request.getFunctionType(),
              request.getParameterTypesList());
          return desc;
        }
        return null;
      } else {
        FunctionDescProto function = findFunction(request.getSignature(), request.getParameterTypesList());
        return function;
      }
    }

    @Override
    public BoolProto containFunction(RpcController controller, ContainFunctionRequest request)
        throws ServiceException {
      boolean returnValue;
      if (request.hasFunctionType()) {
        returnValue = containFunction(request.getSignature(), request.getFunctionType(),
            request.getParameterTypesList());
      } else {
        returnValue = containFunction(request.getSignature());
      }

      return BoolProto.newBuilder().setValue(returnValue).build();
    }
  }

  private static class FunctionSignature implements Comparable<FunctionSignature> {
    private String signature;
    private FunctionType type;
    private DataType [] arguments;

    public FunctionSignature(String signature, FunctionType type, List<DataType> arguments) {
      this.signature = signature;
      this.type = type;
      this.arguments = arguments.toArray(new DataType[arguments.size()]);
    }

    public static FunctionSignature create(FunctionDescProto proto) {
      return new FunctionSignature(proto.getSignature(), proto.getType(), proto.getParameterTypesList());
    }

    public static FunctionSignature create (GetFunctionMetaRequest proto) {
      return new FunctionSignature(proto.getSignature(), proto.getFunctionType(), proto.getParameterTypesList());
    }

    public static FunctionSignature create(ContainFunctionRequest proto) {
      return new FunctionSignature(proto.getSignature(), proto.getFunctionType(), proto.getParameterTypesList());
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(signature);
      sb.append("#").append(type.name());
      sb.append("(");
      int i = 0;
      for (DataType type : arguments) {
        sb.append(type.getType());
        sb.append("[").append(type.getLength()).append("]");
        if(i < arguments.length - 1) {
          sb.append(",");
        }
        i++;
      }
      sb.append(")");

      return sb.toString();
    }

    @Override
    public int hashCode() {
      return com.google.common.base.Objects.hashCode(signature, type, arguments);
    }

    @Override
    public int compareTo(FunctionSignature o) {
      int signatureCmp = signature.compareTo(o.signature);
      if (signatureCmp != 0) {
        return signatureCmp;
      }

      int typeCmp = type.compareTo(o.type);
      if (typeCmp != 0) {
        return typeCmp;
      }

      int min = Math.min(arguments.length, o.arguments.length);
      int argCmp = 0;
      for (int i = 0; i < min; i++) {
        if (arguments.length < min && o.arguments.length < min) {
          argCmp = arguments[i].getType().compareTo(o.arguments[i].getType());

          if (argCmp != 0) {
            return argCmp;
          }
        } else {
          argCmp = arguments.length - o.arguments.length;
        }
      }
      return argCmp;
    }
  }

  public static void main(String[] args) throws Exception {
    TajoConf conf = new TajoConf();
    CatalogServer catalog = new CatalogServer(new ArrayList<FunctionDesc>());
    catalog.init(conf);
    catalog.start();
  }
}
