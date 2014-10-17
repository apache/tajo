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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.tajo.catalog.CatalogProtocol.CatalogProtocolService;
import org.apache.tajo.catalog.exception.*;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.catalog.store.CatalogStore;
import org.apache.tajo.catalog.store.DerbyStore;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.rpc.BlockingRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.ProtoUtil;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceCommand;
import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionType.*;
import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringListProto;

/**
 * This class provides the catalog service. The catalog service enables clients
 * to register, unregister and access information about tables, functions, and
 * cluster information.
 */
@ThreadSafe
public class CatalogServer extends AbstractService {

  private final static String DEFAULT_NAMESPACE = "public";

  private final static Log LOG = LogFactory.getLog(CatalogServer.class);
  private TajoConf conf;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock rlock = lock.readLock();
  private final Lock wlock = lock.writeLock();

  private CatalogStore store;
  private Map<String, List<FunctionDescProto>> functions = new ConcurrentHashMap<String,
      List<FunctionDescProto>>();

  // RPC variables
  private BlockingRpcServer rpcServer;
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

  private Collection<FunctionDesc> builtingFuncs;

  public CatalogServer() throws IOException {
    super(CatalogServer.class.getName());
    this.handler = new CatalogProtocolHandler();
    this.builtingFuncs = new ArrayList<FunctionDesc>();
  }

  public CatalogServer(Collection<FunctionDesc> sqlFuncs) throws IOException {
    this();
    this.builtingFuncs = sqlFuncs;
  }

  public void reloadBuiltinFunctions(List<FunctionDesc> builtingFuncs) throws ServiceException {
    this.builtingFuncs = builtingFuncs;
    initBuiltinFunctions(builtingFuncs);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    Constructor<?> cons;
    try {
      if (conf instanceof TajoConf) {
        this.conf = (TajoConf) conf;
      } else {
        throw new CatalogException("conf must be a TajoConf instance");
      }

      Class<?> storeClass = this.conf.getClass(CatalogConstants.STORE_CLASS, DerbyStore.class);

      LOG.info("Catalog Store Class: " + storeClass.getCanonicalName());
      cons = storeClass.
          getConstructor(new Class [] {Configuration.class});

      this.store = (CatalogStore) cons.newInstance(this.conf);

      initBuiltinFunctions(builtingFuncs);
    } catch (Throwable t) {
      LOG.error("CatalogServer initialization failed", t);
      throw new CatalogException(t);
    }

    super.serviceInit(conf);
  }

  public TajoConf getConf() {
    return conf;
  }

  public String getStoreClassName() {
    return store.getClass().getCanonicalName();
  }

  public String getCatalogServerName() {
    String catalogUri = null;
    if(conf.get(CatalogConstants.DEPRECATED_CATALOG_URI) != null) {
      LOG.warn("Configuration parameter " + CatalogConstants.DEPRECATED_CATALOG_URI + " " +
          "is deprecated. Use " + CatalogConstants.CATALOG_URI + " instead.");
      catalogUri = conf.get(CatalogConstants.DEPRECATED_CATALOG_URI);
    } else {
      catalogUri = conf.get(CatalogConstants.CATALOG_URI);
    }

    return bindAddressStr + ", store=" + this.store.getClass().getSimpleName() + ", catalogUri="
        + catalogUri;
  }

  private void initBuiltinFunctions(Collection<FunctionDesc> functions)
      throws ServiceException {
    for (FunctionDesc desc : functions) {
      handler.createFunction(null, desc.getProto());
    }
  }

  public void start() {
    String serverAddr = conf.getVar(ConfVars.CATALOG_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(serverAddr);
    int workerNum = conf.getIntVar(ConfVars.CATALOG_RPC_SERVER_WORKER_THREAD_NUM);
    try {
      this.rpcServer = new BlockingRpcServer(CatalogProtocol.class, handler, initIsa, workerNum);
      this.rpcServer.start();

      this.bindAddress = NetUtils.getConnectAddress(this.rpcServer.getListenAddress());
      this.bindAddressStr = NetUtils.normalizeInetSocketAddress(bindAddress);
      conf.setVar(ConfVars.CATALOG_ADDRESS, bindAddressStr);
    } catch (Exception e) {
      LOG.error("CatalogServer startup failed", e);
      throw new CatalogException(e);
    }

    LOG.info("Catalog Server startup (" + bindAddressStr + ")");
    super.start();
  }

  public void stop() {
    LOG.info("Catalog Server (" + bindAddressStr + ") shutdown");

    // If CatalogServer shutdowns before it started, rpcServer and store may be NULL.
    // So, we should check Nullity of them.
    if (rpcServer != null) {
      this.rpcServer.shutdown();
    }
    if (store != null) {
      try {
        store.close();
      } catch (IOException ioe) {
        LOG.error(ioe.getMessage(), ioe);
      }
    }
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
    public BoolProto createTablespace(RpcController controller, CreateTablespaceRequest request) throws ServiceException {
      final String tablespaceName = request.getTablespaceName();
      final String uri = request.getTablespaceUri();

      wlock.lock();
      try {
        if (store.existTablespace(tablespaceName)) {
          throw new AlreadyExistsDatabaseException(tablespaceName);
        }

        store.createTablespace(tablespaceName, uri);
        LOG.info(String.format("tablespace \"%s\" (%s) is created", tablespaceName, uri));
        return ProtoUtil.TRUE;

      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        wlock.unlock();
      }
    }

    @Override
    public BoolProto dropTablespace(RpcController controller, StringProto request) throws ServiceException {
      String tablespaceName = request.getValue();

      wlock.lock();
      try {
        if (tablespaceName.equals(TajoConstants.DEFAULT_TABLESPACE_NAME)) {
          throw new CatalogException("default tablespace cannot be dropped.");
        }

        if (!store.existTablespace(tablespaceName)) {
          throw new NoSuchTablespaceException(tablespaceName);
        }

        store.dropTablespace(tablespaceName);
        return ProtoUtil.TRUE;

      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        wlock.unlock();
      }
    }

    @Override
    public BoolProto existTablespace(RpcController controller, StringProto request) throws ServiceException {
      String tablespaceName = request.getValue();

      rlock.lock();
      try {
        if (store.existTablespace(tablespaceName)) {
          return ProtoUtil.TRUE;
        } else {
          return ProtoUtil.FALSE;
        }
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public StringListProto getAllTablespaceNames(RpcController controller, NullProto request) throws ServiceException {
      rlock.lock();
      try {
        return ProtoUtil.convertStrings(store.getAllDatabaseNames());
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public TablespaceProto getTablespace(RpcController controller, StringProto request) throws ServiceException {
      rlock.lock();
      try {
        return store.getTablespace(request.getValue());
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public BoolProto alterTablespace(RpcController controller, AlterTablespaceProto request) throws ServiceException {
      wlock.lock();
      try {
        if (!store.existTablespace(request.getSpaceName())) {
          throw new NoSuchTablespaceException(request.getSpaceName());
        }

        if (request.getCommandList().size() > 0) {
          for (AlterTablespaceCommand command : request.getCommandList()) {
            if (command.getType() == AlterTablespaceProto.AlterTablespaceType.LOCATION) {
              try {
                URI uri = URI.create(command.getLocation().getUri());
                Preconditions.checkArgument(uri.getScheme() != null);
              } catch (Exception e) {
                throw new ServiceException("ALTER TABLESPACE's LOCATION must be a URI form (scheme:///.../), but "
                    + command.getLocation().getUri());
              }
            }
          }
        }

        store.alterTablespace(request);
        return ProtoUtil.TRUE;
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        wlock.unlock();
      }
    }

    @Override
    public BoolProto createDatabase(RpcController controller, CreateDatabaseRequest request) throws ServiceException {
      String databaseName = request.getDatabaseName();
      String tablespaceName = request.getTablespaceName();

      wlock.lock();
      try {
        if (store.existDatabase(databaseName)) {
          throw new AlreadyExistsDatabaseException(databaseName);
        }

        store.createDatabase(databaseName, tablespaceName);
        LOG.info(String.format("database \"%s\" is created", databaseName));
        return ProtoUtil.TRUE;
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        wlock.unlock();
      }
    }

    @Override
    public BoolProto alterTable(RpcController controller, AlterTableDescProto proto) throws ServiceException {
      wlock.lock();
      try {
        String [] split = CatalogUtil.splitTableName(proto.getTableName());
        if (!store.existTable(split[0], split[1])) {
          throw new NoSuchTableException(proto.getTableName());
        }
        store.alterTable(proto);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        return BOOL_FALSE;
      } finally {
        wlock.unlock();
        LOG.info("Table " + proto.getTableName() + " is altered in the catalog ("
            + bindAddressStr + ")");
      }
      return BOOL_TRUE;
    }

    @Override
    public BoolProto dropDatabase(RpcController controller, StringProto request) throws ServiceException {
      String databaseName = request.getValue();

      wlock.lock();
      try {
        if (!store.existDatabase(databaseName)) {
          throw new NoSuchDatabaseException(databaseName);
        }

        store.dropDatabase(databaseName);
        return ProtoUtil.TRUE;

      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        wlock.unlock();
      }
    }

    @Override
    public BoolProto existDatabase(RpcController controller, StringProto request) throws ServiceException {
      String databaseName = request.getValue();

      rlock.lock();
      try {
        if (store.existDatabase(databaseName)) {
          return ProtoUtil.TRUE;
        } else {
          return ProtoUtil.FALSE;
        }
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public StringListProto getAllDatabaseNames(RpcController controller, NullProto request) throws ServiceException {
      rlock.lock();
      try {
        return ProtoUtil.convertStrings(store.getAllDatabaseNames());
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public TableDescProto getTableDesc(RpcController controller,
                                       TableIdentifierProto request) throws ServiceException {
      String databaseName = request.getDatabaseName();
      String tableName = request.getTableName();

      rlock.lock();
      try {
        boolean contain;

        contain = store.existDatabase(databaseName);

        if (contain) {
          contain = store.existTable(databaseName, tableName);
          if (contain) {
            return store.getTable(databaseName, tableName);
          } else {
            throw new NoSuchTableException(tableName);
          }
        } else {
          throw new NoSuchDatabaseException(databaseName);
        }
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public StringListProto getAllTableNames(RpcController controller, StringProto request)
        throws ServiceException {

      String databaseName = request.getValue();

      rlock.lock();
      try {
        if (store.existDatabase(databaseName)) {
          return ProtoUtil.convertStrings(store.getAllTableNames(databaseName));
        } else {
          throw new NoSuchDatabaseException(databaseName);
        }
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
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
    public BoolProto createTable(RpcController controller, TableDescProto request)throws ServiceException {

      String [] splitted =
          CatalogUtil.splitFQTableName(request.getTableName());

      String databaseName = splitted[0];
      String tableName = splitted[1];

      wlock.lock();
      try {

        boolean contain = store.existDatabase(databaseName);

        if (contain) {
          if (store.existTable(databaseName, tableName)) {
            throw new AlreadyExistsTableException(databaseName, tableName);
          }

          store.createTable(request);
          LOG.info(String.format("relation \"%s\" is added to the catalog (%s)",
              CatalogUtil.getCanonicalTableName(databaseName, tableName), bindAddressStr));
        } else {
          throw new NoSuchDatabaseException(databaseName);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        return ProtoUtil.FALSE;
      } finally {
        wlock.unlock();
      }

      return ProtoUtil.TRUE;
    }

    @Override
    public BoolProto dropTable(RpcController controller, TableIdentifierProto request) throws ServiceException {

      String databaseName = request.getDatabaseName();
      String tableName = request.getTableName();

      wlock.lock();
      try {
        boolean contain = store.existDatabase(databaseName);

        if (contain) {
          if (!store.existTable(databaseName, tableName)) {
            throw new NoSuchTableException(databaseName, tableName);
          }

          store.dropTable(databaseName, tableName);
          LOG.info(String.format("relation \"%s\" is deleted from the catalog (%s)",
              CatalogUtil.getCanonicalTableName(databaseName, tableName), bindAddressStr));
        } else {
          throw new NoSuchDatabaseException(databaseName);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        return BOOL_FALSE;
      } finally {
        wlock.unlock();
      }

      return BOOL_TRUE;
    }

    @Override
    public BoolProto existsTable(RpcController controller, TableIdentifierProto request)
        throws ServiceException {
      String databaseName = request.getDatabaseName();
      String tableName = request.getTableName();

      rlock.lock();
      try {

        boolean contain = store.existDatabase(databaseName);

        if (contain) {
          if (store.existTable(databaseName, tableName)) {
            return BOOL_TRUE;
          } else {
            return BOOL_FALSE;
          }
        } else {
          throw new NoSuchDatabaseException(databaseName);
        }
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
      }

    }

    @Override
    public PartitionMethodProto getPartitionMethodByTableName(RpcController controller,
                                                              TableIdentifierProto request)
        throws ServiceException {
      String databaseName = request.getDatabaseName();
      String tableName = request.getTableName();

      rlock.lock();
      try {
        boolean contain;

        contain = store.existDatabase(databaseName);

        if (contain) {
          contain = store.existTable(databaseName, tableName);
          if (contain) {
            if (store.existPartitionMethod(databaseName, tableName)) {
              return store.getPartitionMethod(databaseName, tableName);
            } else {
              throw new NoPartitionedTableException(databaseName, tableName);
            }
          } else {
            throw new NoSuchTableException(databaseName);
          }
        } else {
          throw new NoSuchDatabaseException(databaseName);
        }
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public BoolProto existPartitionMethod(RpcController controller, TableIdentifierProto request)
        throws ServiceException {
      String databaseName = request.getDatabaseName();
      String tableName = request.getTableName();

      rlock.lock();
      try {
        boolean contain;

        contain = store.existDatabase(databaseName);

        if (contain) {
          contain = store.existTable(databaseName, tableName);
          if (contain) {
            if (store.existPartitionMethod(databaseName, tableName)) {
              return ProtoUtil.TRUE;
            } else {
              return ProtoUtil.FALSE;
            }
          } else {
            throw new NoSuchTableException(databaseName);
          }
        } else {
          throw new NoSuchDatabaseException(databaseName);
        }
      } catch (Exception e) {
        LOG.error(e);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public BoolProto dropPartitionMethod(RpcController controller, TableIdentifierProto request)
        throws ServiceException {
      return ProtoUtil.TRUE;
    }

    @Override
    public BoolProto addPartitions(RpcController controller, PartitionsProto request) throws ServiceException {
      return ProtoUtil.TRUE;
    }

    @Override
    public BoolProto addPartition(RpcController controller, PartitionDescProto request) throws ServiceException {
      return ProtoUtil.TRUE;
    }

    @Override
    public PartitionDescProto getPartitionByPartitionName(RpcController controller, StringProto request)
        throws ServiceException {
      return null;
    }

    @Override
    public PartitionsProto getPartitionsByTableName(RpcController controller,
                                                    StringProto request)
        throws ServiceException {
      return null;
    }

    @Override
    public PartitionsProto delAllPartitions(RpcController controller, StringProto request)
        throws ServiceException {
      return null;
    }

    @Override
    public BoolProto createIndex(RpcController controller, IndexDescProto indexDesc)
        throws ServiceException {
      rlock.lock();
      try {
        if (store.existIndexByName(
            indexDesc.getTableIdentifier().getDatabaseName(),
            indexDesc.getIndexName())) {
          throw new AlreadyExistsIndexException(indexDesc.getIndexName());
        }
        store.createIndex(indexDesc);
      } catch (Exception e) {
        LOG.error("ERROR : cannot add index " + indexDesc.getIndexName(), e);
        LOG.error(indexDesc);
        throw new ServiceException(e);
      } finally {
        rlock.unlock();
      }

      return BOOL_TRUE;
    }

    @Override
    public BoolProto existIndexByName(RpcController controller, IndexNameProto request) throws ServiceException {

      String databaseName = request.getDatabaseName();
      String indexName = request.getIndexName();

      rlock.lock();
      try {
        return store.existIndexByName(databaseName, indexName) ? ProtoUtil.TRUE : ProtoUtil.FALSE;
      } catch (Exception e) {
        LOG.error(e);
        return BoolProto.newBuilder().setValue(false).build();
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public BoolProto existIndexByColumn(RpcController controller, GetIndexByColumnRequest request)
        throws ServiceException {

      TableIdentifierProto identifier = request.getTableIdentifier();
      String databaseName = identifier.getDatabaseName();
      String tableName = identifier.getTableName();
      String columnName = request.getColumnName();

      rlock.lock();
      try {
        return store.existIndexByColumn(databaseName, tableName, columnName) ?
            ProtoUtil.TRUE : ProtoUtil.FALSE;
      } catch (Exception e) {
        LOG.error(e);
        return BoolProto.newBuilder().setValue(false).build();
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public IndexDescProto getIndexByName(RpcController controller, IndexNameProto request)
        throws ServiceException {

      String databaseName = request.getDatabaseName();
      String indexName = request.getIndexName();

      rlock.lock();
      try {
        if (!store.existIndexByName(databaseName, indexName)) {
          throw new NoSuchIndexException(databaseName, indexName);
        }
        return store.getIndexByName(databaseName, indexName);
      } catch (Exception e) {
        LOG.error("ERROR : cannot get index " + indexName, e);
        return null;
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public IndexDescProto getIndexByColumn(RpcController controller, GetIndexByColumnRequest request)
        throws ServiceException {

      TableIdentifierProto identifier = request.getTableIdentifier();
      String databaseName = identifier.getDatabaseName();
      String tableName = identifier.getTableName();
      String columnName = request.getColumnName();

      rlock.lock();
      try {
        if (!store.existIndexByColumn(databaseName, tableName, columnName)) {
          throw new NoSuchIndexException(databaseName, columnName);
        }
        return store.getIndexByColumn(databaseName, tableName, columnName);
      } catch (Exception e) {
        LOG.error("ERROR : cannot get index for " + tableName + "." + columnName, e);
        return null;
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public BoolProto dropIndex(RpcController controller, IndexNameProto request)
        throws ServiceException {

      String databaseName = request.getDatabaseName();
      String indexName = request.getIndexName();

      wlock.lock();
      try {
        if (!store.existIndexByName(databaseName, indexName)) {
          throw new NoSuchIndexException(indexName);
        }
        store.dropIndex(databaseName, indexName);
      } catch (Exception e) {
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

    private boolean containFunction(String signature, List<DataType> params) {
      return findFunction(signature, params) != null;
    }

    private boolean containFunction(String signature, FunctionType type, List<DataType> params) {
      return findFunction(signature, type, params, false) != null;
    }

    private List<FunctionDescProto> findFunction(String signature) {
      return functions.get(signature);
    }

    private FunctionDescProto findFunction(String signature, List<TajoDataTypes.DataType> params) {
      List<FunctionDescProto> candidates = Lists.newArrayList();

      if (functions.containsKey(signature)) {
        for (FunctionDescProto func : functions.get(signature)) {
          if (func.getSignature().getParameterTypesList() != null &&
              func.getSignature().getParameterTypesList().equals(params)) {
            candidates.add(func);
          }
        }
      }

      /*
       *
       * FALL BACK to look for nearest match
       * WORKING BUT BAD WAY TO IMPLEMENT.I WOULD RATHER implement compareTo in FunctionDesc to keep them
       * in sorted order of param types LIKE INT1 SHOULD BE BEFORE INT2 should be before INT3 so on.
       * Due to possibility of multiple parameters and types the permutation and combinations are endless
       * to implement compareTo so decided to take the shortcut.
       *
       * */
      if (functions.containsKey(signature)) {
        for (FunctionDescProto func : functions.get(signature)) {
          if (func.getSignature().getParameterTypesList() != null &&
              CatalogUtil.isMatchedFunction(func.getSignature().getParameterTypesList(), params)) {
            candidates.add(func);
          }
        }

        // if there are more than one function candidates, we choose the nearest matched function.
        if (candidates.size() > 0) {
          return findNearestMatchedFunction(candidates);
        } else {
          return null;
        }
      }

      return null;
    }

    private FunctionDescProto findFunction(String signature, FunctionType type, List<TajoDataTypes.DataType> params,
                                           boolean strictTypeCheck) {
      List<FunctionDescProto> candidates = Lists.newArrayList();

      if (functions.containsKey(signature)) {
        if (strictTypeCheck) {
          for (FunctionDescProto func : functions.get(signature)) {
            if (func.getSignature().getType() == type &&
                func.getSignature().getParameterTypesList().equals(params)) {
              candidates.add(func);
            }
          }
        } else {
          for (FunctionDescProto func : functions.get(signature)) {
            if (func.getSignature().getParameterTypesList() != null &&
                CatalogUtil.isMatchedFunction(func.getSignature().getParameterTypesList(), params)) {
              candidates.add(func);
            }
          }
        }
      }

      // if there are more than one function candidates, we choose the nearest matched function.
      if (candidates.size() > 0) {
        return findNearestMatchedFunction(candidates);
      } else {
        return null;
      }
    }

    /**
     * Find the nearest matched function
     *
     * @param candidates Candidate Functions
     * @return
     */
    private FunctionDescProto findNearestMatchedFunction(List<FunctionDescProto> candidates) {
      Collections.sort(candidates, new NearestParamsComparator());
      return candidates.get(0);
    }

    private class NearestParamsComparator implements Comparator<FunctionDescProto> {
      @Override
      public int compare(FunctionDescProto o1, FunctionDescProto o2) {
        List<DataType> types1 = o1.getSignature().getParameterTypesList();
        List<DataType> types2 = o2.getSignature().getParameterTypesList();

        int minLen = Math.min(types1.size(), types2.size());

        for (int i = 0; i < minLen; i++) {
          int cmpVal = types1.get(i).getType().getNumber() - types2.get(i).getType().getNumber();

          if (cmpVal != 0) {
            return cmpVal;
          }
        }

        return types1.size() - types2.size();
      }
    }

    private FunctionDescProto findFunctionStrictType(FunctionDescProto target, boolean strictTypeCheck) {
      return findFunction(target.getSignature().getName(), target.getSignature().getType(),
          target.getSignature().getParameterTypesList(), strictTypeCheck);
    }

    @Override
    public BoolProto createFunction(RpcController controller, FunctionDescProto funcDesc)
        throws ServiceException {
      FunctionSignature signature = FunctionSignature.create(funcDesc);

      if (functions.containsKey(funcDesc.getSignature())) {
        FunctionDescProto found = findFunctionStrictType(funcDesc, true);
        if (found != null) {
          throw new AlreadyExistsFunctionException(signature.toString());
        }
      }

      TUtil.putToNestedList(functions, funcDesc.getSignature().getName(), funcDesc);
      if (LOG.isDebugEnabled()) {
        LOG.info("Function " + signature + " is registered.");
      }

      return BOOL_TRUE;
    }

    @Override
    public BoolProto dropFunction(RpcController controller, UnregisterFunctionRequest request)
        throws ServiceException {

      if (!containFunction(request.getSignature())) {
        throw new NoSuchFunctionException(request.getSignature(), new DataType[] {});
      }

      functions.remove(request.getSignature());
      LOG.info(request.getSignature() + " is dropped.");

      return BOOL_TRUE;
    }

    @Override
    public FunctionDescProto getFunctionMeta(RpcController controller, GetFunctionMetaRequest request)
        throws ServiceException {
      FunctionDescProto function = null;
      if (request.hasFunctionType()) {
        if (containFunction(request.getSignature(), request.getFunctionType(), request.getParameterTypesList())) {
          function = findFunction(request.getSignature(), request.getFunctionType(), request.getParameterTypesList(),true);
        }
      } else {
        function = findFunction(request.getSignature(), request.getParameterTypesList());
      }

      if (function == null) {
        throw new NoSuchFunctionException(request.getSignature(), request.getParameterTypesList());
      } else {
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
        returnValue = containFunction(request.getSignature(), request.getParameterTypesList());
      }
      return BoolProto.newBuilder().setValue(returnValue).build();
    }
  }

  private static class FunctionSignature {
    private String signature;
    private FunctionType type;
    private DataType [] arguments;

    public FunctionSignature(String signature, FunctionType type, List<DataType> arguments) {
      this.signature = signature;
      this.type = type;
      this.arguments = arguments.toArray(new DataType[arguments.size()]);
    }

    public static FunctionSignature create(FunctionDescProto proto) {
      return new FunctionSignature(proto.getSignature().getName(),
          proto.getSignature().getType(), proto.getSignature().getParameterTypesList());
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
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o == null || getClass() != o.getClass()) {
        return false;
      } else {
        return (signature.equals(((FunctionSignature) o).signature)
            && type.equals(((FunctionSignature) o).type)
            && Arrays.equals(arguments, ((FunctionSignature) o).arguments));
      }
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(signature, type, Objects.hashCode(arguments));
    }

  }

  public static void main(String[] args) throws Exception {
    TajoConf conf = new TajoConf();
    CatalogServer catalog = new CatalogServer(new ArrayList<FunctionDesc>());
    catalog.init(conf);
    catalog.start();
  }
}
