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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.tajo.catalog.CatalogProtocol.*;
import org.apache.tajo.catalog.dictionary.InfoSchemaMetadataDictionary;
import org.apache.tajo.exception.*;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.catalog.store.CatalogStore;
import org.apache.tajo.catalog.store.DerbyStore;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.rpc.BlockingRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringListResponse;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.Pair;
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
import static org.apache.tajo.exception.ExceptionUtil.printStackTraceIfError;
import static org.apache.tajo.exception.ReturnStateUtil.*;
import static org.apache.tajo.function.FunctionUtil.buildSimpleFunctionSignature;

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

  protected LinkedMetadataManager linkedMetadataManager;
  protected final InfoSchemaMetadataDictionary metaDictionary = new InfoSchemaMetadataDictionary();

  // RPC variables
  private BlockingRpcServer rpcServer;
  private InetSocketAddress bindAddress;
  private String bindAddressStr;
  final CatalogProtocolHandler handler;

  private Collection<FunctionDesc> builtingFuncs;

  public CatalogServer() throws IOException {
    super(CatalogServer.class.getName());
    this.handler = new CatalogProtocolHandler();
    this.linkedMetadataManager = new LinkedMetadataManager(Collections.EMPTY_LIST);
    this.builtingFuncs = new ArrayList<FunctionDesc>();
  }

  public CatalogServer(Collection<MetadataProvider> metadataProviders, Collection<FunctionDesc> sqlFuncs)
      throws IOException {
    super(CatalogServer.class.getName());
    this.handler = new CatalogProtocolHandler();
    this.linkedMetadataManager = new LinkedMetadataManager(metadataProviders);
    this.builtingFuncs = sqlFuncs;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    Constructor<?> cons;
    try {
      if (conf instanceof TajoConf) {
        this.conf = (TajoConf) conf;
      } else {
        throw new TajoInternalError("conf must be a TajoConf instance");
      }

      Class<?> storeClass = this.conf.getClass(CatalogConstants.STORE_CLASS, DerbyStore.class);

      LOG.info("Catalog Store Class: " + storeClass.getCanonicalName());
      cons = storeClass.
          getConstructor(new Class [] {Configuration.class});

      this.store = (CatalogStore) cons.newInstance(this.conf);

      initBuiltinFunctions(builtingFuncs);
    } catch (Throwable t) {
      LOG.error("CatalogServer initialization failed", t);
      throw new TajoInternalError(t);
    }

    super.serviceInit(conf);
  }

  public TajoConf getConf() {
    return conf;
  }

  public String getStoreClassName() {
    return store.getClass().getCanonicalName();
  }

  public String getStoreUri() {
    return store.getUri();
  }

  private void initBuiltinFunctions(Collection<FunctionDesc> functions)
      throws ServiceException {
    for (FunctionDesc desc : functions) {
      handler.createFunction(null, desc.getProto());
    }
  }

  @Override
  public void serviceStart() throws Exception {
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
      throw new TajoInternalError(e);
    }

    LOG.info("Catalog Server startup (" + bindAddressStr + ")");
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    LOG.info("Catalog Server (" + bindAddressStr + ") shutdown");

    // If CatalogServer shutdowns before it started, rpcServer and store may be NULL.
    // So, we should check Nullity of them.
    if (rpcServer != null) {
      this.rpcServer.shutdown();
    }
    if (store != null) {
      store.close();
    }
    super.serviceStop();
  }

  /**
   * Refresh the linked metadata manager. This must be used for only testing.
   * @param metadataProviders
   */
  @VisibleForTesting
  public void refresh(Collection<MetadataProvider> metadataProviders) {
    this.linkedMetadataManager = new LinkedMetadataManager(metadataProviders);
  }

  public CatalogProtocolHandler getHandler() {
    return this.handler;
  }

  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  public class CatalogProtocolHandler implements CatalogProtocolService.BlockingInterface {

    @Override
    public ReturnState createTablespace(RpcController controller, CreateTablespaceRequest request) {

      final String tablespaceName = request.getTablespaceName();
      final String uri = request.getTablespaceUri();

      wlock.lock();
      try {

        store.createTablespace(tablespaceName, uri);
        LOG.info(String.format("tablespace \"%s\" (%s) is created", tablespaceName, uri));

        return OK;
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      } finally {
        wlock.unlock();
      }
    }

    @Override
    public ReturnState dropTablespace(RpcController controller, StringProto request) {
      String tablespaceName = request.getValue();

      wlock.lock();
      try {
        if (tablespaceName.equals(TajoConstants.DEFAULT_TABLESPACE_NAME)) {
          throw new InsufficientPrivilegeException("drop to default tablespace");
        }

        store.dropTablespace(tablespaceName);
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      } finally {
        wlock.unlock();
      }
    }

    @Override
    public ReturnState existTablespace(RpcController controller, StringProto request) {
      String spaceName = request.getValue();

      rlock.lock();
      try {
        if (store.existTablespace(spaceName)) {
          return OK;
        } else {
          return errUndefinedTablespace(spaceName);
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public StringListResponse getAllTablespaceNames(RpcController controller, NullProto request) throws ServiceException {
      rlock.lock();
      try {
        return StringListResponse.newBuilder()
            .setState(OK)
            .addAllValues(linkedMetadataManager.getTablespaceNames())
            .addAllValues(store.getAllTablespaceNames())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnFailedStringList(t);
      } finally {
        rlock.unlock();
      }
    }
    
    @Override
    public GetTablespaceListResponse getAllTablespaces(RpcController controller, NullProto request)
        throws ServiceException {
      rlock.lock();
      try {

        // retrieves tablespaces from catalog store
        final List<TablespaceProto> tableSpaces = Lists.newArrayList(store.getTablespaces());

        // retrieves tablespaces from linked meta data
        tableSpaces.addAll(Collections2.transform(linkedMetadataManager.getTablespaces(),
            new Function<Pair<String, URI>, TablespaceProto>() {
              @Override
              public TablespaceProto apply(Pair<String, URI> input) {
                return TablespaceProto.newBuilder()
                    .setSpaceName(input.getFirst())
                    .setUri(input.getSecond().toString())
                    .build();
              }
            }));

        return GetTablespaceListResponse.newBuilder()
            .setState(OK)
            .addAllTablespace(tableSpaces)
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        throw new ServiceException(t);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public GetTablespaceResponse getTablespace(RpcController controller, StringProto request) {
      rlock.lock();

      try {

        // if there exists the tablespace in linked meta data
        Optional<Pair<String, URI>> optional = linkedMetadataManager.getTablespace(request.getValue());

        if (optional.isPresent()) {
          Pair<String, URI> spaceInfo = optional.get();
          return GetTablespaceResponse.newBuilder()
              .setState(OK)
              .setTablespace(TablespaceProto.newBuilder()
                  .setSpaceName(spaceInfo.getFirst())
                  .setUri(spaceInfo.getSecond().toString())
              ).build();
        }

        return GetTablespaceResponse.newBuilder()
            .setState(OK)
            .setTablespace(store.getTablespace(request.getValue()))
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return GetTablespaceResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public ReturnState alterTablespace(RpcController controller, AlterTablespaceProto request) {
      wlock.lock();
      try {

        if (linkedMetadataManager.getTablespace(request.getSpaceName()).isPresent()) {
          return errInsufficientPrivilege("alter tablespace '"+request.getSpaceName()+"'");
        }

        if (request.getCommandList().size() > 0) {
          for (AlterTablespaceCommand command : request.getCommandList()) {
            if (command.getType() == AlterTablespaceProto.AlterTablespaceType.LOCATION) {
              try {
                URI uri = URI.create(command.getLocation());
                Preconditions.checkArgument(uri.getScheme() != null);
              } catch (Exception e) {
                throw new ServiceException("ALTER TABLESPACE's LOCATION must be a URI form (scheme:///.../), but "
                    + command.getLocation());
              }
            }
          }
        }

        store.alterTablespace(request);

        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        wlock.unlock();
      }
    }

    @Override
    public ReturnState createDatabase(RpcController controller, CreateDatabaseRequest request) {
      String databaseName = request.getDatabaseName();
      String tablespaceName = request.getTablespaceName();

      if (linkedMetadataManager.existsDatabase(request.getDatabaseName())) {
        return errDuplicateDatabase(request.getDatabaseName());
      }

      // check virtual database manually because catalog actually does not contain them.
      if (metaDictionary.isSystemDatabase(databaseName)) {
        return errDuplicateDatabase(databaseName);
      }
      
      wlock.lock();
      try {
        store.createDatabase(databaseName, tablespaceName);
        LOG.info(String.format("database \"%s\" is created", databaseName));

        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        wlock.unlock();
      }
    }

    @Override
    public ReturnState updateTableStats(RpcController controller, UpdateTableStatsProto proto) {

      wlock.lock();

      try {
        store.updateTableStats(proto);
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        wlock.unlock();
      }
    }

    @Override
    public ReturnState alterTable(RpcController controller, AlterTableDescProto proto) {
      String [] split = CatalogUtil.splitTableName(proto.getTableName());

      if (linkedMetadataManager.existsDatabase(split[0])) {
        return errInsufficientPrivilege("alter a table in database '" + split[0] + "'");
      }

      if (metaDictionary.isSystemDatabase(split[0])) {
        return errInsufficientPrivilege("alter a table in database '" + split[0] + "'");
      }
      
      wlock.lock();

      try {
        store.alterTable(proto);
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        wlock.unlock();
      }
    }

    @Override
    public ReturnState dropDatabase(RpcController controller, StringProto request) {
      String databaseName = request.getValue();

      if (linkedMetadataManager.existsDatabase(databaseName)) {
        return errInsufficientPrivilege("drop a table in database '" + databaseName + "'");
      }

      if (metaDictionary.isSystemDatabase(databaseName)) {
        return errInsufficientPrivilege("drop a table in database '" + databaseName + "'");
      }

      if (databaseName.equals(TajoConstants.DEFAULT_DATABASE_NAME)) {
        return errInsufficientPrivilege("drop a table in database '" + databaseName + "'");
      }

      wlock.lock();
      try {
        store.dropDatabase(databaseName);
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      } finally {
        wlock.unlock();
      }
    }

    @Override
    public ReturnState existDatabase(RpcController controller, StringProto request) {
      String dbName = request.getValue();

      if (linkedMetadataManager.existsDatabase(dbName)) {
        return OK;
      }

      if (metaDictionary.isSystemDatabase(dbName)) {
        return OK;
      }

      rlock.lock();
      try {
        if (store.existDatabase(dbName)) {
          return OK;
        } else {
          return errUndefinedDatabase(dbName);
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public StringListResponse getAllDatabaseNames(RpcController controller, NullProto request) {
      rlock.lock();
      try {
        return StringListResponse.newBuilder()
            .setState(OK)
            .addAllValues(linkedMetadataManager.getDatabases())
            .addValues(metaDictionary.getSystemDatabaseName())
            .addAllValues(store.getAllDatabaseNames())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnFailedStringList(t);

      } finally {
        rlock.unlock();
      }
    }
    
    @Override
    public GetDatabasesResponse getAllDatabases(RpcController controller, NullProto request) throws ServiceException {
      rlock.lock();
      try {
        return GetDatabasesResponse.newBuilder()
            .setState(OK)
            .addAllDatabase(store.getAllDatabases())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return GetDatabasesResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public TableResponse getTableDesc(RpcController controller,
                                      TableIdentifierProto request) throws ServiceException {
      String dbName = request.getDatabaseName();
      String tbName = request.getTableName();

      try {
        if (linkedMetadataManager.existsDatabase(dbName)) {
          return TableResponse.newBuilder()
              .setState(OK)
              .setTable(linkedMetadataManager.getTable(dbName, "", tbName).getProto())
              .build();
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return TableResponse.newBuilder().setState(returnError(t)).build();
      }

      if (metaDictionary.isSystemDatabase(dbName)) {
        try {
          return TableResponse.newBuilder()
              .setState(OK)
              .setTable(metaDictionary.getTableDesc(tbName))
              .build();
        } catch (UndefinedTableException e) {
          return TableResponse.newBuilder()
              .setState(errUndefinedTable(tbName))
              .build();
        }
      }

      rlock.lock();
      try {
        return TableResponse.newBuilder()
            .setState(OK)
            .setTable(store.getTable(dbName, tbName))
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return TableResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public StringListResponse getAllTableNames(RpcController controller, StringProto request) {

      String dbName = request.getValue();

      try {
        if (linkedMetadataManager.existsDatabase(dbName)) {
          return returnStringList(linkedMetadataManager.getTableNames(dbName, null, null));
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnFailedStringList(t);
      }

      if (metaDictionary.isSystemDatabase(dbName)) {
        return returnStringList(metaDictionary.getAllSystemTables());
      }

      rlock.lock();
      try {
        return returnStringList(store.getAllTableNames(dbName));

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnFailedStringList(t);

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
      builder.setState(OK);
      while (iterator.hasNext()) {
        builder.addAllFunctionDesc(iterator.next());
      }
      return builder.build();
    }

    @Override
    public ReturnState createTable(RpcController controller, TableDescProto request) {

      String [] splitted = CatalogUtil.splitFQTableName(request.getTableName());

      String dbName = splitted[0];
      String tbName = splitted[1];

      if (linkedMetadataManager.existsDatabase(dbName)) {
        return errInsufficientPrivilege("drop a table in database '" + dbName + "'");
      }

      if (metaDictionary.isSystemDatabase(dbName)) {
        return errInsufficientPrivilege("create a table in database '" + dbName + "'");
      }

      wlock.lock();
      try {
        store.createTable(request);
        LOG.info(String.format("relation \"%s\" is added to the catalog (%s)",
            CatalogUtil.getCanonicalTableName(dbName, tbName), bindAddressStr));
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        wlock.unlock();
      }
    }

    @Override
    public ReturnState dropTable(RpcController controller, TableIdentifierProto request) throws ServiceException {

      String dbName = request.getDatabaseName();
      String tbName = request.getTableName();

      if (linkedMetadataManager.existsDatabase(dbName)) {
        return errInsufficientPrivilege("drop a table in database '" + dbName + "'");
      }

      if (metaDictionary.isSystemDatabase(dbName)) {
        return errInsufficientPrivilege("drop a table in database '" + dbName + "'");
      }

      wlock.lock();
      try {
        store.dropTable(dbName, tbName);
        LOG.info(String.format("relation \"%s\" is deleted from the catalog (%s)",
            CatalogUtil.getCanonicalTableName(dbName, tbName), bindAddressStr));

        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        wlock.unlock();
      }
    }

    @Override
    public ReturnState existsTable(RpcController controller, TableIdentifierProto request) {
      String dbName = request.getDatabaseName();
      String tbName = request.getTableName();

      if (linkedMetadataManager.existsDatabase(dbName)) {
        return linkedMetadataManager.existsTable(dbName, "", tbName) ? OK : errUndefinedTable(tbName);
      }

      if (metaDictionary.isSystemDatabase(dbName)) {
        return metaDictionary.existTable(tbName) ? OK : errUndefinedTable(tbName);

      } else {
        rlock.lock();
        try {

          if (store.existTable(dbName, tbName)) {
            return OK;
          } else {
            return errUndefinedTable(tbName);
          }

        } catch (Throwable t) {
          printStackTraceIfError(LOG, t);
          return returnError(t);

        } finally {
          rlock.unlock();
        }
      }
    }
    
    @Override
    public GetTablesResponse getAllTables(RpcController controller, NullProto request) throws ServiceException {
      rlock.lock();
      try {
        return GetTablesResponse.newBuilder()
            .setState(OK)
            .addAllTable(store.getAllTables())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return GetTablesResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }
    
    @Override
    public GetTablePropertiesResponse getAllTableProperties(RpcController controller, NullProto request) {
      rlock.lock();
      try {
        return GetTablePropertiesResponse.newBuilder()
        .setState(OK)
        .addAllProperties(store.getAllTableProperties())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return GetTablePropertiesResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }
    
    @Override
    public GetTableStatsResponse getAllTableStats(RpcController controller, NullProto request) {
      rlock.lock();
      try {
        return GetTableStatsResponse.newBuilder()
            .addAllStats(store.getAllTableStats())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return GetTableStatsResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }
    
    @Override
    public GetColumnsResponse getAllColumns(RpcController controller, NullProto request) throws ServiceException {
      rlock.lock();
      try {
        return GetColumnsResponse
            .newBuilder()
            .setState(OK)
            .addAllColumn(store.getAllColumns())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return GetColumnsResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public GetPartitionMethodResponse getPartitionMethodByTableName(RpcController controller,
                                                              TableIdentifierProto request)
        throws ServiceException {
      String dbName = request.getDatabaseName();
      String tbName = request.getTableName();

      try {
        // linked meta data do not support partition.
        // So, the request that wants to get partitions in this db will be failed.
        if (linkedMetadataManager.existsDatabase(dbName)) {
          return GetPartitionMethodResponse.newBuilder().setState(errUndefinedPartitionMethod(tbName))
              .build();
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return GetPartitionMethodResponse.newBuilder().setState(returnError(t)).build();
      }

      if (metaDictionary.isSystemDatabase(dbName)) {
        return GetPartitionMethodResponse.newBuilder().setState(errUndefinedPartitionMethod(tbName)).build();
      }
      
      rlock.lock();
      try {

        return GetPartitionMethodResponse.newBuilder()
            .setState(OK)
            .setPartition(store.getPartitionMethod(dbName, tbName))
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return GetPartitionMethodResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public ReturnState existPartitionMethod(RpcController controller, TableIdentifierProto request) {
      String dbName = request.getDatabaseName();
      String tableName = request.getTableName();

      try {
        // linked meta data do not support partition.
        // So, the request that wants to get partitions in this db will be failed.
        if (linkedMetadataManager.existsDatabase(dbName)) {
          return errUndefinedPartitionMethod(tableName);
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }

      if (metaDictionary.isSystemDatabase(dbName)) {
        ReturnStateUtil.errFeatureNotSupported("partition feature in virtual tables");
      }

      rlock.lock();
      try {
        if (store.existPartitionMethod(dbName, tableName)) {
          return OK;
        } else {
          return errUndefinedPartitionMethod(tableName);
        }

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public GetPartitionDescResponse getPartitionByPartitionName(RpcController controller,
                                                                PartitionIdentifierProto request)
        throws ServiceException {
      String dbName = request.getDatabaseName();
      String tbName = request.getTableName();
      String partitionName = request.getPartitionName();

      try {
        // linked meta data do not support partition.
        // So, the request that wants to get partitions in this db will be failed.
        if (linkedMetadataManager.existsDatabase(dbName)) {
          return GetPartitionDescResponse.newBuilder().setState(errUndefinedPartitionMethod(tbName)).build();
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return GetPartitionDescResponse.newBuilder().setState(returnError(t)).build();
      }

      if (metaDictionary.isSystemDatabase(dbName)) {
        return GetPartitionDescResponse.newBuilder().setState(errUndefinedPartitionMethod(tbName)).build();
      }

      rlock.lock();
      try {

        PartitionDescProto partitionDesc = store.getPartition(dbName, tbName, partitionName);
        if (partitionDesc != null) {
          return GetPartitionDescResponse.newBuilder()
              .setState(OK)
              .setPartition(partitionDesc)
              .build();
        } else {
          return GetPartitionDescResponse.newBuilder()
              .setState(errUndefinedPartition(partitionName))
              .build();
        }

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return GetPartitionDescResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public GetPartitionsResponse getPartitionsByTableName(RpcController controller, PartitionIdentifierProto request)
      throws ServiceException {
      String dbName = request.getDatabaseName();
      String tbName = request.getTableName();

      try {
        // linked meta data do not support partition.
        // So, the request that wants to get partitions in this db will be failed.
        if (linkedMetadataManager.existsDatabase(dbName)) {
          return GetPartitionsResponse.newBuilder().setState(errUndefinedPartitionMethod(tbName)).build();
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return GetPartitionsResponse.newBuilder().setState(returnError(t)).build();
      }

      if (metaDictionary.isSystemDatabase(dbName)) {
        return GetPartitionsResponse.newBuilder().setState(errUndefinedPartitionMethod(tbName)).build();
      }

      rlock.lock();
      try {

        List<PartitionDescProto> partitions = store.getPartitions(dbName, tbName);

        GetPartitionsResponse.Builder builder = GetPartitionsResponse.newBuilder();
        for (PartitionDescProto partition : partitions) {
          builder.addPartition(partition);
        }

        builder.setState(OK);
        return builder.build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return GetPartitionsResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public GetTablePartitionsResponse getAllPartitions(RpcController controller, NullProto request)
        throws ServiceException {

      rlock.lock();

      try {
        return GetTablePartitionsResponse.newBuilder()
            .setState(OK)
            .addAllPart(store.getAllPartitions())
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return GetTablePartitionsResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public ReturnState addPartitions(RpcController controller, AddPartitionsProto request) {

      TableIdentifierProto identifier = request.getTableIdentifier();
      String databaseName = identifier.getDatabaseName();
      String tableName = identifier.getTableName();

      rlock.lock();
      try {

        store.addPartitions(databaseName, tableName, request.getPartitionDescList(), request.getIfNotExists());
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public ReturnState createIndex(RpcController controller, IndexDescProto indexDesc) {
      String dbName = indexDesc.getTableIdentifier().getDatabaseName();

      try {
        // linked meta data do not support index. The request will be failed.
        if (linkedMetadataManager.existsDatabase(dbName)) {
          return errInsufficientPrivilege("to create index in database '" + dbName + "'");
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }

      rlock.lock();
      try {
        store.createIndex(indexDesc);
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public ReturnState existIndexByName(RpcController controller, IndexNameProto request) {

      String dbName = request.getDatabaseName();
      String indexName = request.getIndexName();

      try {
        // linked meta data do not support index. The request will be failed.
        if (linkedMetadataManager.existsDatabase(dbName)) {
          return errUndefinedIndexName(indexName);
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }

      rlock.lock();
      try {
        return store.existIndexByName(dbName, indexName) ? OK : errUndefinedIndexName(indexName);

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public ReturnState existIndexByColumnNames(RpcController controller, GetIndexByColumnNamesRequest request)
        throws ServiceException {

      TableIdentifierProto identifier = request.getTableIdentifier();
      String databaseName = identifier.getDatabaseName();
      String tableName = identifier.getTableName();
      List<String> columnNames = request.getColumnNamesList();

      try {
        // linked meta data do not support index. The request will be failed.
        if (linkedMetadataManager.existsDatabase(databaseName)) {
          return errUndefinedIndex(tableName);
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }

      rlock.lock();
      try {
        return store.existIndexByColumns(databaseName, tableName,
            columnNames.toArray(new String[columnNames.size()])) ? OK : errUndefinedIndex(tableName, columnNames);

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public ReturnState existIndexesByTable(RpcController controller, TableIdentifierProto request)
        throws ServiceException {
      String databaseName = request.getDatabaseName();
      String tableName = request.getTableName();

      try {
        // linked meta data do not support index. The request will be failed.
        if (linkedMetadataManager.existsDatabase(databaseName)) {
          return errUndefinedIndex(tableName);
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }

      rlock.lock();
      try {
        return store.existIndexesByTable(databaseName, tableName) ? OK : errUndefinedIndex(tableName);

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public IndexResponse getIndexByName(RpcController controller, IndexNameProto request) throws ServiceException {

      String databaseName = request.getDatabaseName();
      String indexName = request.getIndexName();

      rlock.lock();
      try {

        if (!store.existIndexByName(databaseName, indexName)) {
          return IndexResponse.newBuilder()
              .setState(errUndefinedIndexName(indexName))
              .build();
        }

        return IndexResponse.newBuilder()
            .setState(OK)
            .setIndexDesc(store.getIndexByName(databaseName, indexName))
            .build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return IndexResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public IndexResponse getIndexByColumnNames(RpcController controller, GetIndexByColumnNamesRequest request)
        throws ServiceException {

      TableIdentifierProto identifier = request.getTableIdentifier();
      String databaseName = identifier.getDatabaseName();
      String tableName = identifier.getTableName();
      List<String> columnNamesList = request.getColumnNamesList();
      String[] columnNames = new String[columnNamesList.size()];
      columnNames = columnNamesList.toArray(columnNames);

      rlock.lock();
      try {

        if (!store.existIndexByColumns(databaseName, tableName, columnNames)) {
          return IndexResponse.newBuilder()
              .setState(errUndefinedIndex(tableName, columnNamesList))
              .build();
        }
        return IndexResponse.newBuilder()
            .setState(OK)
            .setIndexDesc(store.getIndexByColumns(databaseName, tableName, columnNames))
            .build();
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return IndexResponse.newBuilder()
            .setState(returnError(t))
            .build();
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public IndexListResponse getAllIndexesByTable(RpcController controller, TableIdentifierProto request)
        throws ServiceException {
      final String databaseName = request.getDatabaseName();
      final String tableName = request.getTableName();

      try {
        // linked meta data do not support index.
        // So, the request that wants to check the index in this db will get empty list.
        if (linkedMetadataManager.existsDatabase(databaseName)) {
          return IndexListResponse.newBuilder().setState(OK).build();
        }
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return IndexListResponse.newBuilder().setState(returnError(t)).build();
      }

      rlock.lock();
      try {

        if (!store.existIndexesByTable(databaseName, tableName)) {
          return IndexListResponse.newBuilder()
              .setState(errUndefinedIndex(tableName))
              .build();
        }
        IndexListResponse.Builder builder = IndexListResponse.newBuilder().setState(OK);
        for (String eachIndexName : store.getAllIndexNamesByTable(databaseName, tableName)) {
          builder.addIndexDesc(store.getIndexByName(databaseName, eachIndexName));
        }
        return builder.build();
      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return IndexListResponse.newBuilder()
            .setState(returnError(t))
            .build();
      } finally {
        rlock.unlock();
      }
    }

    @Override
    public IndexListResponse getAllIndexes(RpcController controller, NullProto request) throws ServiceException {
      rlock.lock();
      try {
        return IndexListResponse.newBuilder().setState(OK).addAllIndexDesc(store.getAllIndexes()).build();

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return IndexListResponse.newBuilder()
            .setState(returnError(t))
            .build();

      } finally {
        rlock.unlock();
      }
    }

    @Override
    public ReturnState dropIndex(RpcController controller, IndexNameProto request) {

      String dbName = request.getDatabaseName();
      String indexName = request.getIndexName();

      wlock.lock();
      try {
        store.dropIndex(dbName, indexName);
        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);

      } finally {
        wlock.unlock();
      }
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
    public ReturnState createFunction(RpcController controller, FunctionDescProto funcDesc) {

      try {
        FunctionSignature signature = FunctionSignature.create(funcDesc);

        if (functions.containsKey(funcDesc.getSignature())) {
          FunctionDescProto found = findFunctionStrictType(funcDesc, true);
          if (found != null) {
            return errDuplicateFunction(signature.toString());
          }
        }

        TUtil.putToNestedList(functions, funcDesc.getSignature().getName(), funcDesc);

        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
    }

    @Override
    public ReturnState dropFunction(RpcController controller, UnregisterFunctionRequest request) {
      try {
        if (!containFunction(request.getSignature())) {
          return errUndefinedFunction(request.toString());
        }

        functions.remove(request.getSignature());

        return OK;

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
    }

    @Override
    public FunctionResponse getFunctionMeta(RpcController controller, GetFunctionMetaRequest request) {

      FunctionDescProto function = null;

      try {
        if (request.hasFunctionType()) {
          if (containFunction(request.getSignature(), request.getFunctionType(), request.getParameterTypesList())) {
            function = findFunction(request.getSignature(), request.getFunctionType(),
                request.getParameterTypesList(), true);
          }
        } else {
          function = findFunction(request.getSignature(), request.getParameterTypesList());
        }

        if (function != null) {
          return FunctionResponse.newBuilder()
              .setState(OK)
              .setFunction(function)
              .build();
        } else {

          return FunctionResponse.newBuilder()
              .setState(errUndefinedFunction(
                  buildSimpleFunctionSignature(request.getSignature(), request.getParameterTypesList())))
              .build();
        }

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);

        return FunctionResponse.newBuilder()
            .setState(returnError(t))
            .build();
      }
    }

    @Override
    public ReturnState containFunction(RpcController controller, ContainFunctionRequest request) {

      try {
        boolean returnValue;
        if (request.hasFunctionType()) {
          returnValue = containFunction(request.getSignature(), request.getFunctionType(),
              request.getParameterTypesList());
        } else {
          returnValue = containFunction(request.getSignature(), request.getParameterTypesList());
        }

        return returnValue ?
            OK :
            errUndefinedFunction(buildSimpleFunctionSignature(request.getSignature(), request.getParameterTypesList()));

      } catch (Throwable t) {
        printStackTraceIfError(LOG, t);
        return returnError(t);
      }
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
}
