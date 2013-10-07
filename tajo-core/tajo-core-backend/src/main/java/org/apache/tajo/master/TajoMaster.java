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

package org.apache.tajo.master;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.function.Country;
import org.apache.tajo.engine.function.InCountry;
import org.apache.tajo.engine.function.builtin.*;
import org.apache.tajo.engine.function.string.*;
import org.apache.tajo.master.querymaster.QueryJobManager;
import org.apache.tajo.master.rm.WorkerResourceManager;
import org.apache.tajo.master.rm.YarnTajoResourceManager;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.webapp.StaticHttpServer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

public class TajoMaster extends CompositeService {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(TajoMaster.class);

  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  /** rw-r--r-- */
  @SuppressWarnings("OctalInteger")
  final public static FsPermission TAJO_ROOT_DIR_PERMISSION = FsPermission.createImmutable((short) 0644);
  /** rw-r--r-- */
  @SuppressWarnings("OctalInteger")
  final public static FsPermission SYSTEM_DIR_PERMISSION = FsPermission.createImmutable((short) 0644);
  /** rw-r--r-- */
  final public static FsPermission SYSTEM_RESOURCE_DIR_PERMISSION = FsPermission.createImmutable((short) 0644);
  /** rw-r--r-- */
  @SuppressWarnings("OctalInteger")
  final public static FsPermission WAREHOUSE_DIR_PERMISSION = FsPermission.createImmutable((short) 0644);
  /** rw-r--r-- */
  @SuppressWarnings("OctalInteger")
  final public static FsPermission STAGING_ROOTDIR_PERMISSION = FsPermission.createImmutable((short) 0644);
  /** rw-r--r-- */
  @SuppressWarnings("OctalInteger")
  final public static FsPermission SYSTEM_CONF_FILE_PERMISSION = FsPermission.createImmutable((short) 0644);


  private MasterContext context;
  private TajoConf systemConf;
  private FileSystem defaultFS;
  private Clock clock;

  private Path tajoRootPath;
  private Path wareHousePath;

  private CatalogServer catalogServer;
  private CatalogService catalog;
  private AbstractStorageManager storeManager;
  private GlobalEngine globalEngine;
  private AsyncDispatcher dispatcher;
  private TajoMasterClientService tajoMasterClientService;
  private TajoMasterService tajoMasterService;

  private WorkerResourceManager resourceManager;
  //Web Server
  private StaticHttpServer webServer;

  private QueryJobManager queryJobManager;

  public TajoMaster() throws Exception {
    super(TajoMaster.class.getName());
  }

  @Override
  public void init(Configuration _conf) {
    this.systemConf = (TajoConf) _conf;

    context = new MasterContext(systemConf);
    clock = new SystemClock();

    try {
      RackResolver.init(systemConf);

      initResourceManager();
      initWebServer();

      this.dispatcher = new AsyncDispatcher();
      addIfService(dispatcher);

      // check the system directory and create if they are not created.
      checkAndInitializeSystemDirectories();
      this.storeManager = StorageManagerFactory.getStorageManager(systemConf);

      catalogServer = new CatalogServer(initBuiltinFunctions());
      addIfService(catalogServer);
      catalog = new LocalCatalogWrapper(catalogServer);

      globalEngine = new GlobalEngine(context);
      addIfService(globalEngine);

      queryJobManager = new QueryJobManager(context);
      addIfService(queryJobManager);

      tajoMasterClientService = new TajoMasterClientService(context);
      addIfService(tajoMasterClientService);

      tajoMasterService = new TajoMasterService(context);
      addIfService(tajoMasterService);

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    super.init(systemConf);

    LOG.info("Tajo Master is initialized.");
  }

  private void initResourceManager() throws Exception {
    Class<WorkerResourceManager>  resourceManagerClass = (Class<WorkerResourceManager>)
        systemConf.getClass(ConfVars.RESOURCE_MANAGER_CLASS.varname, YarnTajoResourceManager.class);
    Constructor<WorkerResourceManager> constructor = resourceManagerClass.getConstructor(MasterContext.class);
    resourceManager = constructor.newInstance(context);
    resourceManager.init(context.getConf());
  }

  private void initWebServer() throws Exception {
    int httpPort = systemConf.getInt("tajo.master.http.port", 8080);
    webServer = StaticHttpServer.getInstance(this ,"admin", null, httpPort ,
        true, null, context.getConf(), null);
    webServer.start();
  }

  private void checkAndInitializeSystemDirectories() throws IOException {
    // Get Tajo root dir
    this.tajoRootPath = TajoConf.getTajoRootPath(systemConf);
    LOG.info("Tajo Root Directory: " + tajoRootPath);

    // Check and Create Tajo root dir
    this.defaultFS = tajoRootPath.getFileSystem(systemConf);
    systemConf.set("fs.defaultFS", defaultFS.getUri().toString());
    LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");
    if (!defaultFS.exists(tajoRootPath)) {
      defaultFS.mkdirs(tajoRootPath, new FsPermission(TAJO_ROOT_DIR_PERMISSION));
      LOG.info("Tajo Root Directory '" + tajoRootPath + "' is created.");
    }

    // Check and Create system and system resource dir
    Path systemPath = TajoConf.getSystemPath(systemConf);
    if (!defaultFS.exists(systemPath)) {
      defaultFS.mkdirs(systemPath, new FsPermission(SYSTEM_DIR_PERMISSION));
      LOG.info("System dir '" + systemPath + "' is created");
    }
    Path systemResourcePath = TajoConf.getSystemResourcePath(systemConf);
    if (!defaultFS.exists(systemResourcePath)) {
      defaultFS.mkdirs(systemResourcePath, new FsPermission(SYSTEM_RESOURCE_DIR_PERMISSION));
      LOG.info("System resource dir '" + systemResourcePath + "' is created");
    }

    // Get Warehouse dir
    this.wareHousePath = TajoConf.getWarehousePath(systemConf);
    LOG.info("Tajo Warehouse Dir: " + wareHousePath);

    // Check and Create Warehouse dir
    if (!defaultFS.exists(wareHousePath)) {
      defaultFS.mkdirs(wareHousePath, new FsPermission(WAREHOUSE_DIR_PERMISSION));
      LOG.info("Warehouse dir '" + wareHousePath + "' is created");
    }

    Path stagingPath = TajoConf.getStagingRoot(systemConf);
    if (!defaultFS.exists(stagingPath)) {
      defaultFS.mkdirs(stagingPath, new FsPermission(STAGING_ROOTDIR_PERMISSION));
    }
  }

  @SuppressWarnings("unchecked")
  public static List<FunctionDesc> initBuiltinFunctions() throws ServiceException {
    List<FunctionDesc> sqlFuncs = new ArrayList<FunctionDesc>();

    // Sum
    sqlFuncs.add(new FunctionDesc("sum", SumInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4)));
    sqlFuncs.add(new FunctionDesc("sum", SumLong.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT8),
        CatalogUtil.newSimpleDataTypeArray(Type.INT8)));
    sqlFuncs.add(new FunctionDesc("sum", SumFloat.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.FLOAT4),
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT4)));
    sqlFuncs.add(new FunctionDesc("sum", SumDouble.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.FLOAT8),
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT8)));

    // Max
    sqlFuncs.add(new FunctionDesc("max", MaxInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4)));
    sqlFuncs.add(new FunctionDesc("max", MaxLong.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT8),
        CatalogUtil.newSimpleDataTypeArray(Type.INT8)));
    sqlFuncs.add(new FunctionDesc("max", MaxFloat.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.FLOAT4),
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT4)));
    sqlFuncs.add(new FunctionDesc("max", MaxDouble.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.FLOAT8),
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT8)));

    // Min
    sqlFuncs.add(new FunctionDesc("min", MinInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4)));
    sqlFuncs.add(new FunctionDesc("min", MinLong.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT8),
        CatalogUtil.newSimpleDataTypeArray(Type.INT8)));
    sqlFuncs.add(new FunctionDesc("min", MinFloat.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.FLOAT4),
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT4)));
    sqlFuncs.add(new FunctionDesc("min", MinDouble.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.FLOAT8),
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT8)));
    sqlFuncs.add(new FunctionDesc("min", MinString.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.TEXT),
        CatalogUtil.newSimpleDataTypeArray(Type.TEXT)));

    // AVG
    sqlFuncs.add(new FunctionDesc("avg", AvgInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.FLOAT8),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4)));
    sqlFuncs.add(new FunctionDesc("avg", AvgLong.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.FLOAT8),
        CatalogUtil.newSimpleDataTypeArray(Type.INT8)));
    sqlFuncs.add(new FunctionDesc("avg", AvgFloat.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.FLOAT8),
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT4)));
    sqlFuncs.add(new FunctionDesc("avg", AvgDouble.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.FLOAT8),
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT8)));

    // Count
    sqlFuncs.add(new FunctionDesc("count", CountValue.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT8),
        CatalogUtil.newSimpleDataTypeArray(Type.ANY)));
    sqlFuncs.add(new FunctionDesc("count", CountRows.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT8),
        CatalogUtil.newSimpleDataTypeArray()));
    sqlFuncs.add(new FunctionDesc("count", CountValueDistinct.class, FunctionType.DISTINCT_AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT8),
        CatalogUtil.newSimpleDataTypeArray(Type.ANY)));

    // GeoIP
    sqlFuncs.add(new FunctionDesc("in_country", InCountry.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.BOOLEAN),
        CatalogUtil.newSimpleDataTypeArray(Type.TEXT, Type.TEXT)));
    sqlFuncs.add(new FunctionDesc("country", Country.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.TEXT),
        CatalogUtil.newSimpleDataTypeArray(Type.TEXT)));

    // Date
    sqlFuncs.add(new FunctionDesc("date", Date.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT8),
        CatalogUtil.newSimpleDataTypeArray(Type.TEXT)));

    // Today
    sqlFuncs.add(new FunctionDesc("today", Date.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT8),
        CatalogUtil.newSimpleDataTypeArray()));

    sqlFuncs.add(
        new FunctionDesc("random", RandomInt.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.INT4),
            CatalogUtil.newSimpleDataTypeArray(Type.INT4)));

    sqlFuncs.add(
        new FunctionDesc("upper", Upper.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT)));
    sqlFuncs.add(
        new FunctionDesc("lower", Lower.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT)));

    sqlFuncs.add(
        new FunctionDesc("split_part", SplitPart.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT, Type.TEXT, Type.INT4)));
    sqlFuncs.add(
        new FunctionDesc("trim", BTrim.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT)));
    sqlFuncs.add(
        new FunctionDesc("trim", BTrim.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT, Type.TEXT)));
    sqlFuncs.add(
        new FunctionDesc("btrim", BTrim.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT)));
    sqlFuncs.add(
        new FunctionDesc("btrim", BTrim.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT, Type.TEXT)));
    sqlFuncs.add(
        new FunctionDesc("ltrim", LTrim.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT)));
    sqlFuncs.add(
        new FunctionDesc("ltrim", LTrim.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT, Type.TEXT)));
    sqlFuncs.add(
        new FunctionDesc("rtrim", RTrim.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT)));
    sqlFuncs.add(
        new FunctionDesc("rtrim", RTrim.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT, Type.TEXT)));
    sqlFuncs.add(
        new FunctionDesc("regexp_replace", RegexpReplace.class, FunctionType.GENERAL,
            CatalogUtil.newSimpleDataType(Type.TEXT),
            CatalogUtil.newSimpleDataTypeArray(Type.TEXT, Type.TEXT, Type.TEXT)));

    return sqlFuncs;
  }

  public MasterContext getContext() {
    return this.context;
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  @Override
  public void start() {
    LOG.info("TajoMaster startup");
    super.start();

    // Setting the system global configs
    systemConf.setSocketAddr(ConfVars.CATALOG_ADDRESS.varname,
        NetUtils.getConnectAddress(catalogServer.getBindAddress()));

    try {
      writeSystemConf();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void writeSystemConf() throws IOException {
    // Storing the system configs
    Path systemResourcePath = TajoConf.getSystemResourcePath(systemConf);
    Path systemConfPath = new Path(systemResourcePath, "system_conf.xml");
    systemConf.setVar(ConfVars.SYSTEM_CONF_PATH, systemConfPath.toUri().toString());

    defaultFS.delete(systemConfPath, true);
    FSDataOutputStream out = FileSystem.create(defaultFS, systemConfPath,
        new FsPermission(SYSTEM_CONF_FILE_PERMISSION));
    try {
      systemConf.writeXml(out);
    } finally {
      out.close();
    }
    defaultFS.setReplication(systemConfPath, (short) systemConf.getIntVar(ConfVars.SYSTEM_CONF_REPLICA_COUNT));
  }

  @Override
  public void stop() {
    try {
      webServer.stop();
    } catch (Exception e) {
      LOG.error(e);
    }

    super.stop();
    LOG.info("Tajo Master main thread exiting");
  }

  public EventHandler getEventHandler() {
    return dispatcher.getEventHandler();
  }

  public boolean isMasterRunning() {
    return getServiceState() == STATE.STARTED;
  }

  public CatalogService getCatalog() {
    return this.catalog;
  }

  public AbstractStorageManager getStorageManager() {
    return this.storeManager;
  }

  public class MasterContext {
    private final TajoConf conf;

    public MasterContext(TajoConf conf) {
      this.conf = conf;
    }

    public TajoConf getConf() {
      return conf;
    }

    public Clock getClock() {
      return clock;
    }

    public QueryJobManager getQueryJobManager() {
      return queryJobManager;
    }

    public WorkerResourceManager getResourceManager() {
      return resourceManager;
    }

    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    public CatalogService getCatalog() {
      return catalog;
    }

    public GlobalEngine getGlobalEngine() {
      return globalEngine;
    }

    public AbstractStorageManager getStorageManager() {
      return storeManager;
    }

    public TajoMasterService getTajoMasterService() {
      return tajoMasterService;
    }
  }

  public static void main(String[] args) throws Exception {
    StringUtils.startupShutdownMessage(TajoMaster.class, args, LOG);

    try {
      TajoMaster master = new TajoMaster();
      ShutdownHookManager.get().addShutdownHook(new CompositeServiceShutdownHook(master), SHUTDOWN_HOOK_PRIORITY);
      TajoConf conf = new TajoConf(new YarnConfiguration());
      master.init(conf);
      master.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting TajoMaster", t);
      System.exit(-1);
    }
  }
}