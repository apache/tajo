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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tajo.catalog.CatalogServer;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.LocalCatalogWrapper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceCommand;
import org.apache.tajo.catalog.store.AbstractDBStore;
import org.apache.tajo.catalog.store.DerbyStore;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.exception.*;
import org.apache.tajo.io.AsyncTaskService;
import org.apache.tajo.master.rm.TajoResourceManager;
import org.apache.tajo.metrics.ClusterResourceMetricSet;
import org.apache.tajo.metrics.Master;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.rule.EvaluationContext;
import org.apache.tajo.rule.EvaluationFailedException;
import org.apache.tajo.rule.SelfDiagnosisRuleEngine;
import org.apache.tajo.rule.SelfDiagnosisRuleSession;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.session.SessionManager;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.*;
import org.apache.tajo.util.history.HistoryReader;
import org.apache.tajo.util.history.HistoryWriter;
import org.apache.tajo.util.metrics.TajoSystemMetrics;
import org.apache.tajo.webapp.QueryExecutorServlet;
import org.apache.tajo.webapp.StaticHttpServer;
import org.apache.tajo.ws.rs.TajoRestService;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;

public class TajoMaster extends CompositeService {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(TajoMaster.class);

  public static final int SHUTDOWN_HOOK_PRIORITY = 10;

  /** rw-r--r-- */
  @SuppressWarnings("OctalInteger")
  final public static FsPermission TAJO_ROOT_DIR_PERMISSION = FsPermission.createImmutable((short) 0755);
  /** rw-r--r-- */
  @SuppressWarnings("OctalInteger")
  final public static FsPermission SYSTEM_DIR_PERMISSION = FsPermission.createImmutable((short) 0755);
  /** rw-r--r-- */
  final public static FsPermission SYSTEM_RESOURCE_DIR_PERMISSION = FsPermission.createImmutable((short) 0755);
  /** rw-r--r-- */
  @SuppressWarnings("OctalInteger")
  final public static FsPermission WAREHOUSE_DIR_PERMISSION = FsPermission.createImmutable((short) 0755);
  /** rw-r--r-- */
  @SuppressWarnings("OctalInteger")
  final public static FsPermission STAGING_ROOTDIR_PERMISSION = FsPermission.createImmutable((short) 0755);
  /** rw-r--r-- */
  @SuppressWarnings("OctalInteger")
  final public static FsPermission SYSTEM_CONF_FILE_PERMISSION = FsPermission.createImmutable((short) 0755);


  private MasterContext context;
  private TajoConf systemConf;
  private FileSystem defaultFS;
  private Clock clock;

  private Path tajoRootPath;
  private Path wareHousePath;

  private CatalogServer catalogServer;
  private CatalogService catalog;
  private GlobalEngine globalEngine;
  private AsyncTaskService asyncTaskService;
  private AsyncDispatcher dispatcher;
  private TajoMasterClientService tajoMasterClientService;
  private QueryCoordinatorService tajoMasterService;
  private SessionManager sessionManager;

  private TajoResourceManager resourceManager;
  //Web Server
  private StaticHttpServer webServer;
  private TajoRestService restServer;

  private QueryManager queryManager;

  private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

  private TajoSystemMetrics systemMetrics;

  private ServiceTracker haService;

  private JvmPauseMonitor pauseMonitor;

  private HistoryWriter historyWriter;

  private HistoryReader historyReader;

  private static final long CLUSTER_STARTUP_TIME = System.currentTimeMillis();

  public TajoMaster() throws Exception {
    super(TajoMaster.class.getName());
  }

  public String getMasterName() {
    return NetUtils.normalizeInetSocketAddress(tajoMasterService.getBindAddress());
  }

  public String getVersion() {
    return VersionInfo.getDisplayVersion();
  }

  public TajoMasterClientService getTajoMasterClientService() {
    return  tajoMasterClientService;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    this.systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    ShutdownHookManager.get().addShutdownHook(new ShutdownHook(), SHUTDOWN_HOOK_PRIORITY);

    context = new MasterContext(systemConf);
    clock = new SystemClock();
    RackResolver.init(systemConf);

    initResourceManager();

    this.dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    // check the system directory and create if they are not created.
    checkAndInitializeSystemDirectories();
    diagnoseTajoMaster();

    catalogServer = new CatalogServer(TablespaceManager.getMetadataProviders(), FunctionLoader.loadFunctions(systemConf));
    addIfService(catalogServer);
    catalog = new LocalCatalogWrapper(catalogServer, systemConf);

    sessionManager = new SessionManager(dispatcher);
    addIfService(sessionManager);

    globalEngine = new GlobalEngine(context);
    addIfService(globalEngine);

    queryManager = new QueryManager(context);
    addIfService(queryManager);

    tajoMasterClientService = new TajoMasterClientService(context);
    addIfService(tajoMasterClientService);

    asyncTaskService = new AsyncTaskService(context);
    addIfService(asyncTaskService);

    tajoMasterService = new QueryCoordinatorService(context);
    addIfService(tajoMasterService);

    restServer = new TajoRestService(context);
    addIfService(restServer);

    PythonScriptEngine.initPythonScriptEngineFiles();

    // Try to start up all services in TajoMaster.
    // If anyone is failed, the master prints out the errors and immediately should shutdowns
    try {
      super.serviceInit(systemConf);
    } catch (Throwable t) {
      t.printStackTrace();
      Runtime.getRuntime().halt(-1);
    }
    LOG.info("Tajo Master is initialized.");
  }

  private void initSystemMetrics() {
    systemMetrics = new TajoSystemMetrics(systemConf, Master.class, getMasterName());
    systemMetrics.start();

    systemMetrics.register(Master.Cluster.UPTIME, () -> context.getClusterUptime());

    systemMetrics.register(Master.Cluster.class, new ClusterResourceMetricSet(context));
  }

  private void initResourceManager() throws Exception {
    resourceManager = new TajoResourceManager(context);
    addIfService(resourceManager);
  }

  private void initWebServer() throws Exception {
    if (!systemConf.getBoolVar(ConfVars.$TEST_MODE)) {
      InetSocketAddress address = systemConf.getSocketAddrVar(ConfVars.TAJO_MASTER_INFO_ADDRESS);
      webServer = StaticHttpServer.getInstance(this ,"admin", address.getHostName(), address.getPort(),
          true, null, context.getConf(), null);
      webServer.addServlet("queryServlet", "/query_exec", QueryExecutorServlet.class);
      webServer.start();
    }
  }

  private void checkAndInitializeSystemDirectories() throws IOException {
    // Get Tajo root dir
    this.tajoRootPath = TajoConf.getTajoRootDir(systemConf);
    LOG.info("Tajo Root Directory: " + tajoRootPath);

    // Check and Create Tajo root dir
    this.defaultFS = tajoRootPath.getFileSystem(systemConf);
    systemConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultFS.getUri().toString());
    LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");
    if (!defaultFS.exists(tajoRootPath)) {
      defaultFS.mkdirs(tajoRootPath, new FsPermission(TAJO_ROOT_DIR_PERMISSION));
      LOG.info("Tajo Root Directory '" + tajoRootPath + "' is created.");
    }

    // Check and Create system and system resource dir
    Path systemPath = TajoConf.getSystemDir(systemConf);
    if (!defaultFS.exists(systemPath)) {
      defaultFS.mkdirs(systemPath, new FsPermission(SYSTEM_DIR_PERMISSION));
      LOG.info("System dir '" + systemPath + "' is created");
    }
    Path systemResourcePath = TajoConf.getSystemResourceDir(systemConf);
    if (!defaultFS.exists(systemResourcePath)) {
      defaultFS.mkdirs(systemResourcePath, new FsPermission(SYSTEM_RESOURCE_DIR_PERMISSION));
      LOG.info("System resource dir '" + systemResourcePath + "' is created");
    }

    // Get Warehouse dir
    this.wareHousePath = TajoConf.getWarehouseDir(systemConf);
    LOG.info("Tajo Warehouse dir: " + wareHousePath);

    // Check and Create Warehouse dir
    if (!defaultFS.exists(wareHousePath)) {
      defaultFS.mkdirs(wareHousePath, new FsPermission(WAREHOUSE_DIR_PERMISSION));
      LOG.info("Warehouse dir '" + wareHousePath + "' is created");
    }

    Path stagingPath = TajoConf.getDefaultRootStagingDir(systemConf);
    LOG.info("Staging dir: " + wareHousePath);
    if (!defaultFS.exists(stagingPath)) {
      defaultFS.mkdirs(stagingPath, new FsPermission(STAGING_ROOTDIR_PERMISSION));
      LOG.info("Staging dir '" + stagingPath + "' is created");
    }
  }
  
  private void diagnoseTajoMaster() throws EvaluationFailedException {
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    SelfDiagnosisRuleSession ruleSession = ruleEngine.newRuleSession();
    EvaluationContext context = new EvaluationContext();
    
    context.addParameter(TajoConf.class.getName(), systemConf);
    
    ruleSession.withCategoryNames("base", "master").fireRules(context);
  }

  private void startJvmPauseMonitor(){
    pauseMonitor = new JvmPauseMonitor(systemConf);
    pauseMonitor.start();
  }

  public MasterContext getContext() {
    return this.context;
  }

  @Override
  public void serviceStart() throws Exception {
    LOG.info("TajoMaster is starting up");

    startJvmPauseMonitor();

    // check base tablespace and databases
    checkBaseTBSpaceAndDatabase();

    super.serviceStart();
    initSystemMetrics();

    // Setting the system global configs
    systemConf.setSocketAddr(ConfVars.CATALOG_ADDRESS.varname,
        NetUtils.getConnectAddress(catalogServer.getBindAddress()));

    try {
      writeSystemConf();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }

    initWebServer();

    haService = ServiceTrackerFactory.get(systemConf);
    haService.register();

    historyWriter = new HistoryWriter(getMasterName(), true);
    historyWriter.init(getConfig());
    addIfService(historyWriter);
    historyWriter.start();

    historyReader = new HistoryReader(getMasterName(), context.getConf());
  }

  private void writeSystemConf() throws IOException {
    // Storing the system configs
    Path systemConfPath = TajoConf.getSystemConfPath(systemConf);

    if (!defaultFS.exists(systemConfPath.getParent())) {
      defaultFS.mkdirs(systemConfPath.getParent());
    }

    if (defaultFS.exists(systemConfPath)) {
      defaultFS.delete(systemConfPath, false);
    }

    // In TajoMaster HA, some master might see LeaseExpiredException because of lease mismatch. Thus,
    // we need to create below xml file at HdfsServiceTracker::writeSystemConf.
    if (!systemConf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
      try (FSDataOutputStream out = FileSystem.create(defaultFS, systemConfPath,
              new FsPermission(SYSTEM_CONF_FILE_PERMISSION))) {
        systemConf.writeXml(out);
      }
      defaultFS.setReplication(systemConfPath, (short) systemConf.getIntVar(ConfVars.SYSTEM_CONF_REPLICA_COUNT));
    }
  }

  private void checkBaseTBSpaceAndDatabase()
      throws IOException, DuplicateDatabaseException, DuplicateTablespaceException {

    if (catalog.existTablespace(DEFAULT_TABLESPACE_NAME)) { // if default tablespace already exists

      CatalogProtos.TablespaceProto tablespace = null;
      try {
        tablespace = catalog.getTablespace(DEFAULT_TABLESPACE_NAME);
      } catch (UndefinedTablespaceException e) {
        throw new TajoInternalError(e);
      }

      // if warehouse directory and the location of default tablespace are different from each other
      if (!tablespace.getUri().equals(context.getConf().getVar(ConfVars.WAREHOUSE_DIR))) {
        AlterTablespaceCommand.Builder alterCommand =
            AlterTablespaceCommand.newBuilder()
                .setType(AlterTablespaceProto.AlterTablespaceType.LOCATION)
                .setLocation(context.getConf().getVar(ConfVars.WAREHOUSE_DIR));

        AlterTablespaceProto alterTablespace = AlterTablespaceProto.newBuilder()
            .setSpaceName(DEFAULT_TABLESPACE_NAME)
            .addCommand(alterCommand).build();

        // update the location of default tablespace
        try {
          catalog.alterTablespace(alterTablespace);
        } catch (TajoException e) {
          throw new TajoInternalError(e);
        }

        LOG.warn(
            "The location of default tablespace has been changed. " +
            "You may not accept existing managed tables stored in the previous default tablespace");
      }

    } else if (!catalog.existTablespace(DEFAULT_TABLESPACE_NAME)) { // if the default tablespace does not exists
      catalog.createTablespace(DEFAULT_TABLESPACE_NAME, context.getConf().getVar(ConfVars.WAREHOUSE_DIR));
    } else {
      LOG.info(String.format("Default tablespace (%s) is already prepared.", DEFAULT_TABLESPACE_NAME));
    }

    if (!catalog.existDatabase(DEFAULT_DATABASE_NAME)) {
      globalEngine.getDDLExecutor().createDatabase(null, DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME, false);
    } else {
      LOG.info(String.format("Default database (%s) is already prepared.", DEFAULT_DATABASE_NAME));
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (haService != null) haService.delete();

    if (restServer != null) restServer.stop();

    if (webServer != null) webServer.stop();

    if (systemMetrics != null) systemMetrics.stop();

    if (pauseMonitor != null) pauseMonitor.stop();
    super.serviceStop();

    LOG.info("Tajo Master main thread exiting");
  }

  /**
   * This is only for unit tests.
   */
  @VisibleForTesting
  public void refresh() {
    catalogServer.refresh(TablespaceManager.getMetadataProviders());
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

  public CatalogServer getCatalogServer() {
    return this.catalogServer;
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

    public long getClusterUptime() {
      return getClock().getTime() - CLUSTER_STARTUP_TIME;
    }

    public QueryManager getQueryJobManager() {
      return queryManager;
    }

    public TajoResourceManager getResourceManager() {
      return resourceManager;
    }

    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    public CatalogService getCatalog() {
      return catalog;
    }

    public SessionManager getSessionManager() {
      return sessionManager;
    }

    public GlobalEngine getGlobalEngine() {
      return globalEngine;
    }

    public AsyncTaskService asyncTaskExecutor() {
      return asyncTaskService;
    }

    public QueryCoordinatorService getTajoMasterService() {
      return tajoMasterService;
    }

    public TajoSystemMetrics getMetrics() {
      return systemMetrics;
    }

    public ServiceTracker getHAService() {
      return haService;
    }

    public HistoryWriter getHistoryWriter() {
      return historyWriter;
    }

    public HistoryReader getHistoryReader() {
      return historyReader;
    }
    
    public TajoRestService getRestServer() {
      return restServer;
    }
  }

  String getThreadTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }

  public void dumpThread(Writer writer) {
    PrintWriter stream = new PrintWriter(writer);
    int STACK_DEPTH = 20;
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: Tajo Worker");
    stream.println(threadIds.length + " active threads");
    for (long tid : threadIds) {
      ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      stream.println("Thread " + getThreadTaskName(info.getThreadId(), info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      stream.println("  State: " + state + ", Blocked count: " + info.getBlockedCount() +
          ", Waited count: " + info.getWaitedCount());
      if (contention) {
        stream.println("  Blocked time: " + info.getBlockedTime() + ", Waited time: " + info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        stream.println("  Waiting on " + info.getLockName());
      } else if (state == Thread.State.BLOCKED) {
        stream.println("  Blocked on " + info.getLockName() +
            ", Blocked by " + getThreadTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
      }
      stream.println("  Stack:");
      for (StackTraceElement frame : info.getStackTrace()) {
        stream.println("    " + frame.toString());
      }
      stream.println("");
    }
  }

  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      if(!isInState(STATE.STOPPED)) {
        LOG.info("============================================");
        LOG.info("TajoMaster received SIGINT Signal");
        LOG.info("============================================");
        stop();

        // If embedded derby is used as catalog, shutdown it.
        if (catalogServer != null &&
            catalogServer.getStoreClassName().equals("org.apache.tajo.catalog.store.DerbyStore")
            && AbstractDBStore.needShutdown(catalogServer.getStoreUri())) {
          DerbyStore.shutdown();
        }
        RpcClientManager.shutdown();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new TajoUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(TajoMaster.class, args, LOG);

    try {
      TajoMaster master = new TajoMaster();
      TajoConf conf = new TajoConf();
      master.init(conf);
      master.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting TajoMaster", t);
      System.exit(-1);
    }
  }
}
