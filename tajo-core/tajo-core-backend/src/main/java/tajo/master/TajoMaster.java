/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.master;

import com.google.common.collect.Maps;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.RackResolver;
import tajo.QueryId;
import tajo.QueryIdFactory;
import tajo.catalog.CatalogServer;
import tajo.catalog.CatalogService;
import tajo.catalog.FunctionDesc;
import tajo.catalog.LocalCatalog;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.engine.function.Country;
import tajo.engine.function.InCountry;
import tajo.engine.function.builtin.*;
import tajo.master.cluster.event.WorkerEvent;
import tajo.master.event.QueryEvent;
import tajo.master.event.QueryEventType;
import tajo.storage.StorageManager;
import tajo.webapp.StaticHttpServer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TajoMaster extends CompositeService {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(TajoMaster.class);

  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private MasterContext context;
  private TajoConf conf;
  private FileSystem defaultFS;
  private Clock clock;

  private Path basePath;
  private Path dataPath;
  private Path queryStagingPath;
  private Path wareHousePath;


  private CatalogServer catalogServer;
  private CatalogService catalog;
  private StorageManager storeManager;
  private GlobalEngine globalEngine;
  private AsyncDispatcher dispatcher;
  private ClientService clientService;
  private YarnRPC yarnRPC;

  //Web Server
  private StaticHttpServer webServer;

  public TajoMaster() throws Exception {
    super(TajoMaster.class.getName());
  }

  @Override
  public void init(Configuration _conf) {
    this.conf = (TajoConf) _conf;

    context = new MasterContext(conf);
    clock = new SystemClock();


    try {
      webServer = StaticHttpServer.getInstance(this ,"admin", null, 8080 ,
          true, null, context.getConf(), null);
      webServer.start();

      QueryIdFactory.reset();

      // Get the tajo base dir
      this.basePath = new Path(conf.getVar(ConfVars.ENGINE_BASE_DIR));
      LOG.info("Base dir is set " + basePath);
      // Get default DFS uri from the base dir
      this.defaultFS = basePath.getFileSystem(conf);
      LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");

      if (!defaultFS.exists(basePath)) {
        defaultFS.mkdirs(basePath);
        LOG.info("Tajo Base dir (" + basePath + ") is created.");
      }

      // Get the tajo data warehouse dir
      this.wareHousePath = new Path(conf.getVar(ConfVars.WAREHOUSE_PATH));
      LOG.info("Tajo warehouse dir is set to " + wareHousePath);
      if (!defaultFS.exists(wareHousePath)) {
        defaultFS.mkdirs(wareHousePath);
        LOG.info("Warehouse dir (" + wareHousePath + ") is created");
      }

      // Get the tajo query tmp dir
      this.queryStagingPath = new Path(conf.getVar(ConfVars.QUERY_TMP_DIR));
      LOG.info("Tajo query staging dir is set to " + queryStagingPath);
      if (!defaultFS.exists(queryStagingPath)) {
        defaultFS.mkdirs(queryStagingPath);
        LOG.info("Query Staging dir (" + queryStagingPath + ") is created");
      }

      this.dataPath = new Path(conf.getVar(ConfVars.ENGINE_DATA_DIR));
      LOG.info("Tajo data dir is set " + dataPath);
      if (!defaultFS.exists(dataPath)) {
        defaultFS.mkdirs(dataPath);
        LOG.info("Data dir (" + dataPath + ") is created");
      }

      yarnRPC = YarnRPC.create(conf);

      this.dispatcher = new AsyncDispatcher();
      addIfService(dispatcher);

      this.storeManager = new StorageManager(conf);

      // The below is some mode-dependent codes
      // If tajo is local mode
      final boolean mode = conf.getBoolVar(ConfVars.CLUSTER_DISTRIBUTED);
      if (!mode) {
        LOG.info("Enabled Pseudo Distributed Mode");
      } else { // if tajo is distributed mode
        LOG.info("Enabled Distributed Mode");
      }
      // This is temporal solution of the above problem.
      catalogServer = new CatalogServer(initBuiltinFunctions());
      addIfService(catalogServer);
      catalog = new LocalCatalog(catalogServer);

      globalEngine = new GlobalEngine(context, storeManager);
      addIfService(globalEngine);

      dispatcher.register(QueryEventType.class, new QueryEventDispatcher());

      clientService = new ClientService(context);
      addIfService(clientService);

      RackResolver.init(conf);
    } catch (Exception e) {
       e.printStackTrace();
    }

    super.init(conf);
  }

  public static List<FunctionDesc> initBuiltinFunctions() throws ServiceException {
    List<FunctionDesc> sqlFuncs = new ArrayList<FunctionDesc>();

    // Sum
    sqlFuncs.add(new FunctionDesc("sum", NewSumInt.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.INT},
        new DataType[] {DataType.INT}));
    sqlFuncs.add(new FunctionDesc("sum", SumLong.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.LONG},
        new DataType[] {DataType.LONG}));
    sqlFuncs.add(new FunctionDesc("sum", SumFloat.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.FLOAT},
        new DataType[] {DataType.FLOAT}));
    sqlFuncs.add(new FunctionDesc("sum", SumDouble.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.DOUBLE},
        new DataType[] {DataType.DOUBLE}));

    // Max
    sqlFuncs.add(new FunctionDesc("max", MaxInt.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.INT},
        new DataType[] {DataType.INT}));
    sqlFuncs.add(new FunctionDesc("max", MaxLong.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.LONG},
        new DataType[] {DataType.LONG}));
    sqlFuncs.add(new FunctionDesc("max", MaxFloat.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.FLOAT},
        new DataType[] {DataType.FLOAT}));
    sqlFuncs.add(new FunctionDesc("max", MaxDouble.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.DOUBLE},
        new DataType[] {DataType.DOUBLE}));

    // Min
    sqlFuncs.add(new FunctionDesc("min", MinInt.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.INT},
        new DataType[] {DataType.INT}));
    sqlFuncs.add(new FunctionDesc("min", MinLong.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.LONG},
        new DataType[] {DataType.LONG}));
    sqlFuncs.add(new FunctionDesc("min", MinFloat.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.FLOAT},
        new DataType[] {DataType.FLOAT }));
    sqlFuncs.add(new FunctionDesc("min", MinDouble.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.DOUBLE},
        new DataType[] {DataType.DOUBLE}));
    sqlFuncs.add(new FunctionDesc("min", MinString.class, FunctionType.AGGREGATION,
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
    sqlFuncs.add(new FunctionDesc("count", CountValue.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.LONG},
        new DataType[] {DataType.ANY}));
    sqlFuncs.add(new FunctionDesc("count", CountRows.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.LONG},
        new DataType[] {}));

    // GeoIP
    sqlFuncs.add(new FunctionDesc("in_country", InCountry.class, FunctionType.GENERAL,
        new DataType[] {DataType.BOOLEAN},
        new DataType[] {DataType.STRING, DataType.STRING}));
    sqlFuncs.add(new FunctionDesc("country", Country.class, FunctionType.GENERAL,
        new DataType[] {DataType.STRING},
        new DataType[] {DataType.STRING}));

    // Date
    sqlFuncs.add(new FunctionDesc("date", Date.class, FunctionType.GENERAL,
        new DataType[] {DataType.LONG},
        new DataType[] {DataType.STRING}));

    // Today
    sqlFuncs.add(new FunctionDesc("today", Date.class, FunctionType.GENERAL,
        new DataType[] {DataType.LONG},
        new DataType[] {}));

    sqlFuncs.add(
        new FunctionDesc("random", RandomInt.class, FunctionType.GENERAL,
            new DataType[]{DataType.INT},
            new DataType[]{DataType.INT}));

    return sqlFuncs;
  }

  public MasterContext getContext() {
    return this.context;
  }


  static class WorkerEventDispatcher implements EventHandler<WorkerEvent> {
    List<EventHandler<WorkerEvent>> listofHandlers;

    public WorkerEventDispatcher() {
      listofHandlers = new ArrayList<>();
    }

    @Override
    public void handle(WorkerEvent event) {
      for (EventHandler<WorkerEvent> handler: listofHandlers) {
        handler.handle(event);
      }
    }

    public void addHandler(EventHandler<WorkerEvent> handler) {
      listofHandlers.add(handler);
    }
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
  }

  @Override
  public void stop() {
    try {
      webServer.stop();
    } catch (Exception e) {
      LOG.error(e);
    }

    super.stop();
    LOG.info("TajoMaster main thread exiting");
  }

  public EventHandler getEventHandler() {
    return dispatcher.getEventHandler();
  }

  public String getMasterServerName() {
    return null;
  }

  public boolean isMasterRunning() {
    return getServiceState() == STATE.STARTED;
  }

  public CatalogService getCatalog() {
    return this.catalog;
  }

  public StorageManager getStorageManager() {
    return this.storeManager;
  }

  // TODO - to be improved
  public Collection<TaskStatusProto> getProgressQueries() {
    return null;
  }

  private class QueryEventDispatcher implements EventHandler<QueryEvent> {
    @Override
    public void handle(QueryEvent queryEvent) {
      LOG.info("QueryEvent: " + queryEvent.getQueryId());
      LOG.info("Found: " + context.getQuery(queryEvent.getQueryId()).getContext().getQueryId());
      context.getQuery(queryEvent.getQueryId()).handle(queryEvent);
    }
  }

  public static void main(String[] args) throws Exception {
    StringUtils.startupShutdownMessage(TajoMaster.class, args, LOG);

    try {
      TajoMaster master = new TajoMaster();
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(master),
          SHUTDOWN_HOOK_PRIORITY);
      TajoConf conf = new TajoConf(new YarnConfiguration());
      master.init(conf);
      master.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting JobHistoryServer", t);
      System.exit(-1);
    }
  }

  public class MasterContext {
    private final Map<QueryId, QueryMaster> queries = Maps.newConcurrentMap();
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

    public QueryMaster getQuery(QueryId queryId) {
      return queries.get(queryId);
    }

    public Map<QueryId, QueryMaster> getAllQueries() {
      return queries;
    }

    public AsyncDispatcher getDispatcher() {
      return dispatcher;
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

    public StorageManager getStorageManager() {
      return storeManager;
    }

    public YarnRPC getYarnRPC() {
      return yarnRPC;
    }

    public ClientService getClientService() {
      return clientService;
    }
  }
}