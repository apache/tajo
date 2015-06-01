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

package org.apache.tajo.conf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ConfigKey;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.service.BaseServiceTracker;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.NumberUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.datetime.DateTimeConstants;
import org.apache.tajo.validation.ConstraintViolationException;
import org.apache.tajo.validation.Validator;
import org.apache.tajo.validation.Validators;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class TajoConf extends Configuration {
  private static TimeZone SYSTEM_TIMEZONE;
  private static int DATE_ORDER = -1;
  
  private static final Map<String, ConfVars> vars = TUtil.newHashMap();

  static {
    Configuration.addDefaultResource("catalog-default.xml");
    Configuration.addDefaultResource("catalog-site.xml");
    Configuration.addDefaultResource("storage-default.xml");
    Configuration.addDefaultResource("storage-site.xml");
    Configuration.addDefaultResource("tajo-default.xml");
    Configuration.addDefaultResource("tajo-site.xml");

    for (ConfVars confVars: ConfVars.values()) {
      vars.put(confVars.keyname(), confVars);
    }
  }

  private static final String EMPTY_VALUE = "";

  public TajoConf() {
    super();
  }

  public TajoConf(Configuration conf) {
    super(conf);
  }

  public TajoConf(Path path) {
    super();
    addResource(path);
  }

  @SuppressWarnings("unused")
  public TimeZone getSystemTimezone() {
    return TimeZone.getTimeZone(getVar(ConfVars.$TIMEZONE));
  }

  public void setSystemTimezone(TimeZone timezone) {
    setVar(ConfVars.$TIMEZONE, timezone.getID());
  }

  public static int getDateOrder() {
    if (DATE_ORDER < 0) {
      TajoConf tajoConf = new TajoConf();
      String dateOrder = tajoConf.getVar(ConfVars.$DATE_ORDER);
      if ("YMD".equals(dateOrder)) {
        DATE_ORDER = DateTimeConstants.DATEORDER_YMD;
      } else if ("DMY".equals(dateOrder)) {
        DATE_ORDER = DateTimeConstants.DATEORDER_DMY;
      } else if ("MDY".equals(dateOrder)) {
        DATE_ORDER = DateTimeConstants.DATEORDER_MDY;
      } else {
        DATE_ORDER = DateTimeConstants.DATEORDER_YMD;
      }
    }
    return DATE_ORDER;
  }

  @VisibleForTesting
  public static int setDateOrder(int dateOrder) {
    int oldDateOrder = DATE_ORDER;
    DATE_ORDER = dateOrder;
    return oldDateOrder;
  }

  public static enum ConfVars implements ConfigKey {

    ///////////////////////////////////////////////////////////////////////////////////////
    // Tajo System Configuration
    //
    // They are all static configs which are not changed or not overwritten at all.
    ///////////////////////////////////////////////////////////////////////////////////////

    // a username for a running Tajo cluster
    ROOT_DIR("tajo.rootdir", "file:///tmp/tajo-${user.name}/", 
        Validators.groups(Validators.notNull(), Validators.pathUrl())),
    USERNAME("tajo.username", "${user.name}", Validators.javaString()),

    // Configurable System Directories
    WAREHOUSE_DIR("tajo.warehouse.directory", EMPTY_VALUE, Validators.pathUrl()),
    STAGING_ROOT_DIR("tajo.staging.directory", "/tmp/tajo-${user.name}/staging", Validators.pathUrl()),

    SYSTEM_CONF_PATH("tajo.system-conf.path", EMPTY_VALUE, Validators.pathUrl()),
    SYSTEM_CONF_REPLICA_COUNT("tajo.system-conf.replica-count", 20, Validators.min("1")),

    // Tajo Master Service Addresses
    TAJO_MASTER_UMBILICAL_RPC_ADDRESS("tajo.master.umbilical-rpc.address", "localhost:26001",
        Validators.networkAddr()),
    TAJO_MASTER_CLIENT_RPC_ADDRESS("tajo.master.client-rpc.address", "localhost:26002",
        Validators.networkAddr()),
    TAJO_MASTER_INFO_ADDRESS("tajo.master.info-http.address", "0.0.0.0:26080", Validators.networkAddr()),

    // Tajo Rest Service
    REST_SERVICE_PORT("tajo.rest.service.port", 26880),

    // High availability configurations
    TAJO_MASTER_HA_ENABLE("tajo.master.ha.enable", false, Validators.bool()),
    TAJO_MASTER_HA_MONITOR_INTERVAL("tajo.master.ha.monitor.interval", 5 * 1000), // 5 sec
    TAJO_MASTER_HA_CLIENT_RETRY_MAX_NUM("tajo.master.ha.client.read.retry.max-num", 120), // 120 retry
    TAJO_MASTER_HA_CLIENT_RETRY_PAUSE_TIME("tajo.master.ha.client.read.pause-time", 500), // 500 ms

    // Service discovery
    DEFAULT_SERVICE_TRACKER_CLASS("tajo.discovery.service-tracker.class", BaseServiceTracker.class.getCanonicalName()),
    HA_SERVICE_TRACKER_CLASS("tajo.discovery.ha-service-tracker.class", "org.apache.tajo.ha.HdfsServiceTracker"),

    // Resource tracker service
    RESOURCE_TRACKER_RPC_ADDRESS("tajo.resource-tracker.rpc.address", "localhost:26003",
        Validators.networkAddr()),
    RESOURCE_TRACKER_HEARTBEAT_TIMEOUT("tajo.resource-tracker.heartbeat.timeout-secs", 120 * 1000), // seconds

    // QueryMaster resource
    TAJO_QUERYMASTER_DISK_SLOT("tajo.qm.resource.disk.slots", 0.0f, Validators.min("0.0f")),
    TAJO_QUERYMASTER_MEMORY_MB("tajo.qm.resource.memory-mb", 512, Validators.min("64")),
    TAJO_QUERYMASTER_ALLOCATION_TIMEOUT("tajo.qm.resource.allocation.timeout", "3 sec"),

    // Tajo Worker Service Addresses
    WORKER_INFO_ADDRESS("tajo.worker.info-http.address", "0.0.0.0:28080", Validators.networkAddr()),
    WORKER_QM_INFO_ADDRESS("tajo.worker.qm-info-http.address", "0.0.0.0:28081", Validators.networkAddr()),
    WORKER_PEER_RPC_ADDRESS("tajo.worker.peer-rpc.address", "0.0.0.0:28091", Validators.networkAddr()),
    WORKER_CLIENT_RPC_ADDRESS("tajo.worker.client-rpc.address", "0.0.0.0:28092", Validators.networkAddr()),
    WORKER_QM_RPC_ADDRESS("tajo.worker.qm-rpc.address", "0.0.0.0:28093", Validators.networkAddr()),

    // Tajo Worker Temporal Directories
    WORKER_TEMPORAL_DIR("tajo.worker.tmpdir.locations", "/tmp/tajo-${user.name}/tmpdir", Validators.pathUrlList()),
    WORKER_TEMPORAL_DIR_CLEANUP("tajo.worker.tmpdir.cleanup-at-startup", false, Validators.bool()),

    // Tajo Worker Resources
    WORKER_RESOURCE_AVAILABLE_CPU_CORES("tajo.worker.resource.cpu-cores",
        Runtime.getRuntime().availableProcessors(), Validators.min("1")),
    WORKER_RESOURCE_AVAILABLE_MEMORY_MB("tajo.worker.resource.memory-mb", 1024, Validators.min("64")),
    @Deprecated
    WORKER_RESOURCE_AVAILABLE_DISKS("tajo.worker.resource.disks", 1.0f),
    WORKER_RESOURCE_AVAILABLE_DISKS_NUM("tajo.worker.resource.disks.num", 1, Validators.min("1")),
    WORKER_RESOURCE_AVAILABLE_DISK_PARALLEL_NUM("tajo.worker.resource.disk.parallel-execution.num", 2,
        Validators.min("1")),
    WORKER_EXECUTION_MAX_SLOTS("tajo.worker.parallel-execution.max-num", 2),
    WORKER_RESOURCE_DFS_DIR_AWARE("tajo.worker.resource.dfs-dir-aware", false, Validators.bool()),

    // Tajo Worker Dedicated Resources
    WORKER_RESOURCE_DEDICATED("tajo.worker.resource.dedicated", false, Validators.bool()),
    WORKER_RESOURCE_DEDICATED_MEMORY_RATIO("tajo.worker.resource.dedicated-memory-ratio", 0.8f, 
        Validators.range("0.0f", "1.0f")),

    // Tajo History
    WORKER_HISTORY_EXPIRE_PERIOD("tajo.worker.history.expire-interval-minutes", 60), // 1 hours
    QUERYMASTER_HISTORY_EXPIRE_PERIOD("tajo.qm.history.expire-interval-minutes", 6 * 60), // 6 hours

    WORKER_HEARTBEAT_INTERVAL("tajo.worker.heartbeat.interval", 10 * 1000),  // 10 sec

    // Resource Manager
    RESOURCE_MANAGER_CLASS("tajo.resource.manager", "org.apache.tajo.master.rm.TajoWorkerResourceManager",
        Validators.groups(Validators.notNull(), Validators.clazz())),

    // Catalog
    CATALOG_ADDRESS("tajo.catalog.client-rpc.address", "localhost:26005", Validators.networkAddr()),


    // for Yarn Resource Manager ----------------------------------------------

    /** how many launching TaskRunners in parallel */
    @Deprecated
    YARN_RM_TASKRUNNER_LAUNCH_PARALLEL_NUM("tajo.yarn-rm.parallel-task-runner-launcher-num",
        Runtime.getRuntime().availableProcessors() * 2),

    // Query Configuration
    QUERY_SESSION_TIMEOUT("tajo.query.session.timeout-sec", 60, Validators.min("0")),
    QUERY_SESSION_QUERY_CACHE_SIZE("tajo.query.session.query-cache-size-kb", 1024, Validators.min("0")),

    // Shuffle Configuration --------------------------------------------------
    PULLSERVER_PORT("tajo.pullserver.port", 0, Validators.range("0", "65535")),
    SHUFFLE_SSL_ENABLED_KEY("tajo.pullserver.ssl.enabled", false, Validators.bool()),
    SHUFFLE_FILE_FORMAT("tajo.shuffle.file-format", "RAW"),
    SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM("tajo.shuffle.fetcher.parallel-execution.max-num", 2),
    SHUFFLE_FETCHER_CHUNK_MAX_SIZE("tajo.shuffle.fetcher.chunk.max-size",  8192),
    SHUFFLE_FETCHER_READ_TIMEOUT("tajo.shuffle.fetcher.read.timeout-sec", 120),
    SHUFFLE_FETCHER_READ_RETRY_MAX_NUM("tajo.shuffle.fetcher.read.retry.max-num", 20),
    SHUFFLE_HASH_APPENDER_BUFFER_SIZE("tajo.shuffle.hash.appender.buffer.size", 10000),
    SHUFFLE_HASH_APPENDER_PAGE_VOLUME("tajo.shuffle.hash.appender.page.volumn-mb", 30),
    HASH_SHUFFLE_PARENT_DIRS("tajo.hash.shuffle.parent.dirs.count", 10),

    // Storage Configuration --------------------------------------------------
    ROWFILE_SYNC_INTERVAL("rowfile.sync.interval", 100),
    MINIMUM_SPLIT_SIZE("tajo.min.split.size", (long) 1),
    // for RCFile
    HIVEUSEEXPLICITRCFILEHEADER("tajo.exec.rcfile.use.explicit.header", true, Validators.bool()),

    // RPC --------------------------------------------------------------------
    //  Internal RPC Client
    INTERNAL_RPC_CLIENT_WORKER_THREAD_NUM("tajo.internal.rpc.client.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 2),

    // Internal RPC Server
    MASTER_RPC_SERVER_WORKER_THREAD_NUM("tajo.master.rpc.server.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 2),
    QUERY_MASTER_RPC_SERVER_WORKER_THREAD_NUM("tajo.querymaster.rpc.server.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 2),
    WORKER_RPC_SERVER_WORKER_THREAD_NUM("tajo.worker.rpc.server.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 2),
    CATALOG_RPC_SERVER_WORKER_THREAD_NUM("tajo.catalog.rpc.server.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 2),
    SHUFFLE_RPC_SERVER_WORKER_THREAD_NUM("tajo.shuffle.rpc.server.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 2),

    // Client RPC
    RPC_CLIENT_WORKER_THREAD_NUM("tajo.rpc.client.worker-thread-num", 4),

    SHUFFLE_RPC_CLIENT_WORKER_THREAD_NUM("tajo.shuffle.rpc.client.worker-thread-num",
        Runtime.getRuntime().availableProcessors()),

    //Client service RPC Server
    MASTER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM("tajo.master.service.rpc.server.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 1),
    WORKER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM("tajo.worker.service.rpc.server.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 1),
    REST_SERVICE_RPC_SERVER_WORKER_THREAD_NUM("tajo.rest.service.rpc.server.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 1),

    // Task Configuration -----------------------------------------------------
    TASK_DEFAULT_MEMORY("tajo.task.memory-slot-mb.default", 512),
    TASK_DEFAULT_DISK("tajo.task.disk-slot.default", 0.5f),
    TASK_DEFAULT_SIZE("tajo.task.size-mb", 128),

    // Query and Optimization -------------------------------------------------
    // This class provides a ordered list of logical plan rewrite rule classes.
    LOGICAL_PLAN_REWRITE_RULE_PROVIDER_CLASS("tajo.plan.logical.rewriter.provider",
        "org.apache.tajo.plan.rewrite.BaseLogicalPlanRewriteRuleProvider"),
    // This class provides a ordered list of global plan rewrite rule classes.
    GLOBAL_PLAN_REWRITE_RULE_PROVIDER_CLASS("tajo.plan.global.rewriter.provider",
        "org.apache.tajo.engine.planner.global.rewriter.BaseGlobalPlanRewriteRuleProvider"),
    EXECUTOR_EXTERNAL_SORT_THREAD_NUM("tajo.executor.external-sort.thread-num", 1),
    EXECUTOR_EXTERNAL_SORT_FANOUT("tajo.executor.external-sort.fanout-num", 8),

    EXECUTOR_INNER_JOIN_INMEMORY_HASH_TABLE_SIZE("tajo.executor.join.inner.in-memory-table-num", (long)1000000),

    // Metrics ----------------------------------------------------------------
    METRICS_PROPERTY_FILENAME("tajo.metrics.property.file", "tajo-metrics.properties"),

    // Query History  ---------------------------------------------------------
    HISTORY_QUERY_DIR("tajo.history.query.dir", STAGING_ROOT_DIR.defaultVal + "/history"),
    HISTORY_TASK_DIR("tajo.history.task.dir", "file:///tmp/tajo-${user.name}/history"),
    HISTORY_EXPIRY_TIME_DAY("tajo.history.expiry-time-day", 7),
    HISTORY_QUERY_REPLICATION("tajo.history.query.replication", 1, Validators.min("1")),
    HISTORY_TASK_REPLICATION("tajo.history.task.replication", 1, Validators.min("1")),

    // Misc -------------------------------------------------------------------
    // Fragment
    // When making physical plan, the length of fragment is used to determine the physical operation.
    // Some storage does not know the size of the fragment.
    // In this case PhysicalPlanner uses this value to determine.
    FRAGMENT_ALTERNATIVE_UNKNOWN_LENGTH("tajo.fragment.alternative.unknown.length", (long)(512 * 1024 * 1024)),

    // Geo IP
    GEOIP_DATA("tajo.function.geoip-database-location", ""),

    // Python UDF
    PYTHON_CODE_DIR("tajo.function.python.code-dir", ""),
    PYTHON_CONTROLLER_LOG_DIR("tajo.function.python.controller.log-dir", ""),

    /////////////////////////////////////////////////////////////////////////////////
    // User Session Configuration
    //
    // All session variables begin with dollar($) sign. They are default configs
    // for session variables. Do not directly use the following configs. Instead,
    // please use QueryContext in order to access session variables.
    //
    // Also, users can change the default values of session variables in tajo-site.xml.
    /////////////////////////////////////////////////////////////////////////////////


    $EMPTY("tajo._", ""),

    // Query and Optimization ---------------------------------------------------

    // for distributed query strategies
    $DIST_QUERY_BROADCAST_JOIN_THRESHOLD("tajo.dist-query.join.broadcast.threshold-bytes", (long)5 * 1048576),

    $DIST_QUERY_JOIN_TASK_VOLUME("tajo.dist-query.join.task-volume-mb", 128),
    $DIST_QUERY_SORT_TASK_VOLUME("tajo.dist-query.sort.task-volume-mb", 128),
    $DIST_QUERY_GROUPBY_TASK_VOLUME("tajo.dist-query.groupby.task-volume-mb", 128),

    $DIST_QUERY_JOIN_PARTITION_VOLUME("tajo.dist-query.join.partition-volume-mb", 128, Validators.min("1")),
    $DIST_QUERY_GROUPBY_PARTITION_VOLUME("tajo.dist-query.groupby.partition-volume-mb", 256, Validators.min("1")),
    $DIST_QUERY_TABLE_PARTITION_VOLUME("tajo.dist-query.table-partition.task-volume-mb", 256, Validators.min("1")),

    $GROUPBY_MULTI_LEVEL_ENABLED("tajo.dist-query.groupby.multi-level-aggr", true),

    // WARN "tajo.yarn-rm.parallel-task-runner-launcher-num" should be set enough to avoid deadlock
    $QUERY_EXECUTE_PARALLEL_MAX("tajo.query.execute.parallel.max", 1),

    // for physical Executors
    $EXECUTOR_EXTERNAL_SORT_BUFFER_SIZE("tajo.executor.external-sort.buffer-mb", 200L),
    $EXECUTOR_HASH_JOIN_SIZE_THRESHOLD("tajo.executor.join.common.in-memory-hash-threshold-bytes",
        (long)256 * 1048576),
    $EXECUTOR_INNER_HASH_JOIN_SIZE_THRESHOLD("tajo.executor.join.inner.in-memory-hash-threshold-bytes",
        (long)256 * 1048576),
    $EXECUTOR_OUTER_HASH_JOIN_SIZE_THRESHOLD("tajo.executor.join.outer.in-memory-hash-threshold-bytes",
        (long)256 * 1048576),
    $EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD("tajo.executor.groupby.in-memory-hash-threshold-bytes",
        (long)256 * 1048576),
    $MAX_OUTPUT_FILE_SIZE("tajo.query.max-outfile-size-mb", 0), // zero means infinite
    $CODEGEN("tajo.executor.codegen.enabled", false), // Runtime code generation (todo this is broken)

    // Client -----------------------------------------------------------------
    $CLIENT_SESSION_EXPIRY_TIME("tajo.client.session.expiry-time-sec", 3600), // default time is one hour.

    // Command line interface and its behavior --------------------------------
    $CLI_MAX_COLUMN("tajo.cli.max_columns", 120),
    $CLI_NULL_CHAR("tajo.cli.nullchar", ""),
    $CLI_PRINT_PAUSE_NUM_RECORDS("tajo.cli.print.pause.num.records", 100),
    $CLI_PRINT_PAUSE("tajo.cli.print.pause", true),
    $CLI_PRINT_ERROR_TRACE("tajo.cli.print.error.trace", true),
    $CLI_OUTPUT_FORMATTER_CLASS("tajo.cli.output.formatter", "org.apache.tajo.cli.tsql.DefaultTajoCliOutputFormatter"),
    $CLI_ERROR_STOP("tajo.cli.error.stop", false),

    // Timezone & Date ----------------------------------------------------------
    $TIMEZONE("tajo.timezone", TimeZone.getDefault().getID()),
    $DATE_ORDER("tajo.datetime.date-order", "YMD"),

    // FILE FORMAT
    $TEXT_NULL("tajo.text.null", "\\\\N"),

    // Only for Debug and Testing
    $DEBUG_ENABLED("tajo.debug.enabled", false),
    $TEST_BROADCAST_JOIN_ENABLED("tajo.dist-query.join.auto-broadcast", true),
    $TEST_JOIN_OPT_ENABLED("tajo.test.plan.join-optimization.enabled", true),
    $TEST_FILTER_PUSHDOWN_ENABLED("tajo.test.plan.filter-pushdown.enabled", true),
    $TEST_MIN_TASK_NUM("tajo.test.min-task-num", -1),
    $TEST_PLAN_SHAPE_FIX_ENABLED("tajo.test.plan.shape.fix.enabled", false),  // used for explain statement test

    // Behavior Control ---------------------------------------------------------
    $BEHAVIOR_ARITHMETIC_ABORT("tajo.behavior.arithmetic-abort", false),

    // ResultSet ---------------------------------------------------------
    $RESULT_SET_FETCH_ROWNUM("tajo.resultset.fetch.rownum", 200),
    ;

    public final String varname;
    public final String defaultVal;
    public final int defaultIntVal;
    public final long defaultLongVal;
    public final float defaultFloatVal;
    public final Class<?> valClass;
    public final boolean defaultBoolVal;

    private final VarType type;
    private Validator validator;

    ConfVars(String varname, String defaultVal) {
      this.varname = varname;
      this.valClass = String.class;
      this.defaultVal = defaultVal;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
      this.type = VarType.STRING;
    }
    
    ConfVars(String varname, String defaultVal, Validator validator) {
      this(varname, defaultVal);
      this.validator = validator;
    }

    ConfVars(String varname, int defaultIntVal) {
      this.varname = varname;
      this.valClass = Integer.class;
      this.defaultVal = Integer.toString(defaultIntVal);
      this.defaultIntVal = defaultIntVal;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
      this.type = VarType.INT;
    }
    
    ConfVars(String varname, int defaultIntVal, Validator validator) {
      this(varname, defaultIntVal);
      this.validator = validator;
    }

    ConfVars(String varname, long defaultLongVal) {
      this.varname = varname;
      this.valClass = Long.class;
      this.defaultVal = Long.toString(defaultLongVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = defaultLongVal;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
      this.type = VarType.LONG;
    }
    
    ConfVars(String varname, long defaultLongVal, Validator validator) {
      this(varname, defaultLongVal);
      this.validator = validator;
    }

    ConfVars(String varname, float defaultFloatVal) {
      this.varname = varname;
      this.valClass = Float.class;
      this.defaultVal = Float.toString(defaultFloatVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = defaultFloatVal;
      this.defaultBoolVal = false;
      this.type = VarType.FLOAT;
    }
    
    ConfVars(String varname, float defaultFloatVal, Validator validator) {
      this(varname, defaultFloatVal);
      this.validator = validator;
    }

    ConfVars(String varname, boolean defaultBoolVal) {
      this.varname = varname;
      this.valClass = Boolean.class;
      this.defaultVal = Boolean.toString(defaultBoolVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = defaultBoolVal;
      this.type = VarType.BOOLEAN;
    }
    
    ConfVars(String varname, boolean defaultBoolVal, Validator validator) {
      this(varname, defaultBoolVal);
      this.validator = validator;
    }

    enum VarType {
      STRING { void checkType(String value) throws Exception { } },
      INT { void checkType(String value) throws Exception { Integer.valueOf(value); } },
      LONG { void checkType(String value) throws Exception { Long.valueOf(value); } },
      FLOAT { void checkType(String value) throws Exception { Float.valueOf(value); } },
      BOOLEAN { void checkType(String value) throws Exception { Boolean.valueOf(value); } };

      boolean isType(String value) {
        try { checkType(value); } catch (Exception e) { return false; }
        return true;
      }
      String typeString() { return name().toUpperCase();}
      abstract void checkType(String value) throws Exception;
    }

    @Override
    public String keyname() {
      return varname;
    }

    @Override
    public ConfigType type() {
      return ConfigType.SYSTEM;
    }

    @Override
    public Class<?> valueClass() {
      return valClass;
    }

    @Override
    public Validator validator() {
      return validator;
    }
  }

  public static int getIntVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Integer.class);
    return conf.getInt(var.varname, var.defaultIntVal);
  }

  public static void setIntVar(Configuration conf, ConfVars var, int val) {
    assert (var.valClass == Integer.class);
    conf.setInt(var.varname, val);
  }

  public int getIntVar(ConfVars var) {
    return getIntVar(this, var);
  }

  public void setIntVar(ConfVars var, int val) {
    setIntVar(this, var, val);
  }

  public static long getLongVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Long.class || var.valClass == Integer.class);
    if (var.valClass == Integer.class) {
      return conf.getInt(var.varname, var.defaultIntVal);
    } else {
      return conf.getLong(var.varname, var.defaultLongVal);
    }
  }

  public static long getLongVar(Configuration conf, ConfVars var, long defaultVal) {
    return conf.getLong(var.varname, defaultVal);
  }

  public static void setLongVar(Configuration conf, ConfVars var, long val) {
    assert (var.valClass == Long.class);
    conf.setLong(var.varname, val);
  }

  public long getLongVar(ConfVars var) {
    return getLongVar(this, var);
  }

  public void setLongVar(ConfVars var, long val) {
    setLongVar(this, var, val);
  }

  public static float getFloatVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Float.class);
    return conf.getFloat(var.varname, var.defaultFloatVal);
  }

  public static float getFloatVar(Configuration conf, ConfVars var, float defaultVal) {
    return conf.getFloat(var.varname, defaultVal);
  }

  public static void setFloatVar(Configuration conf, ConfVars var, float val) {
    assert (var.valClass == Float.class);
    conf.setFloat(var.varname, val);
  }

  public float getFloatVar(ConfVars var) {
    return getFloatVar(this, var);
  }

  public void setFloatVar(ConfVars var, float val) {
    setFloatVar(this, var, val);
  }

  public static boolean getBoolVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Boolean.class);
    return conf.getBoolean(var.varname, var.defaultBoolVal);
  }

  public static boolean getBoolVar(Configuration conf, ConfVars var, boolean defaultVal) {
    return conf.getBoolean(var.varname, defaultVal);
  }

  public static void setBoolVar(Configuration conf, ConfVars var, boolean val) {
    assert (var.valClass == Boolean.class);
    conf.setBoolean(var.varname, val);
  }

  public boolean getBoolVar(ConfVars var) {
    return getBoolVar(this, var);
  }

  public void setBoolVar(ConfVars var, boolean val) {
    setBoolVar(this, var, val);
  }

  // borrowed from HIVE-5799
  public static long getTimeVar(Configuration conf, ConfVars var, TimeUnit outUnit) {
    return toTime(getVar(conf, var), outUnit);
  }

  public static void setTimeVar(Configuration conf, ConfVars var, long time, TimeUnit timeunit) {
    assert (var.valClass == String.class) : var.varname;
    conf.set(var.varname, time + stringFor(timeunit));
  }

  public long getTimeVar(ConfVars var, TimeUnit outUnit) {
    return getTimeVar(this, var, outUnit);
  }

  public void setTimeVar(ConfVars var, long time, TimeUnit outUnit) {
    setTimeVar(this, var, time, outUnit);
  }

  public static long toTime(String value, TimeUnit outUnit) {
    String[] parsed = parseTime(value.trim());
    return outUnit.convert(Long.valueOf(parsed[0].trim()), unitFor(parsed[1].trim()));
  }

  private static String[] parseTime(String value) {
    char[] chars = value.toCharArray();
    int i = 0;
    for (; i < chars.length && (chars[i] == '-' || Character.isDigit(chars[i])); i++) {
    }
    return new String[] {value.substring(0, i), value.substring(i)};
  }

  public static TimeUnit unitFor(String unit) {
    unit = unit.trim().toLowerCase();
    if (unit.isEmpty() || unit.equals("l")) {
      return TimeUnit.MILLISECONDS;
    } else if (unit.equals("d") || unit.startsWith("day")) {
      return TimeUnit.DAYS;
    } else if (unit.equals("h") || unit.startsWith("hour")) {
      return TimeUnit.HOURS;
    } else if (unit.equals("m") || unit.startsWith("min")) {
      return TimeUnit.MINUTES;
    } else if (unit.equals("s") || unit.startsWith("sec")) {
      return TimeUnit.SECONDS;
    } else if (unit.equals("ms") || unit.startsWith("msec")) {
      return TimeUnit.MILLISECONDS;
    } else if (unit.equals("us") || unit.startsWith("usec")) {
      return TimeUnit.MICROSECONDS;
    } else if (unit.equals("ns") || unit.startsWith("nsec")) {
      return TimeUnit.NANOSECONDS;
    }
    throw new IllegalArgumentException("Invalid time unit " + unit);
  }

  public static String stringFor(TimeUnit timeunit) {
    switch (timeunit) {
      case DAYS: return "day";
      case HOURS: return "hour";
      case MINUTES: return "min";
      case SECONDS: return "sec";
      case MILLISECONDS: return "msec";
      case MICROSECONDS: return "usec";
      case NANOSECONDS: return "nsec";
    }
    throw new IllegalArgumentException("Invalid timeunit " + timeunit);
  }

  public void setClassVar(ConfVars var, Class<?> clazz) {
    setVar(var, clazz.getCanonicalName());
  }

  public Class<?> getClassVar(ConfVars var) {
    String valueString = getVar(var);

    try {
      return getClassByName(valueString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getVar(Configuration conf, ConfVars var) {
    return conf.get(var.varname, var.defaultVal);
  }

  public static String getVar(Configuration conf, ConfVars var, String defaultVal) {
    return conf.get(var.varname, defaultVal);
  }

  public static void setVar(Configuration conf, ConfVars var, String val) {
    assert (var.valClass == String.class);
    conf.set(var.varname, val);
  }

  public static ConfVars getConfVars(String name) {
    return vars.get(name);
  }

  public String getVar(ConfVars var) {
    return getVar(this, var);
  }

  public void setVar(ConfVars var, String val) {
    setVar(this, var, val);
  }

  public void logVars(PrintStream ps) {
    for (ConfVars one : ConfVars.values()) {
      ps.println(one.varname + "=" + ((get(one.varname) != null) ? get(one.varname) : ""));
    }
  }

  public InetSocketAddress getSocketAddrVar(ConfVars var) {
    final String address = getVar(var);
    return NetUtils.createSocketAddr(address);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tajo System Specific Methods
  /////////////////////////////////////////////////////////////////////////////

  public static Path getTajoRootDir(TajoConf conf) {
    String rootPath = conf.getVar(ConfVars.ROOT_DIR);
    Preconditions.checkNotNull(rootPath,
        ConfVars.ROOT_DIR.varname + " must be set before a Tajo Cluster starts up");
    return new Path(rootPath);
  }

  public static Path getWarehouseDir(TajoConf conf) {
    String warehousePath = conf.getVar(ConfVars.WAREHOUSE_DIR);
    if (warehousePath == null || warehousePath.equals("")) {
      Path rootDir = getTajoRootDir(conf);
      warehousePath = new Path(rootDir, TajoConstants.WAREHOUSE_DIR_NAME).toUri().toString();
      conf.setVar(ConfVars.WAREHOUSE_DIR, warehousePath);
      return new Path(warehousePath);
    } else {
      return new Path(warehousePath);
    }
  }

  public static Path getSystemDir(TajoConf conf) {
    Path rootPath = getTajoRootDir(conf);
    return new Path(rootPath, TajoConstants.SYSTEM_DIR_NAME);
  }

  public static Path getSystemResourceDir(TajoConf conf) {
    return new Path(getSystemDir(conf), TajoConstants.SYSTEM_RESOURCE_DIR_NAME);
  }

  public static Path getSystemHADir(TajoConf conf) {
    return new Path(getSystemDir(conf), TajoConstants.SYSTEM_HA_DIR_NAME);
  }

  private static boolean hasScheme(String path) {
    return path.indexOf("file:/") == 0 || path.indexOf("hdfs:/") == 0;
  }

  /**
   * It returns the default root staging directory used by queries without a target table or
   * a specified output directory. An example query is <pre>SELECT a,b,c FROM XXX;</pre>.
   *
   * @param conf TajoConf
   * @return Path which points the default staging directory
   * @throws IOException
   */
  public static Path getDefaultRootStagingDir(TajoConf conf) throws IOException {
    String stagingDirString = conf.getVar(ConfVars.STAGING_ROOT_DIR);
    if (!hasScheme(stagingDirString)) {
      Path warehousePath = getWarehouseDir(conf);
      FileSystem fs = warehousePath.getFileSystem(conf);
      Path path = new Path(fs.getUri().toString(), stagingDirString);
      conf.setVar(ConfVars.STAGING_ROOT_DIR, path.toString());
      return path;
    }
    return new Path(stagingDirString);
  }

  public static Path getQueryHistoryDir(TajoConf conf) throws IOException {
    String historyDirString = conf.getVar(ConfVars.HISTORY_QUERY_DIR);
    if (!hasScheme(historyDirString)) {
      Path stagingPath = getDefaultRootStagingDir(conf);
      FileSystem fs = stagingPath.getFileSystem(conf);
      Path path = new Path(fs.getUri().toString(), historyDirString);
      conf.setVar(ConfVars.HISTORY_QUERY_DIR, path.toString());
      return path;
    }
    return new Path(historyDirString);
  }

  public static Path getTaskHistoryDir(TajoConf conf) throws IOException {
    String historyDirString = conf.getVar(ConfVars.HISTORY_TASK_DIR);
    if (!hasScheme(historyDirString)) {
      //Local dir
      historyDirString = "file://" + historyDirString;
    }
    return new Path(historyDirString);
  }

  public static Path getSystemConfPath(TajoConf conf) {
    String systemConfPathStr = conf.getVar(ConfVars.SYSTEM_CONF_PATH);
    if (systemConfPathStr == null || systemConfPathStr.equals("")) {
      Path systemResourcePath = getSystemResourceDir(conf);
      Path systemConfPath = new Path(systemResourcePath, TajoConstants.SYSTEM_CONF_FILENAME);
      conf.setVar(ConfVars.SYSTEM_CONF_PATH, systemConfPath.toString());
      return systemConfPath;
    } else {
      return new Path(systemConfPathStr);
    }
  }
  
  /**
   * validateProperty function will fetch pre-defined configuration property by keyname.
   * If found, it will validate the supplied value with these validators.
   * 
   * @param name - a string containing specific key
   * @param value - a string containing value
   * @throws ConstraintViolationException
   */
  public void validateProperty(String name, String value) throws ConstraintViolationException {
    ConfigKey configKey = null;
    configKey = TajoConf.getConfVars(name);
    if (configKey == null) {
      configKey = SessionVars.get(name);
    }
    if (configKey != null && configKey.validator() != null && configKey.valueClass() != null) {
      Object valueObj = value;
      if (Number.class.isAssignableFrom(configKey.valueClass())) {
        valueObj = NumberUtil.numberValue(configKey.valueClass(), value);
        if (valueObj == null) {
          return;
        }
      }
      configKey.validator().validate(valueObj, true);
    }
  }

}
