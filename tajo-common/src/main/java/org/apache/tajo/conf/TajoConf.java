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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ConfigKey;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.datetime.DateTimeConstants;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TajoConf extends Configuration {

  private static TimeZone CURRENT_TIMEZONE;
  private static int DATE_ORDER = -1;
  private static final ReentrantReadWriteLock confLock = new ReentrantReadWriteLock();
  private static final Lock writeLock = confLock.writeLock();
  private static final Lock readLock = confLock.readLock();

  static {
    Configuration.addDefaultResource("catalog-default.xml");
    Configuration.addDefaultResource("catalog-site.xml");
    Configuration.addDefaultResource("storage-default.xml");
    Configuration.addDefaultResource("storage-site.xml");
    Configuration.addDefaultResource("tajo-default.xml");
    Configuration.addDefaultResource("tajo-site.xml");

    confStaticInit();
  }

  private static final String EMPTY_VALUE = "";

  private static final Map<String, ConfVars> vars = TUtil.newHashMap();

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

  private static void confStaticInit() {
    TimeZone.setDefault(getCurrentTimeZone());
    getDateOrder();
  }

  public static TimeZone getCurrentTimeZone() {
    writeLock.lock();
    try {
      if (CURRENT_TIMEZONE == null) {
        TajoConf tajoConf = new TajoConf();
        CURRENT_TIMEZONE = TimeZone.getTimeZone(tajoConf.getVar(ConfVars.$TIMEZONE));
      }
      return CURRENT_TIMEZONE;
    } finally {
      writeLock.unlock();
    }
  }

  public static TimeZone setCurrentTimeZone(TimeZone timeZone) {
    writeLock.lock();
    try {
      TimeZone oldTimeZone = CURRENT_TIMEZONE;
      CURRENT_TIMEZONE = timeZone;
      return oldTimeZone;
    } finally {
      writeLock.unlock();
    }
  }

  public static int getDateOrder() {
    writeLock.lock();
    try {
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
    } finally {
      writeLock.unlock();
    }
  }

  public static int setDateOrder(int dateOrder) {
    writeLock.lock();
    try {
      int oldDateOrder = DATE_ORDER;
      DATE_ORDER = dateOrder;
      return oldDateOrder;
    } finally {
    	writeLock.unlock();
    }
  }

  public static enum ConfVars implements ConfigKey {

    ///////////////////////////////////////////////////////////////////////////////////////
    // Tajo System Configuration
    //
    // They are all static configs which are not changed or not overwritten at all.
    ///////////////////////////////////////////////////////////////////////////////////////

    // a username for a running Tajo cluster
    ROOT_DIR("tajo.rootdir", "file:///tmp/tajo-${user.name}/"),
    USERNAME("tajo.username", "${user.name}"),

    // Configurable System Directories
    WAREHOUSE_DIR("tajo.warehouse.directory", EMPTY_VALUE),
    STAGING_ROOT_DIR("tajo.staging.directory", "/tmp/tajo-${user.name}/staging"),

    SYSTEM_CONF_PATH("tajo.system-conf.path", EMPTY_VALUE),
    SYSTEM_CONF_REPLICA_COUNT("tajo.system-conf.replica-count", 20),

    // Tajo Master Service Addresses
    TAJO_MASTER_UMBILICAL_RPC_ADDRESS("tajo.master.umbilical-rpc.address", "localhost:26001"),
    TAJO_MASTER_CLIENT_RPC_ADDRESS("tajo.master.client-rpc.address", "localhost:26002"),
    TAJO_MASTER_INFO_ADDRESS("tajo.master.info-http.address", "0.0.0.0:26080"),

    // Tajo Master HA Configurations
    TAJO_MASTER_HA_ENABLE("tajo.master.ha.enable", false),
    TAJO_MASTER_HA_MONITOR_INTERVAL("tajo.master.ha.monitor.interval", 5 * 1000), // 5 sec

    // Resource tracker service
    RESOURCE_TRACKER_RPC_ADDRESS("tajo.resource-tracker.rpc.address", "localhost:26003"),
    RESOURCE_TRACKER_HEARTBEAT_TIMEOUT("tajo.resource-tracker.heartbeat.timeout-secs", 120 * 1000), // seconds

    // QueryMaster resource
    TAJO_QUERYMASTER_DISK_SLOT("tajo.qm.resource.disk.slots", 0.0f),
    TAJO_QUERYMASTER_MEMORY_MB("tajo.qm.resource.memory-mb", 512),

    // Tajo Worker Service Addresses
    WORKER_INFO_ADDRESS("tajo.worker.info-http.address", "0.0.0.0:28080"),
    WORKER_QM_INFO_ADDRESS("tajo.worker.qm-info-http.address", "0.0.0.0:28081"),
    WORKER_PEER_RPC_ADDRESS("tajo.worker.peer-rpc.address", "0.0.0.0:28091"),
    WORKER_CLIENT_RPC_ADDRESS("tajo.worker.client-rpc.address", "0.0.0.0:28092"),
    WORKER_QM_RPC_ADDRESS("tajo.worker.qm-rpc.address", "0.0.0.0:28093"),

    // Tajo Worker Temporal Directories
    WORKER_TEMPORAL_DIR("tajo.worker.tmpdir.locations", "/tmp/tajo-${user.name}/tmpdir"),
    WORKER_TEMPORAL_DIR_CLEANUP("tajo.worker.tmpdir.cleanup-at-startup", false),

    // Tajo Worker Resources
    WORKER_RESOURCE_AVAILABLE_CPU_CORES("tajo.worker.resource.cpu-cores", 1),
    WORKER_RESOURCE_AVAILABLE_MEMORY_MB("tajo.worker.resource.memory-mb", 1024),
    WORKER_RESOURCE_AVAILABLE_DISKS("tajo.worker.resource.disks", 1.0f),
    WORKER_EXECUTION_MAX_SLOTS("tajo.worker.parallel-execution.max-num", 2),
    WORKER_RESOURCE_DFS_DIR_AWARE("tajo.worker.resource.dfs-dir-aware", false),

    // Tajo Worker Dedicated Resources
    WORKER_RESOURCE_DEDICATED("tajo.worker.resource.dedicated", false),
    WORKER_RESOURCE_DEDICATED_MEMORY_RATIO("tajo.worker.resource.dedicated-memory-ratio", 0.8f),

    // Tajo Worker History
    WORKER_HISTORY_EXPIRE_PERIOD("tajo.worker.history.expire-interval-minutes", 12 * 60), // 12 hours

    WORKER_HEARTBEAT_TIMEOUT("tajo.worker.heartbeat.timeout", 120 * 1000),  // 120 sec

    // Resource Manager
    RESOURCE_MANAGER_CLASS("tajo.resource.manager", "org.apache.tajo.master.rm.TajoWorkerResourceManager"),

    // Catalog
    CATALOG_ADDRESS("tajo.catalog.client-rpc.address", "localhost:26005"),


    // for Yarn Resource Manager ----------------------------------------------

    /** how many launching TaskRunners in parallel */
    YARN_RM_QUERY_MASTER_MEMORY_MB("tajo.querymaster.memory-mb", 512),
    YARN_RM_QUERY_MASTER_DISKS("tajo.yarn-rm.querymaster.disks", 1),
    YARN_RM_TASKRUNNER_LAUNCH_PARALLEL_NUM("tajo.yarn-rm.parallel-task-runner-launcher-num", 16),
    YARN_RM_WORKER_NUMBER_PER_NODE("tajo.yarn-rm.max-worker-num-per-node", 8),

    // Query Configuration
    QUERY_SESSION_TIMEOUT("tajo.query.session.timeout-sec", 60),

    // Shuffle Configuration --------------------------------------------------
    PULLSERVER_PORT("tajo.pullserver.port", 0),
    SHUFFLE_SSL_ENABLED_KEY("tajo.pullserver.ssl.enabled", false),
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
    HIVEUSEEXPLICITRCFILEHEADER("tajo.exec.rcfile.use.explicit.header", true),

    // for Storage Manager v2
    STORAGE_MANAGER_VERSION_2("tajo.storage-manager.v2", false),
    STORAGE_MANAGER_DISK_SCHEDULER_MAX_READ_BYTES_PER_SLOT("tajo.storage-manager.max-read-bytes", 8 * 1024 * 1024),
    STORAGE_MANAGER_DISK_SCHEDULER_REPORT_INTERVAL("tajo.storage-manager.disk-scheduler.report-interval", 60 * 1000),
    STORAGE_MANAGER_CONCURRENCY_PER_DISK("tajo.storage-manager.disk-scheduler.per-disk-concurrency", 2),


    // RPC --------------------------------------------------------------------
    RPC_POOL_MAX_IDLE("tajo.rpc.pool.idle.max", 10),

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

    //Client service RPC Server
    MASTER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM("tajo.master.service.rpc.server.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 1),
    WORKER_SERVICE_RPC_SERVER_WORKER_THREAD_NUM("tajo.worker.service.rpc.server.worker-thread-num",
        Runtime.getRuntime().availableProcessors() * 1),

    // Task Configuration -----------------------------------------------------
    TASK_DEFAULT_MEMORY("tajo.task.memory-slot-mb.default", 512),
    TASK_DEFAULT_DISK("tajo.task.disk-slot.default", 0.5f),
    TASK_DEFAULT_SIZE("tajo.task.size-mb", 128),

    // Query and Optimization -------------------------------------------------
    EXECUTOR_EXTERNAL_SORT_THREAD_NUM("tajo.executor.external-sort.thread-num", 1),
    EXECUTOR_EXTERNAL_SORT_FANOUT("tajo.executor.external-sort.fanout-num", 8),

    EXECUTOR_INNER_JOIN_INMEMORY_HASH_TABLE_SIZE("tajo.executor.join.inner.in-memory-table-num", (long)1000000),

    // Metrics ----------------------------------------------------------------
    METRICS_PROPERTY_FILENAME("tajo.metrics.property.file", "tajo-metrics.properties"),

    // Misc -------------------------------------------------------------------

    // Geo IP
    GEOIP_DATA("tajo.function.geoip-database-location", ""),

    /////////////////////////////////////////////////////////////////////////////////
    // User Session Configuration
    //
    // All session variables begin with dollor($) sign. They are default configs
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

    $DIST_QUERY_JOIN_PARTITION_VOLUME("tajo.dist-query.join.partition-volume-mb", 128),
    $DIST_QUERY_GROUPBY_PARTITION_VOLUME("tajo.dist-query.groupby.partition-volume-mb", 256),
    $DIST_QUERY_TABLE_PARTITION_VOLUME("tajo.dist-query.table-partition.task-volume-mb", 256),

    $GROUPBY_MULTI_LEVEL_ENABLED("tajo.dist-query.groupby.multi-level-aggr", true),

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
    $CODEGEN("tajo.executor.codegen.enabled", false), // Runtime code generation

    // Client -----------------------------------------------------------------
    $CLIENT_SESSION_EXPIRY_TIME("tajo.client.session.expiry-time-sec", 3600), // default time is one hour.

    // Command line interface and its behavior --------------------------------
    $CLI_MAX_COLUMN("tajo.cli.max_columns", 120),
    $CLI_NULL_CHAR("tajo.cli.nullchar", ""),
    $CLI_PRINT_PAUSE_NUM_RECORDS("tajo.cli.print.pause.num.records", 100),
    $CLI_PRINT_PAUSE("tajo.cli.print.pause", true),
    $CLI_PRINT_ERROR_TRACE("tajo.cli.print.error.trace", true),
    $CLI_OUTPUT_FORMATTER_CLASS("tajo.cli.output.formatter", "org.apache.tajo.cli.DefaultTajoCliOutputFormatter"),
    $CLI_ERROR_STOP("tajo.cli.error.stop", false),

    // Timezone & Date ----------------------------------------------------------
    $TIMEZONE("tajo.timezone", System.getProperty("user.timezone")),
    $DATE_ORDER("tajo.date.order", "YMD"),

    // FILE FORMAT
    $CSVFILE_NULL("tajo.csvfile.null", "\\\\N"),

    // Only for Debug and Testing
    $DEBUG_ENABLED("tajo.debug.enabled", false),
    $TEST_BROADCAST_JOIN_ENABLED("tajo.dist-query.join.auto-broadcast", true),
    $TEST_JOIN_OPT_ENABLED("tajo.test.plan.join-optimization.enabled", true),
    $TEST_FILTER_PUSHDOWN_ENABLED("tajo.test.plan.filter-pushdown.enabled", true),
    $TEST_MIN_TASK_NUM("tajo.test.min-task-num", -1),

    // Behavior Control ---------------------------------------------------------
    $BEHAVIOR_ARITHMETIC_ABORT("tajo.behavior.arithmetic-abort", false);
    ;

    public final String varname;
    public final String defaultVal;
    public final int defaultIntVal;
    public final long defaultLongVal;
    public final float defaultFloatVal;
    public final Class<?> valClass;
    public final boolean defaultBoolVal;

    private final VarType type;

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

  public static Path getStagingDir(TajoConf conf) throws IOException {
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
}
