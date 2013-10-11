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
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tajo.TajoConstants;

import java.io.PrintStream;
import java.util.Map;

public class TajoConf extends YarnConfiguration {

  static {
    Configuration.addDefaultResource("catalog-default.xml");
    Configuration.addDefaultResource("catalog-site.xml");
    Configuration.addDefaultResource("storage-default.xml");
    Configuration.addDefaultResource("storage-site.xml");
    Configuration.addDefaultResource("tajo-default.xml");
    Configuration.addDefaultResource("tajo-site.xml");
  }

  private static final String EMPTY_VALUE = "";

  private static final Map<String, ConfVars> vars = Maps.newHashMap();

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

  public static enum ConfVars {
    //////////////////////////////////
    // Tajo System Configuration
    //////////////////////////////////

    // a username for a running Tajo cluster
    TAJO_USERNAME("tajo.cluster.username", "tajo"),

    // Configurable System Directories
    ROOT_DIR("tajo.rootdir", "/tajo"),
    WAREHOUSE_DIR("tajo.wahrehouse-dir", EMPTY_VALUE),
    STAGING_ROOT_DIR("tajo.staging.root.dir", ""),
    TASK_LOCAL_DIR("tajo.task.localdir", ""), // local directory for temporal files
    SYSTEM_CONF_PATH("tajo.system.conf.path", ""),
    SYSTEM_CONF_REPLICA_COUNT("tajo.system.conf.replica.count", 20),

    // Service Addresses
    TASKRUNNER_LISTENER_ADDRESS("tajo.master.taskrunnerlistener.addr", "0.0.0.0:0"),
    CLIENT_SERVICE_ADDRESS("tajo.master.clientservice.addr", "127.0.0.1:9004"),
    TAJO_MASTER_SERVICE_ADDRESS("tajo.master.manager.addr", "0.0.0.0:9005"),

    // Resource Manager
    RESOURCE_MANAGER_CLASS("tajo.resource.manager", "org.apache.tajo.master.rm.YarnTajoResourceManager"),

    // Catalog Address
    CATALOG_ADDRESS("tajo.catalog.master.addr", "0.0.0.0:9002"),

    //////////////////////////////////
    // Worker
    //////////////////////////////////
    /** how many launching TaskRunners in parallel */
    AM_TASKRUNNER_LAUNCH_PARALLEL_NUM("tajo.master.taskrunnerlauncher.parallel.num", 16),
    MAX_WORKER_PER_NODE("tajo.query.max-workernum.per.node", 8),

    //////////////////////////////////
    // Pull Server
    //////////////////////////////////
    PULLSERVER_PORT("tajo.pullserver.port", 0),
    SHUFFLE_SSL_ENABLED_KEY("tajo.pullserver.ssl.enabled", false),

    //////////////////////////////////
    // Storage Configuration
    //////////////////////////////////
    RAWFILE_SYNC_INTERVAL("rawfile.sync.interval", null),
    // for RCFile
    HIVEUSEEXPLICITRCFILEHEADER("tajo.exec.rcfile.use.explicit.header", true),


    //////////////////////////////////
    // Physical Executors
    //////////////////////////////////
    EXTENAL_SORT_BUFFER_NUM("tajo.sort.external.buffer", 1000000),
    BROADCAST_JOIN_THRESHOLD("tajo.join.broadcast.threshold", (long)5 * 1048576),
    INMEMORY_HASH_TABLE_DEFAULT_SIZE("tajo.join.inmemory.table.num", (long)1000000),
    INMEMORY_INNER_HASH_JOIN_THRESHOLD("tajo.join.inner.memhash.threshold", (long)256 * 1048576),
    INMEMORY_HASH_AGGREGATION_THRESHOLD("tajo.aggregation.hash.threshold", (long)256 * 1048576),
    INMEMORY_OUTER_HASH_AGGREGATION_THRESHOLD("tajo.join.outer.memhash.threshold", (long)256 * 1048576),

    //////////////////////////////////////////
    // Distributed Query Execution Parameters
    //////////////////////////////////////////
    JOIN_TASK_VOLUME("tajo.join.task-volume.mb", 128),
    SORT_TASK_VOLUME("tajo.sort.task-volume.mb", 128),
    AGGREGATION_TASK_VOLUME("tajo.task-aggregation.volume.mb", 128),

    JOIN_PARTITION_VOLUME("tajo.join.part-volume.mb", 128),
    SORT_PARTITION_VOLUME("tajo.sort.part-volume.mb", 256),
    AGGREGATION_PARTITION_VOLUME("tajo.aggregation.part-volume.mb", 256),

    //////////////////////////////////
    // The Below is reserved
    //////////////////////////////////

    // GeoIP
    GEOIP_DATA("tajo.geoip.data", "/usr/local/share/GeoIP/GeoIP.dat"),

    //////////////////////////////////
    // Hive Configuration
    //////////////////////////////////
    HIVE_QUERY_MODE("tajo.hive.query.mode", false),
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
      INT { void checkType(String value) throws Exception { Integer
          .valueOf(value); } },
      LONG { void checkType(String value) throws Exception { Long.valueOf(value); } },
      FLOAT { void checkType(String value) throws Exception { Float
          .valueOf(value); } },
      BOOLEAN { void checkType(String value) throws Exception { Boolean
          .valueOf(value); } };

      boolean isType(String value) {
        try { checkType(value); } catch (Exception e) { return false; }
        return true;
      }
      String typeString() { return name().toUpperCase();}
      abstract void checkType(String value) throws Exception;
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
    assert (var.valClass == Long.class);
    return conf.getLong(var.varname, var.defaultLongVal);
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
    assert (var.valClass == String.class);
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

  /////////////////////////////////////////////////////////////////////////////
  // Tajo System Specific Methods
  /////////////////////////////////////////////////////////////////////////////

  public static Path getTajoRootPath(TajoConf conf) {
    String rootPath = conf.getVar(ConfVars.ROOT_DIR);
    Preconditions.checkNotNull(rootPath,
          ConfVars.ROOT_DIR.varname + " must be set before a Tajo Cluster starts up");
    return new Path(rootPath);
  }

  public static Path getWarehousePath(TajoConf conf) {
    String warehousePath = conf.getVar(ConfVars.WAREHOUSE_DIR);
    if (warehousePath == null || warehousePath.equals("")) {
      Path rootDir = getTajoRootPath(conf);
      warehousePath = new Path(rootDir, TajoConstants.WAREHOUSE_DIR_NAME).toString();
      conf.setVar(ConfVars.WAREHOUSE_DIR, warehousePath);
      return new Path(warehousePath);
    } else {
      return new Path(warehousePath);
    }
  }

  public static Path getSystemPath(TajoConf conf) {
    Path rootPath = getTajoRootPath(conf);
    return new Path(rootPath, TajoConstants.SYSTEM_DIR_NAME);
  }

  public static Path getSystemResourcePath(TajoConf conf) {
    return new Path(getSystemPath(conf), TajoConstants.SYSTEM_RESOURCE_DIR_NAME);
  }

  public static Path getStagingRoot(TajoConf conf) {
    String stagingRootDir = conf.getVar(ConfVars.STAGING_ROOT_DIR);
    Preconditions.checkState(stagingRootDir != null && !stagingRootDir.equals(""),
        TajoConstants.STAGING_DIR_NAME + " must be set before starting a Tajo Cluster starts up");
    return new Path(stagingRootDir);
  }

  public static Path getSystemConf(TajoConf conf) {
    String stagingRootDir = conf.getVar(ConfVars.SYSTEM_CONF_PATH);
    Preconditions.checkNotNull(stagingRootDir, ConfVars.SYSTEM_CONF_PATH.varname + " is not set.");
    return new Path(stagingRootDir);
  }
}
