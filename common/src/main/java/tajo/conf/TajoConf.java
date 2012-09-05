/*
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

package tajo.conf;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.PrintStream;
import java.util.Map;

public class TajoConf extends Configuration {
  private static final Log LOG = LogFactory.getLog(TajoConf.class);

  static{
    Configuration.addDefaultResource("tajo-default.xml");
    Configuration.addDefaultResource("tajo-site.xml");
    Configuration.addDefaultResource("yarn-default.xml");
    Configuration.addDefaultResource("yarn-site.xml");
  }

  private static final Map<String, ConfVars> vars = Maps.newHashMap();

  public TajoConf() {
    super();
  }

  public TajoConf(Configuration conf) {
    super(conf);
  }

  public static enum ConfVars {
    //////////////////////////////////
    // System Configuration
    //////////////////////////////////
    CLUSTER_DISTRIBUTED("tajo.cluster.distributed", false),
    MASTER_ADDRESS("engine.master.addr", "localhost:9001"),
    CATALOG_ADDRESS("catalog.master.addr", "0.0.0.0:9002"),
    LEAFSERVER_PORT("engine.leafserver.port", 9003),
    CLIENT_SERVICE_ADDRESS("tajo.clientservice.addr", "localhost:9004"),
    CLIENT_SERVICE_PORT("tajo.clientservice.port", 9004),

    // System Configuration - Zookeeper Section
    ZOOKEEPER_ADDRESS("zookeeper.server.addr", "localhost:2181"),
    ZOOKEEPER_TICK_TIME("zookeeper.server.ticktime", 5000),
    ZOOKEEPER_SESSION_TIMEOUT("zookeeper.session.timeout", 180*1000),
    // the two constants are only used for local zookeepers
    ZOOKEEPER_DATA_DIR("zookeeper.server.datadir", ""),
    ZOOKEEPER_LOG_DIR("zookeeper.server.logdir", ""),
    ZOOKEEPER_RETRY_COUNT("zookeeper.retry.count", 3),
    ZOOKEEPER_RETRY_INTERVALMILLS("zookeeper.retry.intervalmills", 1000),

    // directory
    ENGINE_BASE_DIR("engine.rootdir", ""),
    ENGINE_DATA_DIR("engine.data.dir", ""),
    WORKER_BASE_DIR("tajo.worker.basedir", ""),
    WORKER_TMP_DIR("tajo.worker.tmpdir", ""),


    //////////////////////////////////
    // Storage Configuration
    //////////////////////////////////
    RAWFILE_SYNC_INTERVAL("rawfile.sync.interval", null),

    // for RCFile
    HIVEUSEEXPLICITRCFILEHEADER("tajo.exec.rcfile.use.explicit.header", true),

    //////////////////////////////////
    // Physical Execution Configuration
    //////////////////////////////////
    EXTERNAL_SORT_BUFFER("tajo.extsort.buffer", 400000),
    BROADCAST_JOIN_THRESHOLD("tajo.join.broadcast.threshold", (long)5 * 1048576),

    //////////////////////////////////
    // The Below is reserved
    //////////////////////////////////
    WORKING_DIR("tajo.query.workingdir", null),

    // Global Key Section
    WAREHOUSE_PATH("tajo.warehouse.dir", "/tajo/warehouse"),

    // Query Master
    QUERY_AM_VMEM_MB("tajo.query.am.vmem", 4096),
    QUERY_AM_JAVA_OPT("tajo.query.am.javaopt", "-Xmx1024m"),

    // Query
    QUERY_NAME("tajo.query.name", "tajo query"),
    QUEUE_NAME("tajo.query.queue.name", "default"),
    USERNAME("tajo.query.user.name", ""),
    QUERY_TMP_DIR("tajo.query.tmpdir", ""),
    PLAN("tajo.exec.plan", ""),
    QUERY_OUTPUT_DIR_SUCCESSFUL_MARKER("tajo.query.output.successfulmaker",
        true),
    QUERY_OUTPUT_DIR("tajo.query.output.dir", ""),

    // Task
    LOCAL_TMP_DIR("tajo.task.local.tmpdir", null),

    // TaskAttempt
    APPLICATION_ATTEMPT_ID("tajo.app.attempt.id", (int)0),
    TASK_ATTEMPT_ID("tajo.task.attempt.id", ""),
    ATTEMPT_ID("tajo.query.attempt.id", ""),
    FINAL_SUBQUERY_ID("tajo.query.finalsubquery.id", ""),

    // Query Execution Section
    SORT_BUFFER_SIZE("tajo.sort.mb", (int)1),
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
}
