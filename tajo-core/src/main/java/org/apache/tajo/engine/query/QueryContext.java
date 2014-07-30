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

package org.apache.tajo.engine.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ConfigKey;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.util.KeyValueSet;

import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;

/**
 * QueryContext provides a consolidated config system for a query instant.
 *
 * In Tajo, there are three configurable layers:
 * <ul>
 *   <li>
 *    <ul>System Config - it comes from Hadoop's Configuration class. by tajo-site, catalog-site,
 *    catalog-default and TajoConf.</ul>
 *    <ul>Session variables - they are instantly configured by users.
 *    Each client session has it own set of session variables.</ul>
 *    <ul>Query config - it is internally used for meta information of a query instance.</ul>
 *   </li>
 * </ul>
 *
 * System configs and session variables can set the same config in the same time. System configs are usually used to set
 * default configs, and session variables is user-specified configs. So, session variables can override system configs.
 *
 * QueryContent provides a query with a uniform way to access various configs without considering their priorities.
 */
public class QueryContext extends KeyValueSet {

  public static enum QueryVars implements ConfigKey {
    COMMAND_TYPE,
    STAGING_DIR(),
    OUTPUT_TABLE_NAME(),
    OUTPUT_TABLE_PATH(),
    OUTPUT_PARTITIONS(),
    OUTPUT_OVERWRITE(),
    OUTPUT_AS_DIRECTORY(),
    OUTPUT_PER_FILE_SIZE(),
    ;

    QueryVars() {
    }

    @Override
    public String keyname() {
      return QUERY_CONF_PREFIX + name();
    }

    @Override
    public ConfigType type() {
      return ConfigType.QUERY;
    }
  }

  private static final Log LOG = LogFactory.getLog(QueryContext.class);

  private TajoConf conf;

  public QueryContext(final TajoConf conf) {
    this.conf = conf;
  }

  public QueryContext(final TajoConf conf, Session session) {
    this.conf = conf;
    putAll(session.getAllVariables());
  }

  public QueryContext(final TajoConf conf, KeyValueSetProto proto) {
    super(proto);
    this.conf = conf;
  }

  public void setConf(TajoConf conf) {
    this.conf = conf;
  }

  public TajoConf getConf() {
    return conf;
  }

  public void setBool(ConfigKey key, boolean val) {
    setBool(key.keyname(), val);
  }

  public boolean getBool(ConfigKey key, Boolean defaultVal) {
    switch (key.type()) {
    case QUERY:
      return getBool(key.keyname());
    case SESSION:
      return getBool(key.keyname(), conf.getBoolVar(((SessionVars) key).getConfVars()));
    case SYSTEM:
      return conf.getBoolVar((TajoConf.ConfVars) key);
    default:
      return getBool(key.keyname(), defaultVal);
    }
  }

  public boolean getBool(ConfigKey key) {
    return getBool(key, null);
  }

  public int getInt(ConfigKey key, Integer defaultVal) {
    switch (key.type()) {
    case QUERY:
      return getInt(key.keyname());
    case SESSION:
      return getInt(key.keyname(), conf.getIntVar(((SessionVars) key).getConfVars()));
    case SYSTEM:
      return conf.getIntVar((TajoConf.ConfVars) key);
    default:
      return getInt(key.keyname(), defaultVal);
    }
  }

  public int getInt(ConfigKey key) {
    return getInt(key, null);
  }

  public long getLong(ConfigKey key, Long defaultVal) {
    switch (key.type()) {
    case QUERY:
      return getLong(key.keyname());
    case SESSION:
      return getLong(key.keyname(), conf.getLongVar(((SessionVars) key).getConfVars()));
    case SYSTEM:
      return conf.getLongVar((TajoConf.ConfVars) key);
    default:
      return getLong(key.keyname(), defaultVal);
    }
  }

  public long getLong(ConfigKey key) {
    return getLong(key, null);
  }

  public float getFloat(ConfigKey key, Float defaultVal) {
    switch (key.type()) {
    case QUERY:
      return getFloat(key.keyname());
    case SESSION:
      return getFloat(key.keyname(), conf.getFloatVar(((SessionVars) key).getConfVars()));
    case SYSTEM:
      return conf.getFloatVar((TajoConf.ConfVars) key);
    default:
      return getFloat(key.keyname(), defaultVal);
    }
  }

  public float getFloat(ConfigKey key) {
    return getLong(key, null);
  }

  public void put(ConfigKey key, String val) {
    set(key.keyname(), val);
  }

  public static String getSessionKey(String key) {
    return ConfigKey.SESSION_PREFIX + key;
  }

  public static String getQueryKey(String key) {
    return ConfigKey.QUERY_CONF_PREFIX + key;
  }

  public String get(ConfigKey key, String defaultVal) {
    switch (key.type()) {
    case QUERY:
      return get(key.keyname());
    case SESSION:
      return get(key.keyname(), conf.getVar(((SessionVars) key).getConfVars()));
    case SYSTEM:
      return conf.getVar((TajoConf.ConfVars) key);
    default:
      return get(key.keyname(), defaultVal);
    }
  }

  public String get(ConfigKey key) {
    return get(key, null);
  }

  //-----------------------------------------------------------------------------------------------
  // Query Config Specified Section
  //-----------------------------------------------------------------------------------------------

  public String getCurrentDatabase() {
    return get(SessionVars.CURRENT_DATABASE);
  }

  public void setUser(String username) {
    put(SessionVars.USERNAME, username);
  }

  public String getUser() {
    return get(SessionVars.USERNAME);
  }

  public void setStagingDir(Path path) {
    put(QueryVars.STAGING_DIR, path.toUri().toString());
  }

  public Path getStagingDir() {
    String strVal = get(QueryVars.STAGING_DIR);
    return strVal != null ? new Path(strVal) : null;
  }

  /**
   * Set a target table name
   *
   * @param tableName The target table name
   */
  public void setOutputTable(String tableName) {
    put(QueryVars.OUTPUT_TABLE_NAME, tableName);
  }

  /**
   * The fact that QueryContext has an output path means this query will write the output to a specific directory.
   * In other words, this query is 'CREATE TABLE' or 'INSERT (OVERWRITE) INTO (<table name>|LOCATION)' statement.
   *
   * @return
   */
  public boolean hasOutputPath() {
    return containsKey(QueryVars.OUTPUT_TABLE_PATH);
  }

  public void setOutputPath(Path path) {
    put(QueryVars.OUTPUT_TABLE_PATH, path.toUri().toString());
  }

  public Path getOutputPath() {
    String strVal = get(QueryVars.OUTPUT_TABLE_PATH);
    return strVal != null ? new Path(strVal) : null;
  }

  public boolean hasPartition() {
    return containsKey(QueryVars.OUTPUT_PARTITIONS);
  }

  public void setPartitionMethod(PartitionMethodDesc partitionMethodDesc) {
    put(QueryVars.OUTPUT_PARTITIONS, partitionMethodDesc != null ? partitionMethodDesc.toJson() : null);
  }

  public PartitionMethodDesc getPartitionMethod() {
    return PartitionMethodDesc.fromJson(get(QueryVars.OUTPUT_PARTITIONS));
  }

  public void setOutputOverwrite() {
    setBool(QueryVars.OUTPUT_OVERWRITE, true);
  }

  public boolean isOutputOverwrite() {
    return getBool(QueryVars.OUTPUT_OVERWRITE);
  }

  public void setFileOutput() {
    setBool(QueryVars.OUTPUT_AS_DIRECTORY, true);
  }

  public boolean containsKey(ConfigKey key) {
    return containsKey(key.keyname());
  }

  public boolean equalKey(ConfigKey key, String another) {
    if (containsKey(key)) {
      return get(key).equals(another);
    } else {
      return false;
    }
  }

  public boolean isCommandType(NodeType commandType) {
    return equalKey(QueryVars.COMMAND_TYPE, commandType.name());
  }

  public void setCommandType(NodeType nodeType) {
    put(QueryVars.COMMAND_TYPE, nodeType.name());
  }

  public NodeType getCommandType() {
    String strVal = get(QueryVars.COMMAND_TYPE);
    return strVal != null ? NodeType.valueOf(strVal) : null;
  }

  public void setCreateTable() {
    setCommandType(NodeType.CREATE_TABLE);
  }

  public boolean isCreateTable() {
    return isCommandType(NodeType.CREATE_TABLE);
  }

  public void setInsert() {
    setCommandType(NodeType.INSERT);
  }

  public boolean isInsert() {
    return isCommandType(NodeType.INSERT);
  }
}
