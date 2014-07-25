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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.InstantConfig;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.util.KeyValueSet;

import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;

public class QueryContext extends KeyValueSet {

  public static enum QueryVars implements InstantConfig {
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

    public String key() {
      return PREFIX + name();
    }
  }

  private static final Log LOG = LogFactory.getLog(QueryContext.class);

  public static final String TRUE_VALUE = "true";
  public static final String FALSE_VALUE = "false";

  private final TajoConf conf;

  public QueryContext() {
    this(new TajoConf());
  }

  public QueryContext(final TajoConf conf) {
    this.conf = conf;
  }

  public QueryContext(final TajoConf conf, KeyValueSetProto proto) {
    super(proto);
    this.conf = conf;
  }

  public TajoConf getConf() {
    return conf;
  }

  public void put(QueryVars key, String val) {
    put(key.key(), val);
  }

  public void put(TajoConf.ConfVars key, String value) {
    put(key.varname, value);
  }

  public String get(TajoConf.ConfVars key) {
    return get(key.varname);
  }

  public String get(SessionVars key) {
    return get(key.name());
  }

  public String get(QueryVars key) {
    return get(key.key());
  }

  public String get(String key) {
    return super.get(key);
  }

  public void setBool(QueryVars key, boolean val) {
    put(key.key(), val ? TRUE_VALUE : FALSE_VALUE);
  }

  public void setBool(String key, boolean val) {
    put(key, val ? TRUE_VALUE : FALSE_VALUE);
  }

  public boolean getBool(QueryVars key) {
    String strVal = get(key);
    return strVal != null ? strVal.equalsIgnoreCase(TRUE_VALUE) : false;
  }

  public boolean getBool(String key) {
    String strVal = get(key);
    return strVal != null ? strVal.equalsIgnoreCase(TRUE_VALUE) : false;
  }

  public void setUser(String username) {
    put(SessionVars.USER_NAME.getConfVars(), username);
  }

  public String getUser() {
    return get(SessionVars.USER_NAME);
  }

  public void setStagingDir(Path path) {
    put(QueryVars.STAGING_DIR, path.toUri().toString());
  }

  public Path getStagingDir() {
    String strVal = get(QueryVars.STAGING_DIR);
    return strVal != null ? new Path(strVal) : null;
  }

  /**
   * The fact that QueryContext has an output table means this query has a target table.
   * In other words, this query is 'CREATE TABLE' or 'INSERT (OVERWRITE) INTO <table name>' statement.
   * This config is not set if a query has INSERT (OVERWRITE) INTO LOCATION '/path/..'.
   */
  public boolean hasOutputTable() {
    return get(QueryVars.OUTPUT_TABLE_NAME) != null;
  }

  /**
   * Set a target table name
   *
   * @param tableName The target table name
   */
  public void setOutputTable(String tableName) {
    put(QueryVars.OUTPUT_TABLE_NAME, tableName);
  }

  public String getOutputTable() {
    String strVal = get(QueryVars.OUTPUT_TABLE_NAME);
    return strVal != null ? strVal : null;
  }

  /**
   * The fact that QueryContext has an output path means this query will write the output to a specific directory.
   * In other words, this query is 'CREATE TABLE' or 'INSERT (OVERWRITE) INTO (<table name>|LOCATION)' statement.
   *
   * @return
   */
  public boolean hasOutputPath() {
    return get(QueryVars.OUTPUT_TABLE_PATH) != null;
  }

  public void setOutputPath(Path path) {
    put(QueryVars.OUTPUT_TABLE_PATH, path.toUri().toString());
  }

  public Path getOutputPath() {
    String strVal = get(QueryVars.OUTPUT_TABLE_PATH);
    return strVal != null ? new Path(strVal) : null;
  }

  public boolean hasPartition() {
    return get(QueryVars.OUTPUT_PARTITIONS) != null;
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
    return getCommandType() == NodeType.CREATE_TABLE;
  }

  public void setInsert() {
    setCommandType(NodeType.INSERT);
  }

  public boolean isInsert() {
    return getCommandType() == NodeType.INSERT;
  }

  public static boolean getBoolVar(QueryContext context, TajoConf conf, TajoConf.ConfVars key) {
    if (context.get(key.varname) != null) {
      return context.getBool(key.varname);
    } else {
      return conf.getBoolVar(key);
    }
  }

  public static Integer getIntVar(QueryContext context, TajoConf conf, TajoConf.ConfVars key) {
    if (context.get(key.varname) != null) {
      String val = context.get(key.varname);
      try {
        return Integer.valueOf(val);
      } catch (NumberFormatException nfe) {
        LOG.warn(nfe.getMessage());
        return conf.getIntVar(key);
      }
    } else {
      return conf.getIntVar(key);
    }
  }

  public static Long getLongVar(QueryContext context, TajoConf conf, TajoConf.ConfVars key) {
    if (context.get(key.varname) != null) {
      String val = context.get(key.varname);
      try {
        return Long.valueOf(val);
      } catch (NumberFormatException nfe) {
        LOG.warn(nfe.getMessage());
        return conf.getLongVar(key);
      }
    } else {
      return conf.getLongVar(key);
    }
  }
}
