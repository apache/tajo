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
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.logical.NodeType;

import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;

public class QueryContext extends KeyValueSet {
  private static final Log LOG = LogFactory.getLog(QueryContext.class);

  public static final String COMMAND_TYPE = "tajo.query.command";

  public static final String STAGING_DIR = "tajo.query.staging_dir";

  public static final String USER_NAME = "tajo.query.username";

  public static final String OUTPUT_TABLE_NAME = "tajo.query.output.table";
  public static final String OUTPUT_TABLE_PATH = "tajo.query.output.path";
  public static final String OUTPUT_PARTITIONS = "tajo.query.output.partitions";
  public static final String OUTPUT_OVERWRITE = "tajo.query.output.overwrite";
  public static final String OUTPUT_AS_DIRECTORY = "tajo.query.output.asdirectory";
  public static final String OUTPUT_PER_FILE_SIZE = "tajo.query.output.perfile-size-bytes";

  public static final String TRUE_VALUE = "true";
  public static final String FALSE_VALUE = "false";

  public QueryContext() {}

  public QueryContext(KeyValueSetProto proto) {
    super(proto);
  }

  public void put(TajoConf.ConfVars key, String value) {
    put(key.varname, value);
  }

  public String get(TajoConf.ConfVars key) {
    return get(key.varname);
  }

  public String get(String key) {
    return super.get(key);
  }

  public void setBool(String key, boolean val) {
    put(key, val ? TRUE_VALUE : FALSE_VALUE);
  }

  public boolean getBool(String key) {
    String strVal = get(key);
    return strVal != null ? strVal.equalsIgnoreCase(TRUE_VALUE) : false;
  }

  public void setUser(String username) {
    put(USER_NAME, username);
  }

  public String getUser() {
    return get(USER_NAME);
  }

  public void setStagingDir(Path path) {
    put(STAGING_DIR, path.toUri().toString());
  }

  public Path getStagingDir() {
    String strVal = get(STAGING_DIR);
    return strVal != null ? new Path(strVal) : null;
  }

  /**
   * The fact that QueryContext has an output table means this query has a target table.
   * In other words, this query is 'CREATE TABLE' or 'INSERT (OVERWRITE) INTO <table name>' statement.
   * This config is not set if a query has INSERT (OVERWRITE) INTO LOCATION '/path/..'.
   */
  public boolean hasOutputTable() {
    return get(OUTPUT_TABLE_NAME) != null;
  }

  /**
   * Set a target table name
   *
   * @param tableName The target table name
   */
  public void setOutputTable(String tableName) {
    put(OUTPUT_TABLE_NAME, tableName);
  }

  public String getOutputTable() {
    String strVal = get(OUTPUT_TABLE_NAME);
    return strVal != null ? strVal : null;
  }

  /**
   * The fact that QueryContext has an output path means this query will write the output to a specific directory.
   * In other words, this query is 'CREATE TABLE' or 'INSERT (OVERWRITE) INTO (<table name>|LOCATION)' statement.
   *
   * @return
   */
  public boolean hasOutputPath() {
    return get(OUTPUT_TABLE_PATH) != null;
  }

  public void setOutputPath(Path path) {
    put(OUTPUT_TABLE_PATH, path.toUri().toString());
  }

  public Path getOutputPath() {
    String strVal = get(OUTPUT_TABLE_PATH);
    return strVal != null ? new Path(strVal) : null;
  }

  public boolean hasPartition() {
    return get(OUTPUT_PARTITIONS) != null;
  }

  public void setPartitionMethod(PartitionMethodDesc partitionMethodDesc) {
    put(OUTPUT_PARTITIONS, partitionMethodDesc != null ? partitionMethodDesc.toJson() : null);
  }

  public PartitionMethodDesc getPartitionMethod() {
    return PartitionMethodDesc.fromJson(get(OUTPUT_PARTITIONS));
  }

  public void setOutputOverwrite() {
    setBool(OUTPUT_OVERWRITE, true);
  }

  public boolean isOutputOverwrite() {
    return getBool(OUTPUT_OVERWRITE);
  }

  public void setFileOutput() {
    setBool(OUTPUT_AS_DIRECTORY, true);
  }

  public boolean isFileOutput() {
    return getBool(OUTPUT_AS_DIRECTORY);
  }

  public void setCommandType(NodeType nodeType) {
    put(COMMAND_TYPE, nodeType.name());
  }

  public NodeType getCommandType() {
    String strVal = get(COMMAND_TYPE);
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

  public void setHiveQueryMode() {
    setBool("hive.query.mode", true);
  }

  public boolean isHiveQueryMode() {
    return getBool("hive.query.mode");
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
