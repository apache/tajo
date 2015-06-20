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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.ConfigKey;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.QueryVars;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.session.Session;

import java.net.URI;

import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;

/**
 * QueryContent is a overridable config, and it provides a set of various configs for a query instance.
 */
public class QueryContext extends OverridableConf {
  public QueryContext(TajoConf conf) {
    super(conf, ConfigKey.ConfigType.QUERY);
  }

  public QueryContext(TajoConf conf, Session session) {
    super(conf, ConfigKey.ConfigType.QUERY, ConfigKey.ConfigType.SESSION);
    putAll(session.getAllVariables());
  }

  public QueryContext(TajoConf conf, KeyValueSetProto proto) {
    super(conf, proto, ConfigKey.ConfigType.QUERY, ConfigKey.ConfigType.SESSION);
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

  /**
   * Set the default tablespace uri
   *
   * @param uri The default tablespace uri
   */
  public void setDefaultSpaceUri(URI uri) {
    put(QueryVars.DEFAULT_SPACE_URI, uri.toString());
  }

  /**
   * Return the default tablespace uri
   */
  public URI getDefaultSpaceUri() {
    String strVal = get(QueryVars.DEFAULT_SPACE_URI, "");
    return strVal != null && !strVal.isEmpty() ? URI.create(strVal) : null;
  }

  /**
   * Set the root uri of the default tablespace
   *
   * @param uri The root uri of the default tablespace
   */
  public void setDefaultSpaceRootUri(URI uri) {
    put(QueryVars.DEFAULT_SPACE_ROOT_URI, uri.toString());
  }

  /**
   * Return the root of the default tablespace
   */
  public URI getDefaultSpaceRootUri() {
    String strVal = get(QueryVars.DEFAULT_SPACE_ROOT_URI, "");
    return strVal != null && !strVal.isEmpty() ? URI.create(strVal) : null;
  }

  public void setStagingDir(URI uri) {
    put(QueryVars.STAGING_DIR, uri.toString());
  }

  public Path getStagingDir() {
    String strVal = get(QueryVars.STAGING_DIR, "");
    return strVal != null && !strVal.isEmpty() ? new Path(strVal) : null;
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
   * The final output table's uri. It will be set if a query is CTAS or INSERT (OVERWRITE) INTO statement
   *
   * @return True if a output table uri is set. Otherwise, it will return false
   */
  public boolean hasOutputTableUri() {
    return containsKey(QueryVars.OUTPUT_TABLE_URI);
  }

  /**
   * Set the final output table uri
   *
   * @param uri
   */
  public void setOutputPath(URI uri) {
    if (uri != null) {
      put(QueryVars.OUTPUT_TABLE_URI, uri.toString());
    }
  }

  /**
   * Get the final output table uri
   *
   * @return The final output table uri
   */
  public URI getOutputTableUri() {
    String strVal = get(QueryVars.OUTPUT_TABLE_URI);
    return strVal != null ? URI.create(strVal) : null;
  }

  public boolean hasPartition() {
    return containsKey(QueryVars.OUTPUT_PARTITIONS);
  }

  public void setPartitionMethod(PartitionMethodDesc partitionMethodDesc) {
    put(QueryVars.OUTPUT_PARTITIONS, partitionMethodDesc != null ? partitionMethodDesc.toJson() : null);
  }

//  public PartitionMethodDesc getPartitionMethod() {
//    return PartitionMethodDesc.fromJson(get(QueryVars.OUTPUT_PARTITIONS));
//  }

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

  public boolean isCommandType(String commandType) {
    return equalKey(QueryVars.COMMAND_TYPE, commandType);
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
    return isCommandType(NodeType.CREATE_TABLE.name());
  }

  public void setInsert() {
    setCommandType(NodeType.INSERT);
  }

  public boolean isInsert() {
    return isCommandType(NodeType.INSERT.name());
  }
}
