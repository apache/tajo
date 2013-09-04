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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.InsertNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.logical.CreateTableNode;

import static org.apache.tajo.catalog.proto.CatalogProtos.KeyValueSetProto;

public class QueryMeta extends Options {

  public QueryMeta() {}

  public QueryMeta(KeyValueSetProto proto) {
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

  public void setUser(String username) {
    put(TajoConf.ConfVars.QUERY_USERNAME, username);
  }

  public String getUser() {
    return get(TajoConf.ConfVars.QUERY_USERNAME);
  }

  public void setOutputTable(String tableName) {
    put(TajoConf.ConfVars.QUERY_OUTPUT_TABLE, PlannerUtil.normalizeTableName(tableName));
  }

  public String getOutputTable() {
    return get(TajoConf.ConfVars.QUERY_OUTPUT_TABLE);
  }

  public void setOutputPath(Path path) {
    // it is determined in QueryMaster.initStagingDir().
    put(TajoConf.ConfVars.QUERY_OUTPUT_DIR, path.toUri().toString());
  }

  public Path getOutputPath() {
    return new Path(get(TajoConf.ConfVars.QUERY_OUTPUT_DIR));
  }

  public void setOutputOverwrite() {
    put("tajo.output.overwrite", "1");
  }

  public boolean isOutputOverwrite() {
    String overwrite = get("tajo.output.overwrite");
    if (overwrite != null && overwrite.equals("1")) {
      return true;
    } else {
      return false;
    }
  }

  public void setFileOutput() {
    put("tajo.output.fileoutput", "1");
  }

  public boolean isFileOutput() {
    String fileoutput = get("tajo.output.fileoutput");
    if (fileoutput != null && fileoutput.equals("1")) {
      return true;
    } else {
      return false;
    }
  }

  public void setCreateTable() {
    put("tajo.query.command", CreateTableNode.class.getSimpleName());
  }

  public boolean isCreateTable() {
    String command = get("tajo.query.command");
    return command != null && command.equals(CreateTableNode.class.getSimpleName());
  }

  public void setInsert() {
    put("tajo.query.command", InsertNode.class.getSimpleName());
  }

  public boolean isInsert() {
    String command = get("tajo.query.command");
    return command != null && command.equals(InsertNode.class.getSimpleName());
  }
}
