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

package tajo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import tajo.conf.TajoConf;

public class QueryConf extends TajoConf {
  public static String FILENAME = "queryconf.xml";

  public QueryConf() {
    super();
  }

  public QueryConf(Configuration conf) {
    super(conf);
    if (! (conf instanceof QueryConf)) {
      this.reloadConfiguration();
    }
  }

  public void setUser(String username) {
    setVar(ConfVars.QUERY_USERNAME, username);
  }

  public String getUser() {
    return getVar(ConfVars.QUERY_USERNAME);
  }

  public void setOutputTable(String tableName) {
    // it is determined in GlobalEngine.executeQuery().
    setVar(ConfVars.QUERY_OUTPUT_TABLE, tableName);
  }

  public String getOutputTable() {
    return getVar(ConfVars.QUERY_OUTPUT_TABLE);
  }

  public void setOutputPath(Path path) {
    // it is determined in QueryMaster.initStagingDir().
    setVar(ConfVars.QUERY_OUTPUT_DIR, path.toUri().toString());
  }

  public Path getOutputPath() {
    return new Path(getVar(ConfVars.QUERY_OUTPUT_DIR));
  }
}
