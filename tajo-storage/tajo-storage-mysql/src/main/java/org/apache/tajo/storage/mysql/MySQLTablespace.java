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

package org.apache.tajo.storage.mysql;

import net.minidev.json.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.*;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.FormatProperty;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.jdbc.*;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * <h3>URI Examples:</h3>
 * <ul>
 *   <li>jdbc:mysql//primaryhost,secondaryhost1,secondaryhost2/test?profileSQL=true</li>
 * </ul>
 */
public class MySQLTablespace extends JdbcTablespace {

  public MySQLTablespace(String name, URI uri, JSONObject config) {
    super(name, uri, config);
  }

  public MetadataProvider getMetadataProvider() {
    return new MySQLMetadataProvider(this, "db1");
  }
}
