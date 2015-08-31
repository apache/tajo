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

import com.google.common.base.Preconditions;
import net.minidev.json.JSONObject;
import org.apache.tajo.catalog.*;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.jdbc.JdbcFragment;
import org.apache.tajo.storage.jdbc.JdbcTablespace;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;

/**
 * <h3>URI Examples:</h3>
 * <ul>
 *   <li>jdbc:mysql//primaryhost,secondaryhost1,secondaryhost2/test?profileSQL=true</li>
 * </ul>
 */
public class MySQLTablespace extends JdbcTablespace {
  private final String database;

  public MySQLTablespace(String name, URI uri, JSONObject config) {
    super(name, uri, config);
    database = ((JSONObject)config.get(TablespaceManager.TABLESPACE_SPEC_CONFIGS_KEY)).getAsString("database");
  }

  public MetadataProvider getMetadataProvider() {
    return new MySQLMetadataProvider(this, database);
  }

  @Override
  public Scanner getScanner(TableMeta meta,
                            Schema schema,
                            Fragment fragment,
                            @Nullable Schema target) throws IOException {
    if (!(fragment instanceof JdbcFragment)) {
      throw new TajoInternalError("fragment must be JdbcFragment");
    }

    if (target == null) {
      target = schema;
    }

    if (fragment.isEmpty()) {
      Scanner scanner = new NullScanner(conf, schema, meta, fragment);
      scanner.setTarget(target.toArray());

      return scanner;
    }

    return new MySQLJdbcScanner(getDatabaseMetaData(), schema, meta, (JdbcFragment) fragment);
  }
}
