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

package org.apache.tajo.storage.pgsql;

import net.minidev.json.JSONObject;
import org.apache.tajo.catalog.MetadataProvider;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.NullScanner;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.jdbc.JdbcFragment;
import org.apache.tajo.storage.jdbc.JdbcTablespace;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;

/**
 * Postgresql Database Tablespace
 */
public class PgSQLTablespace extends JdbcTablespace {

  public PgSQLTablespace(String name, URI uri, JSONObject config) {
    super(name, uri, config);
  }

  public MetadataProvider getMetadataProvider() {
    return new PgSQLMetadataProvider(this, database);
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

    Scanner scanner;
    if (fragment.isEmpty()) {
      scanner = new NullScanner(conf, schema, meta, fragment);
    } else {
      scanner = new PgSQLJdbcScanner(getDatabaseMetaData(), connProperties, schema, meta, (JdbcFragment) fragment);
    }
    scanner.setTarget(target.toArray());
    return scanner;
  }
}
