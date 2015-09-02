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

package org.apache.tajo.storage.jdbc;

import com.google.common.collect.Lists;
import net.minidev.json.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.*;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.Fragment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * JDBC Tablespace
 */
public abstract class JdbcTablespace extends Tablespace {

  static final StorageProperty STORAGE_PROPERTY = new StorageProperty("rowstore", false, true, false, true);
  static final FormatProperty  FORMAT_PROPERTY = new FormatProperty(false, false, false);
  public static final String MAPPED_DATABASE_CONFIG_KEY = "mapped_database";

  private Connection conn;

  public JdbcTablespace(String name, URI uri, JSONObject config) {
    super(name, uri, config);
  }

  @Override
  protected void storageInit() throws IOException {
    try {
      this.conn = DriverManager.getConnection(uri.toASCIIString());
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getTableVolume(URI uri) throws UnsupportedException {
    throw new UnsupportedException();
  }

  @Override
  public URI getTableUri(String databaseName, String tableName) {
    return URI.create(getUri() + "&table=" + tableName);
  }

  @Override
  public List<Fragment> getSplits(String inputSourceId,
                                  TableDesc tableDesc,
                                  @Nullable EvalNode filterCondition) throws IOException {
    return Lists.newArrayList((Fragment)new JdbcFragment(inputSourceId, tableDesc.getUri().toASCIIString()));
  }

  @Override
  public StorageProperty getProperty() {
    return STORAGE_PROPERTY;
  }

  @Override
  public FormatProperty getFormatProperty(TableMeta meta) {
    return FORMAT_PROPERTY;
  }

  @Override
  public void close() {
  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext,
                                          TableDesc tableDesc,
                                          Schema inputSchema,
                                          SortSpec[] sortSpecs,
                                          TupleRange dataRange) throws IOException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public void prepareTable(LogicalNode node) throws IOException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public Path commitTable(OverridableConf queryContext, ExecutionBlockId finalEbId, LogicalPlan plan, Schema schema,
                          TableDesc tableDesc) throws IOException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public void rollbackTable(LogicalNode node) throws IOException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  public abstract MetadataProvider getMetadataProvider();

  @Override
  public abstract Scanner getScanner(TableMeta meta,
                            Schema schema,
                            Fragment fragment,
                            @Nullable Schema target) throws IOException;

  public DatabaseMetaData getDatabaseMetaData() {
    try {
      return conn.getMetaData();
    } catch (SQLException e) {
      throw new TajoInternalError(e);
    }
  }
}
