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

package org.apache.tajo.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.util.KeyValueSet;

import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@ThreadSafe
public class TajoClientImpl extends SessionConnection implements TajoClient, QueryClient, CatalogAdminClient {

  private final Log LOG = LogFactory.getLog(TajoClientImpl.class);
  QueryClient queryClient;
  CatalogAdminClient catalogClient;

  /**
   * Connect to TajoMaster
   *
   * @param tracker ServiceTracker to discovery Tajo Client RPC
   * @param baseDatabase The base database name. It is case sensitive. If it is null,
   *                     the 'default' database will be used.
   * @param properties configurations
   * @throws java.io.IOException
   */
  public TajoClientImpl(ServiceTracker tracker, @Nullable String baseDatabase, KeyValueSet properties)
      throws SQLException {

    super(tracker, baseDatabase, properties);

    this.queryClient = new QueryClientImpl(this);
    this.catalogClient = new CatalogAdminClientImpl(this);
  }

  /**
   * Connect to TajoMaster
   *
   * @param addr TajoMaster address
   * @param baseDatabase The base database name. It is case sensitive. If it is null,
   *                     the 'default' database will be used.
   * @param properties configurations
   * @throws java.io.IOException
   */
  public TajoClientImpl(InetSocketAddress addr, @Nullable String baseDatabase, KeyValueSet properties)
      throws SQLException {
    this(new DummyServiceTracker(addr), baseDatabase, properties);
  }

  public TajoClientImpl(ServiceTracker serviceTracker) throws SQLException {
    this(serviceTracker, null);
  }

  public TajoClientImpl(ServiceTracker serviceTracker, @Nullable String baseDatabase) throws SQLException {
    this(serviceTracker, baseDatabase, new KeyValueSet());
  }
  /*------------------------------------------------------------------------*/
  // QueryClient wrappers
  /*------------------------------------------------------------------------*/

  public void closeQuery(final QueryId queryId) throws SQLException {
    queryClient.closeQuery(queryId);
  }

  public void closeNonForwardQuery(final QueryId queryId) throws SQLException {
    queryClient.closeNonForwardQuery(queryId);
  }

  public SubmitQueryResponse executeQuery(final String sql) throws SQLException {
    return queryClient.executeQuery(sql);
  }

  public SubmitQueryResponse executeQueryWithJson(final String json) throws SQLException {
    return queryClient.executeQueryWithJson(json);
  }

  public ResultSet executeQueryAndGetResult(final String sql) throws SQLException {
    return queryClient.executeQueryAndGetResult(sql);
  }

  public ResultSet executeJsonQueryAndGetResult(final String json) throws SQLException {
    return queryClient.executeJsonQueryAndGetResult(json);
  }

  public QueryStatus getQueryStatus(QueryId queryId) throws SQLException {
    return queryClient.getQueryStatus(queryId);
  }

  public ResultSet getQueryResult(QueryId queryId) throws SQLException {
    return queryClient.getQueryResult(queryId);
  }

  public ResultSet createNullResultSet(QueryId queryId) throws SQLException {
    return TajoClientUtil.createNullResultSet(queryId);
  }

  public GetQueryResultResponse getResultResponse(QueryId queryId) throws SQLException {
    return queryClient.getResultResponse(queryId);
  }

  public TajoMemoryResultSet fetchNextQueryResult(final QueryId queryId, final int fetchRowNum) throws SQLException {
    return queryClient.fetchNextQueryResult(queryId, fetchRowNum);
  }

  public boolean updateQuery(final String sql) throws SQLException {
    return queryClient.updateQuery(sql);
  }

  public boolean updateQueryWithJson(final String json) throws SQLException {
    return queryClient.updateQueryWithJson(json);
  }

  public QueryStatus killQuery(final QueryId queryId) throws SQLException {
    return queryClient.killQuery(queryId);
  }

  public List<BriefQueryInfo> getRunningQueryList() throws SQLException {
    return queryClient.getRunningQueryList();
  }

  public List<BriefQueryInfo> getFinishedQueryList() throws SQLException {
    return queryClient.getFinishedQueryList();
  }

  public List<WorkerResourceInfo> getClusterInfo() throws SQLException {
    return queryClient.getClusterInfo();
  }

  public QueryInfoProto getQueryInfo(final QueryId queryId) throws SQLException {
    return queryClient.getQueryInfo(queryId);
  }

  public QueryHistoryProto getQueryHistory(final QueryId queryId) throws SQLException {
    return queryClient.getQueryHistory(queryId);
  }

  public void setMaxRows(int maxRows) {
	  queryClient.setMaxRows(maxRows);
  }

  public int getMaxRows() {
	  return queryClient.getMaxRows();
  }
  
  /*------------------------------------------------------------------------*/
  // CatalogClient wrappers
  /*------------------------------------------------------------------------*/

  public boolean createDatabase(final String databaseName) throws SQLException {
    return catalogClient.createDatabase(databaseName);
  }

  public boolean existDatabase(final String databaseName) throws SQLException {
    return catalogClient.existDatabase(databaseName);
  }

  public boolean dropDatabase(final String databaseName) throws SQLException {
    return catalogClient.dropDatabase(databaseName);
  }

  public List<String> getAllDatabaseNames() throws SQLException {
    return catalogClient.getAllDatabaseNames();
  }

  public boolean existTable(final String tableName) throws SQLException {
    return catalogClient.existTable(tableName);
  }

  public TableDesc createExternalTable(final String tableName, final Schema schema, final URI path,
                                       final TableMeta meta) throws SQLException {
    return catalogClient.createExternalTable(tableName, schema, path, meta);
  }

  public TableDesc createExternalTable(final String tableName, final Schema schema, final URI path,
                                       final TableMeta meta, final PartitionMethodDesc partitionMethodDesc)
      throws SQLException {
    return catalogClient.createExternalTable(tableName, schema, path, meta, partitionMethodDesc);
  }

  public boolean dropTable(final String tableName) throws SQLException {
    return dropTable(tableName, false);
  }

  public boolean dropTable(final String tableName, final boolean purge) throws SQLException {
    return catalogClient.dropTable(tableName, purge);
  }

  public List<String> getTableList(@Nullable final String databaseName) throws SQLException {
    return catalogClient.getTableList(databaseName);
  }

  public TableDesc getTableDesc(final String tableName) throws SQLException {
    return catalogClient.getTableDesc(tableName);
  }

  public List<CatalogProtos.FunctionDescProto> getFunctions(final String functionName) throws SQLException {
    return catalogClient.getFunctions(functionName);
  }

  @Override
  public IndexDescProto getIndex(String indexName) throws SQLException {
    return catalogClient.getIndex(indexName);
  }

  @Override
  public boolean existIndex(String indexName) throws SQLException {
    return catalogClient.existIndex(indexName);
  }

  @Override
  public List<IndexDescProto> getIndexes(String tableName) throws SQLException {
    return catalogClient.getIndexes(tableName);
  }

  @Override
  public boolean hasIndexes(String tableName) throws SQLException {
    return catalogClient.hasIndexes(tableName);
  }

  @Override
  public IndexDescProto getIndex(String tableName, String[] columnNames) throws SQLException {
    return catalogClient.getIndex(tableName, columnNames);
  }

  @Override
  public boolean existIndex(String tableName, String[] columnName) throws SQLException {
    return catalogClient.existIndex(tableName, columnName);
  }

  @Override
  public boolean dropIndex(String indexName) throws SQLException {
    return catalogClient.dropIndex(indexName);
  }
}
