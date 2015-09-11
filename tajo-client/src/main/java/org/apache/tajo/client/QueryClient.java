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

import org.apache.tajo.QueryId;
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.exception.NoSuchSessionVariableException;
import org.apache.tajo.exception.QueryNotFoundException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.GetQueryResultResponse;
import org.apache.tajo.ipc.ClientProtos.QueryHistoryProto;
import org.apache.tajo.ipc.ClientProtos.QueryInfoProto;
import org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse;
import org.apache.tajo.jdbc.TajoMemoryResultSet;

import java.io.Closeable;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public interface QueryClient extends Closeable {

  boolean isConnected();

  String getSessionId();

  Map<String, String> getClientSideSessionVars();

  String getBaseDatabase();
  
  void setMaxRows(int maxRows);
  
  int getMaxRows();

  @Override
  void close();

  UserRoleInfo getUserInfo();

  /**
   * Call to QueryMaster closing query resources
   * @param queryId
   */
  void closeQuery(final QueryId queryId);

  void closeNonForwardQuery(final QueryId queryId);

  String getCurrentDatabase();

  void selectDatabase(final String databaseName) throws UndefinedDatabaseException;

  Map<String, String> updateSessionVariables(final Map<String, String> variables);

  Map<String, String> unsetSessionVariables(final List<String> variables);

  String getSessionVariable(final String varname) throws NoSuchSessionVariableException;

  boolean existSessionVariable(final String varname);

  Map<String, String> getAllSessionVariables();

  /**
   * It submits a query statement and get a response immediately.
   * The response only contains a query id, and submission status.
   * In order to get the result, you should use {@link #getQueryResult(org.apache.tajo.QueryId)}.
   */
  SubmitQueryResponse executeQuery(final String sql);

  SubmitQueryResponse executeQueryWithJson(final String json);

  /**
   * It submits a query statement and get a response.
   * The main difference from {@link #executeQuery(String)}
   * is a blocking method. So, this method is wait for
   * the finish of the submitted query.
   *
   * @return If failed, return null.
   */
  ResultSet executeQueryAndGetResult(final String sql) throws TajoException;

  ResultSet executeJsonQueryAndGetResult(final String json) throws TajoException;

  QueryStatus getQueryStatus(QueryId queryId) throws QueryNotFoundException;

  ResultSet getQueryResult(QueryId queryId) throws TajoException;

  ResultSet createNullResultSet(QueryId queryId);

  GetQueryResultResponse getResultResponse(QueryId queryId) throws TajoException;

  Future<TajoMemoryResultSet> fetchNextQueryResultAsync(final QueryId queryId, final int fetchRowNum);

  boolean updateQuery(final String sql) throws TajoException;

  boolean updateQueryWithJson(final String json) throws TajoException;

  List<ClientProtos.BriefQueryInfo> getRunningQueryList();

  List<ClientProtos.BriefQueryInfo> getFinishedQueryList();

  List<ClientProtos.WorkerResourceInfo> getClusterInfo();

  QueryStatus killQuery(final QueryId queryId) throws QueryNotFoundException;

  QueryInfoProto getQueryInfo(final QueryId queryId) throws QueryNotFoundException;

  QueryHistoryProto getQueryHistory(final QueryId queryId) throws QueryNotFoundException;
}
