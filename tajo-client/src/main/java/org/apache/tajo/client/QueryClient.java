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

import com.google.protobuf.ServiceException;
import org.apache.tajo.QueryId;
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.QueryHistoryProto;
import org.apache.tajo.ipc.ClientProtos.QueryInfoProto;
import org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse;
import org.apache.tajo.jdbc.TajoMemoryResultSet;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.TajoIdProtos.SessionIdProto;

public interface QueryClient extends Closeable {

  boolean isConnected();

  SessionIdProto getSessionId();

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

  String getCurrentDatabase() throws ServiceException;

  Boolean selectDatabase(final String databaseName) throws ServiceException;

  Map<String, String> updateSessionVariables(final Map<String, String> variables) throws ServiceException;

  Map<String, String> unsetSessionVariables(final List<String> variables) throws ServiceException;

  String getSessionVariable(final String varname) throws ServiceException;

  Boolean existSessionVariable(final String varname) throws ServiceException;

  Map<String, String> getAllSessionVariables() throws ServiceException;

  /**
   * It submits a query statement and get a response immediately.
   * The response only contains a query id, and submission status.
   * In order to get the result, you should use {@link #getQueryResult(org.apache.tajo.QueryId)}.
   */
  SubmitQueryResponse executeQuery(final String sql) throws ServiceException;

  SubmitQueryResponse executeQueryWithJson(final String json) throws ServiceException;

  /**
   * It submits a query statement and get a response.
   * The main difference from {@link #executeQuery(String)}
   * is a blocking method. So, this method is wait for
   * the finish of the submitted query.
   *
   * @return If failed, return null.
   */
  ResultSet executeQueryAndGetResult(final String sql) throws ServiceException, IOException;

  ResultSet executeJsonQueryAndGetResult(final String json) throws ServiceException, IOException;

  QueryStatus getQueryStatus(QueryId queryId) throws ServiceException;

  ResultSet getQueryResult(QueryId queryId) throws ServiceException, IOException;

  ResultSet createNullResultSet(QueryId queryId) throws IOException;

  ClientProtos.GetQueryResultResponse getResultResponse(QueryId queryId) throws ServiceException;

  TajoMemoryResultSet fetchNextQueryResult(final QueryId queryId, final int fetchRowNum) throws ServiceException;

  boolean updateQuery(final String sql) throws ServiceException;

  boolean updateQueryWithJson(final String json) throws ServiceException;

  List<ClientProtos.BriefQueryInfo> getRunningQueryList() throws ServiceException;

  List<ClientProtos.BriefQueryInfo> getFinishedQueryList() throws ServiceException;

  List<ClientProtos.WorkerResourceInfo> getClusterInfo() throws ServiceException;

  QueryStatus killQuery(final QueryId queryId) throws ServiceException, IOException;

  QueryInfoProto getQueryInfo(final QueryId queryId) throws ServiceException;

  QueryHistoryProto getQueryHistory(final QueryId queryId) throws ServiceException;
}
