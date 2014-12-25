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
import org.apache.tajo.jdbc.TajoMemoryResultSet;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.TajoIdProtos.SessionIdProto;

public interface QueryClient extends Closeable {

  public void setSessionId(SessionIdProto sessionId);

  public boolean isConnected();

  public SessionIdProto getSessionId();

  public Map<String, String> getClientSideSessionVars();

  public String getBaseDatabase();

  @Override
  public void close();

  public UserRoleInfo getUserInfo();

  /**
   * Call to QueryMaster closing query resources
   * @param queryId
   */
  public void closeQuery(final QueryId queryId);

  public void closeNonForwardQuery(final QueryId queryId);

  public String getCurrentDatabase() throws ServiceException;

  public Boolean selectDatabase(final String databaseName) throws ServiceException;

  public Map<String, String> updateSessionVariables(final Map<String, String> variables) throws ServiceException;

  public Map<String, String> unsetSessionVariables(final List<String> variables) throws ServiceException;

  public String getSessionVariable(final String varname) throws ServiceException;

  public Boolean existSessionVariable(final String varname) throws ServiceException;

  public Map<String, String> getAllSessionVariables() throws ServiceException;

  /**
   * It submits a query statement and get a response immediately.
   * The response only contains a query id, and submission status.
   * In order to get the result, you should use {@link #getQueryResult(org.apache.tajo.QueryId)}.
   */
  public ClientProtos.SubmitQueryResponse executeQuery(final String sql) throws ServiceException;

  public ClientProtos.SubmitQueryResponse executeQueryWithJson(final String json) throws ServiceException;

  /**
   * It submits a query statement and get a response.
   * The main difference from {@link #executeQuery(String)}
   * is a blocking method. So, this method is wait for
   * the finish of the submitted query.
   *
   * @return If failed, return null.
   */
  public ResultSet executeQueryAndGetResult(final String sql) throws ServiceException, IOException;

  public ResultSet executeJsonQueryAndGetResult(final String json) throws ServiceException, IOException;

  public QueryStatus getQueryStatus(QueryId queryId) throws ServiceException;

  public ResultSet getQueryResult(QueryId queryId) throws ServiceException, IOException;

  public ResultSet createNullResultSet(QueryId queryId) throws IOException;

  public ClientProtos.GetQueryResultResponse getResultResponse(QueryId queryId) throws ServiceException;

  public TajoMemoryResultSet fetchNextQueryResult(final QueryId queryId, final int fetchRowNum) throws ServiceException;

  public boolean updateQuery(final String sql) throws ServiceException;

  public boolean updateQueryWithJson(final String json) throws ServiceException;

  public List<ClientProtos.BriefQueryInfo> getRunningQueryList() throws ServiceException;

  public List<ClientProtos.BriefQueryInfo> getFinishedQueryList() throws ServiceException;

  public List<ClientProtos.WorkerResourceInfo> getClusterInfo() throws ServiceException;

  public QueryStatus killQuery(final QueryId queryId) throws ServiceException, IOException;

  public QueryInfoProto getQueryInfo(final QueryId queryId) throws ServiceException;

  public QueryHistoryProto getQueryHistory(final QueryId queryId) throws ServiceException;
}
