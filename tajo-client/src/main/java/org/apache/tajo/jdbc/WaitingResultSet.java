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

package org.apache.tajo.jdbc;

import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.QueryClient;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.exception.SQLExceptionUtil;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.ipc.ClientProtos;

import java.sql.SQLException;

/**
 * Blocks on schema retrieval if it's not ready
 */
public class WaitingResultSet extends FetchResultSet {

  public WaitingResultSet(QueryClient tajoClient, QueryId queryId, int fetchRowNum)
      throws SQLException {
    super(tajoClient, null, queryId, fetchRowNum);
  }

  @Override
  public boolean next() throws SQLException {
    getSchema();
    return super.next();
  }

  @Override
  protected Schema getSchema() throws SQLException {
    return schema == null ? schema = waitOnResult() : schema;
  }

  private Schema waitOnResult() throws SQLException {
    try {
      QueryStatus status = TajoClientUtil.waitCompletion(tajoClient, queryId);

      if (status.getState() != TajoProtos.QueryState.QUERY_SUCCEEDED) {
        throw new SQLException(status.getErrorMessage(),
            SQLExceptionUtil.toSQLState(ResultCode.INTERNAL_ERROR), ResultCode.INTERNAL_ERROR.getNumber());
      }

      ClientProtos.GetQueryResultResponse response = tajoClient.getResultResponse(queryId);
      TableDesc tableDesc = CatalogUtil.newTableDesc(response.getTableDesc());
      return tableDesc.getLogicalSchema();
    } catch (TajoException e) {
      throw SQLExceptionUtil.toSQLException(e);
    }
  }
}
