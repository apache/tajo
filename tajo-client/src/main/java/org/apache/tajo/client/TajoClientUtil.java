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
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.jdbc.FetchResultSet;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.jdbc.TajoResultSetBase;
import org.apache.tajo.rpc.RpcUtils;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;

import java.io.IOException;
import java.sql.ResultSet;

public class TajoClientUtil {

  /* query submit */
  public static boolean isQueryWaitingForSchedule(TajoProtos.QueryState state) {
    return state == TajoProtos.QueryState.QUERY_NOT_ASSIGNED ||
        state == TajoProtos.QueryState.QUERY_MASTER_INIT ||
        state == TajoProtos.QueryState.QUERY_MASTER_LAUNCHED;
  }

  /* query submitted. but is not running */
  public static boolean isQueryInited(TajoProtos.QueryState state) {
    return  state == TajoProtos.QueryState.QUERY_NEW || state == TajoProtos.QueryState.QUERY_INIT;
  }

  /* query started. but is not complete */
  public static boolean isQueryRunning(TajoProtos.QueryState state) {
    return isQueryInited(state) || state == TajoProtos.QueryState.QUERY_RUNNING;
  }

  /* query complete */
  public static boolean isQueryComplete(TajoProtos.QueryState state) {
    return !isQueryWaitingForSchedule(state) && !isQueryRunning(state);
  }

  /* query complete */
  public static boolean isTerminal(TajoProtos.QueryState state) {
    return state == TajoProtos.QueryState.QUERY_SUCCEEDED ||
        state == TajoProtos.QueryState.QUERY_ERROR ||
        state == TajoProtos.QueryState.QUERY_FAILED ||
        state == TajoProtos.QueryState.QUERY_KILLED;
  }

  public static QueryStatus waitTerminal(QueryClient client, QueryId queryId, long timeout)
      throws ServiceException {
    QueryStatus status = client.getQueryStatus(queryId);

    long interval = 100;
    RpcUtils.Timer timer = new RpcUtils.Timer(timeout);
    for (;!TajoClientUtil.isTerminal(status.getState()) && !timer.isTimedOut(); timer.elapsed()) {
      timer.interval(interval);
      status = client.getQueryStatus(queryId);
      interval = Math.min(1000, interval << 1);
    }
    return status;
  }

  public static QueryStatus waitCompletion(QueryClient client, QueryId queryId) throws ServiceException {
    QueryStatus status = client.getQueryStatus(queryId);

    while(!isQueryComplete(status.getState())) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      status = client.getQueryStatus(queryId);
    }
    return status;
  }

  public static ResultSet createResultSet(TajoClient client, QueryId queryId,
                                          ClientProtos.GetQueryResultResponse response, int fetchRows)
      throws IOException {
    TableDesc desc = CatalogUtil.newTableDesc(response.getTableDesc());
    return new FetchResultSet(client, desc.getLogicalSchema(), queryId, fetchRows);
  }

  public static TajoResultSetBase createResultSet(QueryClient client, ClientProtos.SubmitQueryResponse response, int fetchRows)
      throws IOException {
    if (response.hasTableDesc()) {
      // non-forward query
      // select * from table1 [limit 10]
      int fetchRowNum = fetchRows;
      if (response.hasSessionVars()) {
        for (PrimitiveProtos.KeyValueProto eachKeyValue: response.getSessionVars().getKeyvalList()) {
          if (eachKeyValue.getKey().equals(SessionVars.FETCH_ROWNUM.keyname())) {
            fetchRowNum = Integer.parseInt(eachKeyValue.getValue());
          }
        }
      }
      TableDesc tableDesc = new TableDesc(response.getTableDesc());
      return new FetchResultSet(client, tableDesc.getLogicalSchema(), new QueryId(response.getQueryId()), fetchRowNum);
    } else {
      // simple eval query
      // select substr('abc', 1, 2)
      ClientProtos.SerializedResultSet serializedResultSet = response.getResultSet();
      return new TajoMemoryResultSet(new QueryId(response.getQueryId()),
          new Schema(serializedResultSet.getSchema()),
          serializedResultSet.getSerializedTuplesList(),
          response.getMaxRowNum(),
          client.getClientSideSessionVars());
    }
  }

  public static TajoResultSetBase createNullResultSet() {
    return new TajoMemoryResultSet(QueryIdFactory.NULL_QUERY_ID, new Schema(), null, 0, null);
  }

  public static TajoResultSetBase createNullResultSet(QueryId queryId) {
    return new TajoMemoryResultSet(queryId, new Schema(), null, 0, null);
  }
}
