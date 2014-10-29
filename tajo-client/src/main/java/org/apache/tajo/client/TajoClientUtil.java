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
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.jdbc.FetchResultSet;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.jdbc.TajoResultSet;
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

  public static ResultSet createResultSet(TajoConf conf, TajoClient client, QueryId queryId,
                                          ClientProtos.GetQueryResultResponse response)
      throws IOException {
    TableDesc desc = CatalogUtil.newTableDesc(response.getTableDesc());
    conf.setVar(TajoConf.ConfVars.USERNAME, response.getTajoUserName());
    return new TajoResultSet(client, queryId, conf, desc);
  }

  public static ResultSet createResultSet(TajoConf conf, QueryClient client, ClientProtos.SubmitQueryResponse response)
      throws IOException {
    if (response.hasTableDesc()) {
      // non-forward query
      // select * from table1 [limit 10]
      int fetchRowNum = conf.getIntVar(TajoConf.ConfVars.$RESULT_SET_FETCH_ROWNUM);
      if (response.hasSessionVariables()) {
        for (PrimitiveProtos.KeyValueProto eachKeyValue: response.getSessionVariables().getKeyvalList()) {
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
      return new TajoMemoryResultSet(
          new Schema(serializedResultSet.getSchema()),
          serializedResultSet.getSerializedTuplesList(),
          response.getMaxRowNum());
    }
  }
}
