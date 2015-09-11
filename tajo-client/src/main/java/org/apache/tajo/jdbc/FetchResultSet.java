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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.client.QueryClient;
import org.apache.tajo.error.Errors;
import org.apache.tajo.exception.DefaultTajoException;
import org.apache.tajo.exception.SQLExceptionUtil;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FetchResultSet extends TajoResultSetBase {
  protected QueryClient tajoClient;
  private int fetchRowNum;
  private TajoMemoryResultSet currentResultSet;
  private Future<TajoMemoryResultSet> nextResultSet;
  private boolean finished;
  // maxRows number is limit value of resultSet. The value must be >= 0, and 0 means there is not limit.
  private int maxRows;

  public FetchResultSet(QueryClient tajoClient, Schema schema, QueryId queryId, int fetchRowNum) {
    super(queryId, schema, tajoClient.getClientSideSessionVars());
    this.tajoClient = tajoClient;
    this.maxRows = tajoClient.getMaxRows();
    this.fetchRowNum = fetchRowNum;
    this.totalRow = Integer.MAX_VALUE;
  }

  @Override
  protected Tuple nextTuple() throws IOException {
    if (finished || (maxRows > 0 && curRow >= maxRows)) {
      return null;
    }

    try {
      while (!finished) {
        Tuple tuple;
        if (currentResultSet != null) {
          currentResultSet.next();
          tuple = currentResultSet.cur;

          if (tuple == null) {
            currentResultSet.close();
            currentResultSet = null;
          } else {
            return tuple;
          }

        } else {
          if(nextResultSet == null) {
            nextResultSet = tajoClient.fetchNextQueryResultAsync(queryId, fetchRowNum);
          } else {
            currentResultSet = nextResultSet.get();

            if(currentResultSet.totalRow == 0) {
              currentResultSet.close();
              currentResultSet = null;
              nextResultSet = null;
              finished = true;
            } else {
              // pre-fetch
              nextResultSet = tajoClient.fetchNextQueryResultAsync(queryId, fetchRowNum);
            }
          }
        }
      }

      return null;
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      throw new IOException(t.getMessage(), t);
    } catch (Throwable t) {
      throw new TajoInternalError(t);
    }
  }

  @Override
  public void close() throws SQLException {
    if (currentResultSet != null) {
      currentResultSet.close();
      currentResultSet = null;
    }

    if (nextResultSet != null) {
      try {
        nextResultSet.get(1, TimeUnit.SECONDS).close();
      } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof DefaultTajoException) {
          throw SQLExceptionUtil.toSQLException((DefaultTajoException) t);
        } else {
          throw new TajoSQLException(Errors.ResultCode.INTERNAL_ERROR, t, t.getMessage());
        }
      } catch (Throwable t) {
        throw new TajoSQLException(Errors.ResultCode.INTERNAL_ERROR, t, t.getMessage());
      }
    }
    tajoClient.closeQuery(queryId);
  }
}
