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

package org.apache.tajo.client;

import com.google.common.base.Preconditions;
import org.apache.tajo.QueryId;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.exception.ErrorMessages;
import org.apache.tajo.exception.ErrorUtil;
import org.apache.tajo.exception.ExceptionUtil;
import org.apache.tajo.exception.TajoExceptionInterface;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.ReturnState;

public class ClientErrorUtil {

  public static final ClientProtos.ReturnState OK;

  static {
    ClientProtos.ReturnState.Builder builder = ReturnState.newBuilder();
    builder.setReturnCode(ResultCode.OK);
    OK = builder.build();
  }

  public static ClientProtos.ReturnState returnError(ResultCode code) {
    ReturnState.Builder builder = ReturnState.newBuilder();
    builder.setReturnCode(code);
    builder.setMessage(ErrorMessages.getMessage(code));
    return builder.build();
  }

  public static ReturnState returnError(ResultCode code, String...args) {
    Preconditions.checkNotNull(args);

    ClientProtos.ReturnState.Builder builder = ReturnState.newBuilder();
    builder.setReturnCode(code);
    builder.setMessage(ErrorMessages.getMessage(code, args));
    return builder.build();
  }

  public static ClientProtos.ReturnState returnError(Throwable t) {
    ReturnState.Builder builder = ClientProtos.ReturnState.newBuilder();

    if (ExceptionUtil.isExceptionWithResultCode(t)) {
      TajoExceptionInterface tajoException = (TajoExceptionInterface) t;
      builder.setReturnCode(tajoException.getErrorCode());
      builder.setMessage(tajoException.getMessage());
    } else {
      builder.setReturnCode(ResultCode.INTERNAL_ERROR);
      builder.setMessage(ErrorMessages.getInternalErrorMessage(t));
      builder.setStackTrace(ErrorUtil.convertStacktrace(t));
    }

    return builder.build();
  }

  /**
   * This method check if the state is successful.
   *
   * @param state ResponseState to be checked
   * @return True if ResponseState is success.
   */
  public static boolean isSuccess(ClientProtos.ReturnState state) {
    return ErrorUtil.isOk(state.getReturnCode());
  }

  /**
   * This method check if the state is failed.
   *
   * @param state ResponseState to be checked
   * @return True if ResponseState is failed.
   */
  public static boolean isError(ClientProtos.ReturnState state) {
    return ErrorUtil.isFailed(state.getReturnCode());
  }

  public static ReturnState errInvalidRpcCall(String message) {
    return returnError(ResultCode.INVALID_RPC_CALL, message);
  }

  public static ClientProtos.ReturnState errNoSuchQueryId(QueryId queryId) {
    return returnError(ResultCode.NO_SUCH_QUERYID, queryId.toString());
  }

  public static ClientProtos.ReturnState errNoData(QueryId queryId) {
    return returnError(ResultCode.NO_DATA, queryId.toString());
  }

  public static ReturnState errIncompleteQuery(QueryId queryId) {
    return returnError(ResultCode.INCOMPLETE_QUERY, queryId.toString());
  }

  public static ClientProtos.ReturnState errInvalidSession(String sessionId) {
    return returnError(ResultCode.INVALID_SESSION, sessionId);
  }

  public static ClientProtos.ReturnState errNoSessionVar(String varName) {
    return returnError(ResultCode.NO_SUCH_QUERYID, varName);
  }

  public static ReturnState errUndefinedDatabase(String dbName) {
    return returnError(ResultCode.UNDEFINED_DATABASE, dbName);
  }

  public static ClientProtos.ReturnState errUndefinedTable(String tableName) {
    return returnError(ResultCode.UNDEFINED_TABLE, tableName);
  }

  public static ClientProtos.ReturnState errDuplicateDatabase(String dbName) {
    return returnError(ResultCode.DUPLICATE_DATABASE, dbName);
  }
}
