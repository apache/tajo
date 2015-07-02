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
import org.apache.tajo.exception.TajoExceptionInterface;
import org.apache.tajo.ipc.ClientProtos.ResponseState;

import java.sql.SQLException;

public class ClientErrorUtil {

  public static final ResponseState OK;

  static {
    ResponseState.Builder builder = ResponseState.newBuilder();
    builder.setReturnCode(ResultCode.OK);
    OK = builder.build();
  }

  public static ResponseState returnOk() {
    return OK;
  }

  public static ResponseState returnError(ResultCode code) {
    ResponseState.Builder builder = ResponseState.newBuilder();
    builder.setReturnCode(code);
    builder.setMessage(ErrorMessages.getMessage(code));
    return builder.build();
  }

  public static ResponseState returnError(ResultCode code, String...args) {
    Preconditions.checkNotNull(args);

    ResponseState.Builder builder = ResponseState.newBuilder();
    builder.setReturnCode(code);
    builder.setMessage(ErrorMessages.getMessage(code, args));
    return builder.build();
  }

  public static ResponseState returnError(Throwable t) {
    ResponseState.Builder builder = ResponseState.newBuilder();

    if (t instanceof TajoExceptionInterface) {
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
  public static boolean isSuccess(ResponseState state) {
    return ErrorUtil.isOk(state.getReturnCode());
  }

  /**
   * This method check if the state is failed.
   *
   * @param state ResponseState to be checked
   * @return True if ResponseState is failed.
   */
  public static boolean isError(ResponseState state) {
    return ErrorUtil.isFailed(state.getReturnCode());
  }

  public static boolean isThisError(ResponseState state, ResultCode expected) {
    return state.getReturnCode() == expected;
  }

  public static ResponseState ERR_INVALID_RPC_CALL(String message) {
    return returnError(ResultCode.INVALID_RPC_CALL, message);
  }

  public static ResponseState ERR_NO_SUCH_QUERY_ID(QueryId queryId) {
    return returnError(ResultCode.NO_SUCH_QUERYID, queryId.toString());
  }

  public static ResponseState ERR_NO_DATA(QueryId queryId) {
    return returnError(ResultCode.NO_DATA, queryId.toString());
  }

  public static ResponseState ERR_INCOMPLETE_QUERY(QueryId queryId) {
    return returnError(ResultCode.INCOMPLETE_QUERY, queryId.toString());
  }

  public static ResponseState ERR_INVALID_SESSION(String sessionId) {
    return returnError(ResultCode.INVALID_SESSION, sessionId);
  }

  public static ResponseState ERR_NO_SESSION_VARIABLE(String varName) {
    return returnError(ResultCode.NO_SUCH_QUERYID, varName);
  }

  public static ResponseState ERR_UNDEFIED_DATABASE(String dbName) {
    return returnError(ResultCode.UNDEFINED_DATABASE, dbName);
  }

  public static ResponseState ERR_UNDEFIED_TABLE(String tableName) {
    return returnError(ResultCode.UNDEFINED_TABLE, tableName);
  }

  public static ResponseState ERR_DUPLICATE_DATABASE(String dbName) {
    return returnError(ResultCode.DUPLICATE_DATABASE, dbName);
  }
}
