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

package org.apache.tajo.exception;

import com.google.common.collect.Maps;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;

import javax.xml.transform.Result;
import java.sql.SQLException;
import java.util.Map;

import static org.apache.tajo.exception.ReturnStateUtil.isError;

public class SQLExceptionUtil {

  private static final Map<ResultCode, String> SQLSTATES = Maps.newHashMap();

  static {
    // TODO - All SQLState should be be filled

    SQLSTATES.put(ResultCode.INTERNAL_ERROR, "XX000");
    SQLSTATES.put(ResultCode.NOT_IMPLEMENTED, "0A000");
    SQLSTATES.put(ResultCode.FEATURE_NOT_SUPPORTED, "0A000");
    // for future
    // SQLSTATES.put(ResultCode.INVALID_RPC_CALL, "XX003");

    SQLSTATES.put(ResultCode.SYNTAX_ERROR,                "42601");

    SQLSTATES.put(ResultCode.UNDEFINED_DATABASE,          "42T01");
    SQLSTATES.put(ResultCode.UNDEFINED_SCHEMA,            "42T02");
    SQLSTATES.put(ResultCode.UNDEFINED_TABLE,             "42P01");
    SQLSTATES.put(ResultCode.UNDEFINED_COLUMN,            "42703");
    SQLSTATES.put(ResultCode.UNDEFINED_FUNCTION,          "42883");
    SQLSTATES.put(ResultCode.UNDEFINED_INDEX_FOR_TABLE,   "42T03");
    SQLSTATES.put(ResultCode.UNDEFINED_INDEX_FOR_COLUMNS, "42T04");
    SQLSTATES.put(ResultCode.UNDEFINED_PARTITION,         "42T05");
    SQLSTATES.put(ResultCode.UNDEFINED_PARTITION_METHOD,  "42T06");
    SQLSTATES.put(ResultCode.UNDEFINED_OPERATOR,          "42883"); // == UNDEFINED_FUNCTION

    // Client Connection
    SQLSTATES.put(ResultCode.CLIENT_CONNECTION_EXCEPTION, "08001");
  }

  public static boolean isThisError(SQLException e, ResultCode code) {
    if (SQLSTATES.containsKey(code)) {
      return e.getSQLState().equals(SQLSTATES.get(code));
    } else {
      throw new TajoInternalError("Unknown error code: " + code.name());
    }
  }

  public static void throwIfError(ReturnState state) throws SQLException {
    if (isError(state)) {
      throw toSQLException(state);
    }
  }

  private static SQLException toSQLException(ResultCode code, String message) throws SQLException {
    if (SQLSTATES.containsKey(code)) {

      return new SQLException(
          message,
          SQLSTATES.get(code),
          code.getNumber()
      );

    } else {
      // If there is no SQLState corresponding to error code,
      // It will make SQLState '42000' (Syntax Error Or Access Rule Violation).
      return new SQLException(
          message,
          "42000",
          ResultCode.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_VALUE
      );
    }
  }

  public static String toSQLState(ResultCode code) {
    if (SQLSTATES.containsKey(code)) {
      return SQLSTATES.get(code);
    } else {
      return "42000";
    }
  }

  public static SQLException toSQLException(DefaultTajoException e) throws SQLException {
    return toSQLException(e.getErrorCode(), e.getMessage());
  }

  public static SQLException toSQLException(ReturnState state) throws SQLException {
    return toSQLException(state.getReturnCode(), state.getMessage());
  }

  public static SQLException makeSQLException(ResultCode code, String ...args) {
    if (SQLSTATES.containsKey(code)) {
      return new SQLException(
          ErrorMessages.getMessage(code, args),
          SQLSTATES.get(code),
          code.getNumber());
    } else {
      // If there is no SQLState corresponding to error code,
      // It will make SQLState '42000' (Syntax Error Or Access Rule Violation).
      return new SQLException(
          code.name(),
          "42000",
          code.getNumber());
    }

  }

  public static SQLException makeUnableToEstablishConnection(Throwable t) {
    return makeSQLException(
        ResultCode.CLIENT_UNABLE_TO_ESTABLISH_CONNECTION, t.getMessage());
  }
}
