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

import java.sql.SQLException;
import java.util.Map;

import static org.apache.tajo.exception.ReturnStateUtil.isError;

public class SQLExceptionUtil {

  private static final Map<ResultCode, String> SQLSTATES = Maps.newHashMap();

  static {
    SQLSTATES.put(ResultCode.INTERNAL_ERROR,                        "XX000");
    SQLSTATES.put(ResultCode.NOT_IMPLEMENTED,                       "0A000");
    SQLSTATES.put(ResultCode.FEATURE_NOT_SUPPORTED,                 "0A000");
    SQLSTATES.put(ResultCode.INVALID_RPC_CALL,                      "08P01"); // Protocol violation


    // Class 61 - Query Management and Scheduler
    SQLSTATES.put(ResultCode.QUERY_FAILED,                          "61T01");
    SQLSTATES.put(ResultCode.QUERY_KILLED,                          "61T02");
    SQLSTATES.put(ResultCode.QUERY_TIMEOUT,                         "61T03");
    SQLSTATES.put(ResultCode.QUERY_NOT_FOUND,                       "61T04");
    SQLSTATES.put(ResultCode.NO_DATA,                               "61T05");
    SQLSTATES.put(ResultCode.INCOMPLETE_QUERY,                      "61T06");


    // Class 62 - Session
    SQLSTATES.put(ResultCode.INVALID_SESSION,                       "62T01");
    SQLSTATES.put(ResultCode.NO_SUCH_SESSION_VARIABLE,              "62T02");
    SQLSTATES.put(ResultCode.INVALID_SESSION_VARIABLE,              "62T03");


    // Data Exception (SQLState Class - 22)
    SQLSTATES.put(ResultCode.DIVISION_BY_ZERO,                      "22012");


    // Section: Class 42 - Syntax Error or Access Rule Violation
    SQLSTATES.put(ResultCode.SYNTAX_ERROR,                          "42601");

    SQLSTATES.put(ResultCode.UNDEFINED_DATABASE,                    "42T01");
    SQLSTATES.put(ResultCode.UNDEFINED_SCHEMA,                      "42T02");
    SQLSTATES.put(ResultCode.UNDEFINED_TABLE,                       "42P01");
    SQLSTATES.put(ResultCode.UNDEFINED_COLUMN,                      "42703");
    SQLSTATES.put(ResultCode.UNDEFINED_FUNCTION,                    "42883");
    SQLSTATES.put(ResultCode.UNDEFINED_INDEX_FOR_TABLE,             "42T03");
    SQLSTATES.put(ResultCode.UNDEFINED_INDEX_FOR_COLUMNS,           "42T04");
    SQLSTATES.put(ResultCode.UNDEFINED_PARTITION,                   "42T05");
    SQLSTATES.put(ResultCode.UNDEFINED_PARTITION_METHOD,            "42T06");
    SQLSTATES.put(ResultCode.UNDEFINED_OPERATOR,                    "42883"); // == UNDEFINED_FUNCTION
    SQLSTATES.put(ResultCode.UNDEFINED_PARTITION_KEY,               "42T07");

    SQLSTATES.put(ResultCode.DUPLICATE_TABLESPACE,                  "42T08");
    SQLSTATES.put(ResultCode.DUPLICATE_DATABASE,                    "42P04");
    SQLSTATES.put(ResultCode.DUPLICATE_SCHEMA,                      "42P06");
    SQLSTATES.put(ResultCode.DUPLICATE_TABLE,                       "42P07");
    SQLSTATES.put(ResultCode.DUPLICATE_COLUMN,                      "42701");
    SQLSTATES.put(ResultCode.DUPLICATE_ALIAS,                       "42712");
    SQLSTATES.put(ResultCode.DUPLICATE_FUNCTION,                    "42723");
    SQLSTATES.put(ResultCode.DUPLICATE_INDEX,                       "42710");
    SQLSTATES.put(ResultCode.DUPLICATE_PARTITION,                   "42T09");

    SQLSTATES.put(ResultCode.AMBIGUOUS_TABLE,                       "42723");
    SQLSTATES.put(ResultCode.AMBIGUOUS_COLUMN,                      "42723");
    SQLSTATES.put(ResultCode.AMBIGUOUS_FUNCTION,                    "42723");
    SQLSTATES.put(ResultCode.AMBIGUOUS_PARTITION_DIRECTORY,         "42T10");

    SQLSTATES.put(ResultCode.CANNOT_CAST,                           "42846");
    SQLSTATES.put(ResultCode.GROUPING_ERROR,                        "42803");
    SQLSTATES.put(ResultCode.WINDOWING_ERROR,                       "42P20");
    SQLSTATES.put(ResultCode.INVALID_RECURSION,                     "42P19");
    SQLSTATES.put(ResultCode.SET_OPERATION_SCHEMA_MISMATCH,         "42601");
    SQLSTATES.put(ResultCode.SET_OPERATION_DATATYPE_MISMATCH,       "42601");
    SQLSTATES.put(ResultCode.INVALID_FOREIGN_KEY,                   "42830");
    SQLSTATES.put(ResultCode.INVALID_NAME,                          "42602");
    SQLSTATES.put(ResultCode.INVALID_COLUMN_DEFINITION,             "42611");
    SQLSTATES.put(ResultCode.NAME_TOO_LONG,                         "42622");
    SQLSTATES.put(ResultCode.RESERVED_NAME,                         "42939");
    SQLSTATES.put(ResultCode.DATATYPE_MISMATCH,                     "42804");
    SQLSTATES.put(ResultCode.INDETERMINATE_DATATYPE,                "42P18");

    // Client Connection
    SQLSTATES.put(ResultCode.CLIENT_CONNECTION_EXCEPTION,           "08001");
    SQLSTATES.put(ResultCode.CLIENT_UNABLE_TO_ESTABLISH_CONNECTION, "08002");
    SQLSTATES.put(ResultCode.CLIENT_CONNECTION_DOES_NOT_EXIST,      "08003");
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
    if (e instanceof TajoRuntimeException) {
      return toSQLException(e.getErrorCode(), ((TajoRuntimeException) e).getCause().getMessage());
    } else {
      return toSQLException(e.getErrorCode(), e.getMessage());
    }
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
