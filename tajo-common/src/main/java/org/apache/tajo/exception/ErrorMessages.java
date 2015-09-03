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
import org.apache.tajo.util.Pair;

import java.util.Map;

import static org.apache.tajo.error.Errors.ResultCode.*;

public class ErrorMessages {
  public static final Map<ResultCode, Pair<String, Integer>> MESSAGES;

  static {
    MESSAGES = Maps.newHashMap();

    // Warnings

    // General Errors
    ADD_MESSAGE(INTERNAL_ERROR, "internal error: %s", 1);
    ADD_MESSAGE(NOT_IMPLEMENTED, "not implemented feature: %s", 1);
    ADD_MESSAGE(FEATURE_NOT_SUPPORTED, "unsupported feature: %s", 1);
    ADD_MESSAGE(INVALID_RPC_CALL, "invalid RPC Call: %s", 1);

    // Query Management and Scheduler
    ADD_MESSAGE(QUERY_FAILED, "query has been failed due to %s", 1);
    ADD_MESSAGE(QUERY_KILLED, "query has been killed");
    ADD_MESSAGE(QUERY_NOT_FOUND, "query %s does not exist", 1);
    ADD_MESSAGE(NO_DATA, "no data for %s due to query failure or error", 1);
    ADD_MESSAGE(INCOMPLETE_QUERY, "query %s is stilling running", 1);

    // Session
    ADD_MESSAGE(INVALID_SESSION, "invalid Session '%s'", 1);
    ADD_MESSAGE(NO_SUCH_SESSION_VARIABLE, "no such session variable '%s", 1);
    ADD_MESSAGE(INVALID_SESSION_VARIABLE, "invalid session variable '%s': %s", 2);


    // Syntax Error or Access Rule Violation
    ADD_MESSAGE(SYNTAX_ERROR, "%s", 1);
    ADD_MESSAGE(INSUFFICIENT_PRIVILEGE, "Insufficient privilege to %s", 1);
    ADD_MESSAGE(INVALID_NAME, "Invalid name '%s'");

    ADD_MESSAGE(UNDEFINED_TABLESPACE, "tablespace '%s' does not exist", 1);
    ADD_MESSAGE(UNDEFINED_DATABASE, "database '%s' does not exist", 1);
    ADD_MESSAGE(UNDEFINED_SCHEMA, "schema '%s' does not exist", 1);
    ADD_MESSAGE(UNDEFINED_TABLE, "relation '%s' does not exist", 1);
    ADD_MESSAGE(UNDEFINED_COLUMN, "column '%s' does not exist", 1);
    ADD_MESSAGE(UNDEFINED_FUNCTION, "function does not exist: %s", 1);
    ADD_MESSAGE(UNDEFINED_PARTITION_METHOD, "table '%s' is not a partitioned table", 1);
    ADD_MESSAGE(UNDEFINED_PARTITION, "partition '%s' does not exist", 1);
    ADD_MESSAGE(UNDEFINED_PARTITION_KEY, "'%s' column is not a partition key", 1);
    ADD_MESSAGE(UNDEFINED_OPERATOR, "operator does not exist: '%s'", 1);
    ADD_MESSAGE(UNDEFINED_INDEX_FOR_TABLE, "index ''%s' does not exist", 1);
    ADD_MESSAGE(UNDEFINED_INDEX_FOR_COLUMNS, "index does not exist for '%s' columns of '%s' table", 2);
    ADD_MESSAGE(UNDEFINED_INDEX_NAME, "index name '%s' does not exist", 1);

    ADD_MESSAGE(DUPLICATE_TABLESPACE, "tablespace '%s' already exists", 1);
    ADD_MESSAGE(DUPLICATE_DATABASE, "database '%s' already exists", 1);
    ADD_MESSAGE(DUPLICATE_SCHEMA, "schema '%s' already exists", 1);
    ADD_MESSAGE(DUPLICATE_TABLE, "table '%s' already exists", 1);
    ADD_MESSAGE(DUPLICATE_COLUMN, "column '%s' already exists", 1);
    ADD_MESSAGE(DUPLICATE_ALIAS, "table name '%s' specified more than once", 1);
    ADD_MESSAGE(DUPLICATE_INDEX, "index '%s' already exists", 1);
    ADD_MESSAGE(DUPLICATE_PARTITION, "partition for '%s' already exists", 1);

    ADD_MESSAGE(AMBIGUOUS_TABLE, "table name '%s' is ambiguous", 1);
    ADD_MESSAGE(AMBIGUOUS_COLUMN, "column name '%s' is ambiguous", 1);
    ADD_MESSAGE(AMBIGUOUS_FUNCTION, "function '%s' is ambiguous", 1);

    ADD_MESSAGE(DIVISION_BY_ZERO, "Division by zero: %s", 1);

    ADD_MESSAGE(DATATYPE_MISMATCH,
        "column '%s' is of type %s but expression %s is of type %s", 4);

    ADD_MESSAGE(SET_OPERATION_SCHEMA_MISMATCH, "each %s query must have the same number of columns", 1);
    ADD_MESSAGE(SET_OPERATION_DATATYPE_MISMATCH, "%s types %s and %s cannot be matched");

    ADD_MESSAGE(CAT_UPGRADE_REQUIRED, "catalog must be upgraded");
    ADD_MESSAGE(CAT_CANNOT_CONNECT, "cannot connect metadata store '%s': %s", 2);

    ADD_MESSAGE(CAT_UNSUPPORTED_CATALOG_STORE, "unsupported catalog store: %s", 1);

    ADD_MESSAGE(LMD_NO_MATCHED_DATATYPE, "no matched type for %s", 1);

    // Storage and Data Format
    ADD_MESSAGE(UNAVAILABLE_TABLE_LOCATION, "unavailable table location '%s': %s", 2);
    ADD_MESSAGE(UNKNOWN_DATAFORMAT, "unknown data format: '%s'", 1);
    ADD_MESSAGE(UNSUPPORTED_DATATYPE, "unsupported data type: '%s'", 1);
    ADD_MESSAGE(INVALID_TABLE_PROPERTY, "invalid table property '%s': '%s'", 2);
    ADD_MESSAGE(MISSING_TABLE_PROPERTY, "table property '%s' required for '%s'", 2);

    ADD_MESSAGE(AMBIGUOUS_PARTITION_DIRECTORY, "There is a directory which is assumed to be a partitioned directory" +
      " : '%s'", 1);

    ADD_MESSAGE(TOO_LARGE_INPUT_FOR_CROSS_JOIN, "Cross join of large tables is not allowed: (%s). " +
        "To execute cross join, please increase BROADCAST_CROSS_JOIN_THRESHOLD " +
        "which is currently set to %s.", 2);
    ADD_MESSAGE(INVALID_INPUTS_FOR_CROSS_JOIN, "At least one of both inputs for the cross join must be a simple " +
        "relation.");

    ADD_MESSAGE(CLIENT_CONNECTION_EXCEPTION, "%s", 1);
    ADD_MESSAGE(CLIENT_UNABLE_TO_ESTABLISH_CONNECTION, "Client is unable to establish connection to '%s'", 1);
    ADD_MESSAGE(CLIENT_CONNECTION_DOES_NOT_EXIST, "This connection has been closed.");
  }

  private static void ADD_MESSAGE(ResultCode code, String msgFormat) {
    ADD_MESSAGE(code, msgFormat, 0);
  }

  private static void ADD_MESSAGE(ResultCode code, String msgFormat, int argNum) {
    MESSAGES.put(code, new Pair<String, Integer>(msgFormat, argNum));
  }

  public static String getInternalErrorMessage() {
    return MESSAGES.get(INTERNAL_ERROR).getFirst();
  }

  public static String getInternalErrorMessage(Throwable t) {
    if (t.getMessage() != null) {
      return String.format(MESSAGES.get(INTERNAL_ERROR).getFirst(), t.getMessage());
    } else {
      return getInternalErrorMessage();
    }

  }

  public static String concat(String[] args) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String s : args) {
      if (!first) {
        sb.append(",");
      }
      sb.append(s);
    }
    return sb.toString();
  }

  public static String getMessage(ResultCode code, String...args) {
    if (!MESSAGES.containsKey(code)) {
      throw new TajoInternalError("no error message for " + code);
    } else {

      Pair<String, Integer> messageFormat = MESSAGES.get(code);

      if (messageFormat.getSecond() == args.length) { // if arguments are matched

        if (args.length == 0) { // no argument
          return MESSAGES.get(code).getFirst();
        } else {
          return String.format(MESSAGES.get(code).getFirst(), args);
        }

      } else {
        throw new TajoInternalError(
            "Error message arguments are invalid: code=" + code.name() + ", args=" + concat(args) +
                ". Please report this bug to https://issues.apache.org/jira/browse/TAJO.");
      }
    }
  }
}
