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

public class ErrorMessages {
  public static final Map<ResultCode, Pair<String, Integer>> MESSAGES;

  static {
    MESSAGES = Maps.newHashMap();

    // Warnings

    // General Errors
    ADD_MESSAGE(ResultCode.INTERNAL_ERROR, "internal error: %s", 1);
    ADD_MESSAGE(ResultCode.NOT_IMPLEMENTED, "not implemented feature: %s", 1);
    ADD_MESSAGE(ResultCode.UNSUPPORTED, "unsupported feature: %s", 1);
    ADD_MESSAGE(ResultCode.INVALID_RPC_CALL, "invalid RPC Call: %s", 1);

    // Query Management and Scheduler
    ADD_MESSAGE(ResultCode.NO_SUCH_QUERYID, "query %s does not exist", 1);
    ADD_MESSAGE(ResultCode.NO_DATA, "no data for %s due to query failure or error", 1);
    ADD_MESSAGE(ResultCode.INCOMPLETE_QUERY, "query %s is stilling running", 1);

    // Session
    ADD_MESSAGE(ResultCode.INVALID_SESSION, "invalid Session '%s'", 1);
    ADD_MESSAGE(ResultCode.NO_SUCH_SESSION_VARIABLE, "no such session variable '%s", 1);
    ADD_MESSAGE(ResultCode.INVALID_SESSION_VARIABLE, "invalid session variable '%s': %s", 2);


    ADD_MESSAGE(ResultCode.INSUFFICIENT_PRIVILEGE, "Insufficient privilege to %s");

    ADD_MESSAGE(ResultCode.SYNTAX_ERROR, "%s", 1);

    ADD_MESSAGE(ResultCode.UNDEFINED_DATABASE, "database '%s' does not exist", 1);
    ADD_MESSAGE(ResultCode.UNDEFINED_SCHEMA, "schema '%s' does not exist", 1);
    ADD_MESSAGE(ResultCode.UNDEFINED_TABLE, "relation '%s' does not exist", 1);
    ADD_MESSAGE(ResultCode.UNDEFINED_COLUMN, "column '%s' does not exist", 1);
    ADD_MESSAGE(ResultCode.UNDEFINED_FUNCTION, "function does not exist: %s", 1);
    ADD_MESSAGE(ResultCode.UNDEFINED_OPERATOR, "operator does not exist: '%s'", 1);

    ADD_MESSAGE(ResultCode.DUPLICATE_DATABASE, "database '%s' already exists", 1);
    ADD_MESSAGE(ResultCode.DUPLICATE_SCHEMA, "schema '%s' already exists", 1);
    ADD_MESSAGE(ResultCode.DUPLICATE_TABLE, "table '%s' already exists", 1);
    ADD_MESSAGE(ResultCode.DUPLICATE_COLUMN, "column '%s' already exists", 1);
    ADD_MESSAGE(ResultCode.DUPLICATE_ALIAS, "table name '%s' specified more than once", 1);
    ADD_MESSAGE(ResultCode.DUPLICATE_INDEX, "index '%s' already exists", 1);
    ADD_MESSAGE(ResultCode.DUPLICATE_PARTITION, "partition '%s' already exists", 1);

    ADD_MESSAGE(ResultCode.DIVISION_BY_ZERO, "Division by zero: %s", 1);

    ADD_MESSAGE(ResultCode.DATATYPE_MISMATCH,
        "column \"%s\" is of type %s but expression %s is of type %s", 4);

    ADD_MESSAGE(ResultCode.SET_OPERATION_SCHEMA_MISMATCH, "each %s query must have the same number of columns", 1);
    ADD_MESSAGE(ResultCode.SET_OPERATION_DATATYPE_MISMATCH, "%s types %s and %s cannot be matched");

    ADD_MESSAGE(ResultCode.CAT_UPGRADE_REQUIRED, "catalog must be upgraded");
    ADD_MESSAGE(ResultCode.CAT_CANNOT_CONNECT, "cannot connect metadata store '%s': %s", 2);

    ADD_MESSAGE(ResultCode.TMC_NO_MATCHED_DATATYPE, "no matched type for %s", 1);

    ADD_MESSAGE(ResultCode.UNKNOWN_DATAFORMAT, "Unknown data format: '%s'", 1);
  }

  private static void ADD_MESSAGE(ResultCode code, String msgFormat) {
    ADD_MESSAGE(code, msgFormat, 0);
  }

  private static void ADD_MESSAGE(ResultCode code, String msgFormat, int argNum) {
    MESSAGES.put(code, new Pair<String, Integer>(msgFormat, argNum));
  }

  public static String getInternalErrorMessage() {
    return MESSAGES.get(ResultCode.INTERNAL_ERROR).getFirst();
  }

  public static String getInternalErrorMessage(Throwable t) {
    if (t.getMessage() != null) {
      return MESSAGES.get(ResultCode.INTERNAL_ERROR).getFirst() + ": " + t.getMessage();
    } else {
      return getInternalErrorMessage();
    }

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
        throw new TajoRuntimeException(code, args);
      }
    }
  }
}
