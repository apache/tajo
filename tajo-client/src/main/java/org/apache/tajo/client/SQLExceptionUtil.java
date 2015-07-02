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

import com.google.common.collect.Maps;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.exception.ErrorMessages;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;

import java.sql.SQLException;
import java.util.Map;

import static org.apache.tajo.client.ClientErrorUtil.isError;

public class SQLExceptionUtil {

  private static final Map<ResultCode, String> SQLSTATES = Maps.newHashMap();

  static {
    // TODO - All SQLState should be be filled
    SQLSTATES.put(ResultCode.FEATURE_NOT_SUPPORTED, "0A000");
    SQLSTATES.put(ResultCode.NOT_IMPLEMENTED, "0A000");

    SQLSTATES.put(ResultCode.SYNTAX_ERROR, "42601");
  }

  public static void throwIfError(ReturnState state) throws SQLException {
    if (isError(state)) {
      throw toSQLException(state);
    }
  }

  public static SQLException toSQLException(ReturnState state) throws SQLException {
    if (SQLSTATES.containsKey(state.getReturnCode())) {
      return new SQLException(
          state.getMessage(),
          SQLSTATES.get(state.getReturnCode()),
          state.getReturnCode().getNumber()
      );
    } else {
      // If there is no SQLState corresponding to error code,
      // It will make SQLState '42000' (Syntax Error Or Access Rule Violation).
      return new SQLException(
          state.getMessage(),
          "42000",
          ResultCode.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_VALUE
      );
    }
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
