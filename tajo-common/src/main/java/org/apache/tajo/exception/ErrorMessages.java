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

import java.util.Collections;
import java.util.Map;

public class ErrorMessages {
  public static final Map<ResultCode, Pair<String, Integer>> MESSAGES;

  static {
    Map<ResultCode, Pair<String, Integer>> msgs = Maps.newHashMap();

    ADD_MESSAGE(ResultCode.INTERNAL_ERROR, "Internal Error: %s", 1);
    ADD_MESSAGE(ResultCode.INVALID_RPC_CALL, "Invalid RPC Call: %s", 1);

    ADD_MESSAGE(ResultCode.NO_SUCH_QUERYID, "Query id '%s' does not exist", 1);

    ADD_MESSAGE(ResultCode.INVALID_SESSION, "Invalid Session '%s'", 1);
    ADD_MESSAGE(ResultCode.NO_SUCH_SESSION_VARIABLE, "No such session variable '%s", 1);

    ADD_MESSAGE(ResultCode.UNDEFINED_DATABASE, "Database '%s' does not exist", 1);
    ADD_MESSAGE(ResultCode.UNDEFINED_TABLE, "Table '%s' does not exist", 1);

    MESSAGES = Collections.unmodifiableMap(msgs);
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
      throw new TajoRuntimeException(code, args);
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
