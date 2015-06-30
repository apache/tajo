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
import org.apache.tajo.error.Errors;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.ResponseState;

import java.sql.SQLException;
import java.util.Map;

public class SQLExceptionUtil {

  private static final Map<Errors.ResultCode, String> SQL_STATES = Maps.newHashMap();

  public static SQLException convert(ResponseState state) {
    return new SQLException(
        state.getMessage(),
        SQL_STATES.get(state.getReturnCode()),
        state.getReturnCode().getNumber());
  }
}
