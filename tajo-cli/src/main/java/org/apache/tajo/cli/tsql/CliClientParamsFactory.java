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

package org.apache.tajo.cli.tsql;

import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.client.ClientParameters;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

class CliClientParamsFactory {
  static Map<String, String> DEFAULT_PARAMS = new HashMap<>();

  static {
    // Keep lexicographic order of parameter names.
    DEFAULT_PARAMS.put(ClientParameters.CONNECT_TIMEOUT, "0");
    DEFAULT_PARAMS.put(ClientParameters.SOCKET_TIMEOUT, "0");
    DEFAULT_PARAMS.put(ClientParameters.RETRY, "3");
    DEFAULT_PARAMS.put(ClientParameters.ROW_FETCH_SIZE, "200");
    DEFAULT_PARAMS.put(ClientParameters.USE_COMPRESSION, "false");
    DEFAULT_PARAMS.put(ClientParameters.TIMEZONE, TimeZone.getDefault().getID());
  }

  public static Properties get(@Nullable Properties connParam) {
    Properties copy = connParam == null ? new Properties() : (Properties) connParam.clone();
    for (Map.Entry<String, String> entry : DEFAULT_PARAMS.entrySet()) {
      if (!copy.contains(entry.getKey())) {
        copy.setProperty(entry.getKey(), entry.getValue());
      }
    }
    return copy;
  }
}
