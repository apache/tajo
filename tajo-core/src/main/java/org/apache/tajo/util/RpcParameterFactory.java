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
package org.apache.tajo.util;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.rpc.RpcConstants;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Helper class to get RPC Client Connection Parameters
 */
public class RpcParameterFactory {

  static final Map<String, ConfVars> PROPERTIES_MAP = new HashMap<>();

  static {
    PROPERTIES_MAP.put(RpcConstants.CLIENT_RETRY_NUM, ConfVars.RPC_CLIENT_RETRY_NUM);
    PROPERTIES_MAP.put(RpcConstants.CLIENT_CONNECTION_TIMEOUT, ConfVars.RPC_CLIENT_CONNECTION_TIMEOUT);
    PROPERTIES_MAP.put(RpcConstants.CLIENT_SOCKET_TIMEOUT, ConfVars.RPC_CLIENT_SOCKET_TIMEOUT);
    PROPERTIES_MAP.put(RpcConstants.CLIENT_HANG_DETECTION, ConfVars.RPC_CLIENT_HANG_DETECTION_ENABLED);
  }

  public static Properties get(TajoConf conf) {
    final Properties properties = new Properties();

    for (Map.Entry<String, ConfVars> e : PROPERTIES_MAP.entrySet()) {
      properties.put(e.getKey(), conf.getVar(e.getValue()));
    }

    return properties;
  }
}
