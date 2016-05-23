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

package org.apache.tajo.client;

import org.apache.tajo.SessionVars;
import org.apache.tajo.rpc.RpcConstants;
import org.apache.tajo.util.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.tajo.SessionVars.COMPRESSED_RESULT_TRANSFER;
import static org.apache.tajo.SessionVars.FETCH_ROWNUM;
import static org.apache.tajo.SessionVars.TIMEZONE;
import static org.apache.tajo.client.ClientParameterHelper.ActionType.CONNECTION_PARAM;
import static org.apache.tajo.client.ClientParameterHelper.ActionType.SESSION_UPDATE;
import static org.apache.tajo.rpc.RpcConstants.CLIENT_CONNECTION_TIMEOUT;
import static org.apache.tajo.rpc.RpcConstants.CLIENT_SOCKET_TIMEOUT;

/**
 * <ul>
 *   <li><code>useCompression=bool</code> - Enable compressed transfer for ResultSet. </li>
 *   <li><code>defaultRowFetchSize=int</code> - Determine the number of rows fetched in ResultSet by
 *       one fetch with trip to the Server.</li>
 *   <li><code>connectTimeout=int</code> - The timeout value used for socket connect operations.
 *       If connecting to the server takes longer than this value,the connection is broken. The
 *       timeout is specified in seconds and a value of zero means that it is disabled.</li>
 *   <li><code>socketTimeout=int</code></li> - The timeout value used for socket read operations.
 *       If reading from the server takes longer than this value, the connection is closed.
 *       This can be used as both a brute force global query timeout and a method of detecting
 *       network problems. The timeout is specified in seconds and a value of zero means that
 *       it is disabled.</li>
 *   <li><code>retry=int</code>Number of retry operation. Tajo JDBC driver is resilient
 *   against some network or connection problems. It determines how many times the connection will retry.</li>
 * </ul>
 */
class ClientParameterHelper {

  public static Map<String, Action> PARAMETERS = new HashMap<>();

  static {
    PARAMETERS.put(ClientParameters.USE_COMPRESSION, new SimpleSessionAction(COMPRESSED_RESULT_TRANSFER));
    PARAMETERS.put(ClientParameters.ROW_FETCH_SIZE, new SimpleSessionAction(FETCH_ROWNUM));
    PARAMETERS.put(ClientParameters.CONNECT_TIMEOUT, new ConnectionParamAction() {
      @Override
      Pair<String, String> doAction(String param) {
        int seconds = Integer.parseInt(param);
        // convert seconds into mili seconds
        return new Pair<>(CLIENT_CONNECTION_TIMEOUT, String.valueOf(TimeUnit.SECONDS.toMillis(seconds)));
      }
    });
    PARAMETERS.put(ClientParameters.SOCKET_TIMEOUT, new ConnectionParamAction() {
      @Override
      Pair<String, String> doAction(String param) {
        int seconds = Integer.parseInt(param);
        return new Pair<>(CLIENT_SOCKET_TIMEOUT, String.valueOf(TimeUnit.SECONDS.toMillis(seconds)));
      }
    });
    PARAMETERS.put(ClientParameters.RETRY, new SimpleConnectionParamAction(RpcConstants.CLIENT_RETRY_NUM));
    PARAMETERS.put(ClientParameters.TIMEZONE, new SimpleSessionAction(TIMEZONE));
  }

  enum ActionType {
    SESSION_UPDATE,
    CONNECTION_PARAM
  }

  interface Action {
    ActionType type();
  }

  static class SimpleSessionAction extends SessionAction {
    private final String sessionKey;

    SimpleSessionAction(SessionVars sessionVar) {
      this.sessionKey = sessionVar.name();
    }

    Pair<String, String> doAction(String param) {
      return new Pair<>(sessionKey, param);
    }
  }

  @SuppressWarnings("unused")
  static abstract class SessionAction implements Action {

    @Override
    public ActionType type() {
      return SESSION_UPDATE;
    }

    abstract Pair<String, String> doAction(String param);
  }

  static class SimpleConnectionParamAction extends ConnectionParamAction {
    final String connParamKey;

    SimpleConnectionParamAction(String connParamKey) {
      this.connParamKey = connParamKey;
    }

    public Pair<String, String> doAction(String param) {
      return new Pair<>(connParamKey, param);
    }
  }

  @SuppressWarnings("unused")
  static abstract class ConnectionParamAction implements Action {

    @Override
    public ActionType type() {
      return ActionType.CONNECTION_PARAM;
    }

    abstract Pair<String, String> doAction(String param);
  }

  public static Properties getConnParams(Collection<Map.Entry<String, String>> properties) {
    Properties connParams = new Properties();
    for (Map.Entry<String, String> entry : properties) {
      if(PARAMETERS.containsKey(entry.getKey()) && PARAMETERS.get(entry.getKey()).type() == CONNECTION_PARAM) {
        Pair<String, String> keyValue =
            ((ConnectionParamAction)PARAMETERS.get(entry.getKey())).doAction(entry.getValue());
        connParams.put(keyValue.getFirst(), keyValue.getSecond());
      }
    }

    return connParams;
  }

  public static Map<String, String> getSessionVars(Collection<Map.Entry<String, String>> properties) {
    Map<String, String> sessionVars = new HashMap<>();

    for (Map.Entry<String, String> entry : properties) {
      if(PARAMETERS.containsKey(entry.getKey()) && PARAMETERS.get(entry.getKey()).type() == SESSION_UPDATE) {
        Pair<String, String> keyValue =
            ((SessionAction)PARAMETERS.get(entry.getKey())).doAction(entry.getValue());
        sessionVars.put(keyValue.getFirst(), keyValue.getSecond());
      }
    }

    return sessionVars;
  }
}
