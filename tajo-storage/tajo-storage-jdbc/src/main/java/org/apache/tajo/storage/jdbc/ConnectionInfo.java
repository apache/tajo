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

package org.apache.tajo.storage.jdbc;

import org.apache.tajo.exception.TajoInternalError;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ConnectionInfo {
  String scheme;
  String host;
  String dbName;
  String tableName;
  String user;
  String password;
  Map<String, String> params;

  public String scheme() {
    return scheme;
  }

  public String database() {
    return dbName;
  }

  public String table() {
    return tableName;
  }

  public String user() {
    return user;
  }

  public String password() {
    return password;
  }

  public static ConnectionInfo fromURI(String originalUri) {
    return fromURI(URI.create(originalUri));
  }

  public static ConnectionInfo fromURI(URI originalUri) {
    final String uriStr = originalUri.toASCIIString();
    URI uri = originalUri;

    final ConnectionInfo connInfo = new ConnectionInfo();
    connInfo.scheme = uriStr.substring(0, uriStr.indexOf("://"));

    if (connInfo.scheme.split(":").length > 1) {
      int idx = uriStr.indexOf(':');
      uri = URI.create(uriStr.substring(idx + 1));
    }

    connInfo.host = uri.getHost();

    String path = uri.getPath();
    if (path != null && !path.isEmpty()) {
      String [] pathElements = path.substring(1).split("/");
      if (pathElements.length != 1) {
        throw new TajoInternalError("Invalid JDBC path: " + path);
      }
      connInfo.dbName = pathElements[0];
    }

    Map<String, String> params = new HashMap<>();

    int paramIndex = uriStr.indexOf("?");
    if (paramIndex > 0) {
      String parameterPart = uriStr.substring(paramIndex+1, uriStr.length());

      String [] eachParam = parameterPart.split("&");

      for (String each: eachParam) {
        String [] keyValues = each.split("=");
        if (keyValues.length != 2) {
          throw new TajoInternalError("Invalid URI Parameters: " + parameterPart);
        }
        params.put(keyValues[0], keyValues[1]);
      }
    }

    if (params.containsKey("table")) {
      connInfo.tableName = params.remove("table");
    }

    if (params.containsKey("user")) {
      connInfo.user = params.remove("user");
    }
    if (params.containsKey("password")) {
      connInfo.password = params.remove("password");
    }

    connInfo.params = params;

    return connInfo;
  }
}
