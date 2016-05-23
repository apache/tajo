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

/**
 * Client Parameters which can enable or disable some features of TajoClient.
 * This class contains the parameter keys. In more detail,
 * please refer to http://tajo.apache.org/docs/current/jdbc_driver.html##connection-parameters
 */
public interface ClientParameters {
  String USE_COMPRESSION = "useCompression";
  String ROW_FETCH_SIZE = "defaultRowFetchSize";
  String CONNECT_TIMEOUT = "connectTimeout";
  String SOCKET_TIMEOUT = "socketTimeout";
  String RETRY = "retry";
  String TIMEZONE = "timezone";
}
