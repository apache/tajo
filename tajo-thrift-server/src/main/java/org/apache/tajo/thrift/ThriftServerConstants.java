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

package org.apache.tajo.thrift;

public interface ThriftServerConstants {
  public static final String MIN_WORKERS_OPTION = "minWorkers";
  public static final String MAX_WORKERS_OPTION = "workers";
  public static final String MAX_QUEUE_SIZE_OPTION = "queue";
  public static final String KEEP_ALIVE_SEC_OPTION = "keepAliveSec";
  public static final String BIND_OPTION = "bind";
  public static final String PORT_OPTION = "port";

  public static final String SERVER_ADDRESS_CONF_KEY = "tajo.thrift.server.address";
  public static final String SERVER_PORT_CONF_KEY = "tajo.thrift.server.port";

  public static final String INFO_SERVER_ADDRESS_CONF_KEY = "tajo.thrift.infoserver.address";
  public static final String MAX_SESSION_CONF_KEY = "tajo.thrift.max.sessions";
  public static final String MAX_TASK_RUNNER_CONF_KEY = "tajo.thrift.max.taskrunners";

  static final String DEFAULT_BIND_ADDRESS = "0.0.0.0";
  static final int DEFAULT_LISTEN_PORT = 26700;
  static final String INFO_SERVER_DEFAULT_ADDRESS = "0.0.0.0:26800";

  static final int DEFAULT_FETCH_SIZE = 1000;
}
