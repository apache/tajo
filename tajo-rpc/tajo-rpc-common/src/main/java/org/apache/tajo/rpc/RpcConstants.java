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

package org.apache.tajo.rpc;

import java.util.concurrent.TimeUnit;

/**
 * Constants for RPC
 */
public class RpcConstants {

  public static final String PING_PACKET = "TAJO";
  public static final int DEFAULT_PAUSE = 1000; // 1 sec
  public static final int FUTURE_TIMEOUT_SECONDS_DEFAULT = 10;

  /** How many times the connect will retry */
  public static final String CLIENT_RETRY_NUM = "tajo.rpc.client.retry-num";
  public static final int CLIENT_RETRY_NUM_DEFAULT = 0;

  /** Client connection timeout (milliseconds) */
  public static final String CLIENT_CONNECTION_TIMEOUT = "tajo.rpc.client.connection-timeout-ms";
  /** Default client connection timeout 15 seconds */
  public final static long CLIENT_CONNECTION_TIMEOUT_DEFAULT = TimeUnit.SECONDS.toMillis(15);

  /**
   * Socket timeout (milliseconds).
   */
  public static final String CLIENT_SOCKET_TIMEOUT = "tajo.rpc.client.socket-timeout-ms";
  /** Default socket timeout - 60 seconds */
  public final static long CLIENT_SOCKET_TIMEOUT_DEFAULT =  TimeUnit.SECONDS.toMillis(180);

  public static final String CLIENT_HANG_DETECTION = "tajo.rpc.client.hang-detection";
  public final static boolean CLIENT_HANG_DETECTION_DEFAULT =  false;
}
