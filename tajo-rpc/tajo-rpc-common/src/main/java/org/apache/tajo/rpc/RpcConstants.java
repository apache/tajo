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

public class RpcConstants {

  public static final String PING_PACKET = "TAJO";
  public static final String RPC_CLIENT_RETRY_MAX = "tajo.rpc.client.retry.max";
  public static final String RPC_CLIENT_TIMEOUT_SECS = "tajo.rpc.client.timeout-secs";

  public static final int DEFAULT_RPC_RETRIES = 3;
  public static final int DEFAULT_RPC_TIMEOUT_SECONDS = 180;
  public static final int DEFAULT_CONNECT_TIMEOUT = 60000;  // 60 sec
  public static final int DEFAULT_PAUSE = 1000; // 1 sec
  public static final int DEFAULT_FUTURE_TIMEOUT_SECONDS = 10;
}
