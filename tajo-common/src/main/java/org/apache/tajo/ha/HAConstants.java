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

package org.apache.tajo.ha;

public class HAConstants {
  public final static int MASTER_UMBILICAL_RPC_ADDRESS = 1;
  public final static int MASTER_CLIENT_RPC_ADDRESS = 2;
  public final static int RESOURCE_TRACKER_RPC_ADDRESS = 3;
  public final static int CATALOG_ADDRESS = 4;
  public final static int MASTER_INFO_ADDRESS = 5;
  public final static String ACTIVE_MASTER_FILE_NAME = "active_master";
}
