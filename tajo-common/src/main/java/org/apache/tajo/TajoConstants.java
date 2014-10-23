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

package org.apache.tajo;

public class TajoConstants {
  public static final String SYSTEM_CONF_FILENAME = "system_conf.xml";

  public static final String SYSTEM_DIR_NAME = "system";
  public static final String WAREHOUSE_DIR_NAME = "warehouse";
  public static final String SYSTEM_RESOURCE_DIR_NAME = "resource";
  public static final String RESULT_DIR_NAME="RESULT";
  public static final String INSERT_OVERWIRTE_OLD_TABLE_NAME ="OLD_TABLE";

  public static final String DEFAULT_TABLESPACE_NAME = "default";
  public static final String DEFAULT_DATABASE_NAME = "default";
  public static final String DEFAULT_SCHEMA_NAME = "public";


  public static final String EMPTY_STRING = "";

  public static final String SYSTEM_HA_DIR_NAME = "ha";
  public static final String SYSTEM_HA_ACTIVE_DIR_NAME = "active";
  public static final String SYSTEM_HA_BACKUP_DIR_NAME = "backup";

  public static final int UNKNOWN_ROW_NUMBER = -1;

  private TajoConstants() {}
}
