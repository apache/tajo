/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo;

import com.google.common.collect.Maps;

import java.util.Map;

import static org.apache.tajo.SessionVars.VariableMode.*;
import static org.apache.tajo.conf.TajoConf.ConfVars;

public enum SessionVars implements ConfigKey {

  // Common Suffix Naming Rules:
  //
  // * LIMIT   - We use the suffix 'LIMIT' if the variable is threshold. So, if some value is greater or less than
  //             the variable with suffix 'LIMIT', some action will be different from before.
  // * SIZE    - The suffix 'SIZE' means a data volume like bytes or mega bytes.
  //             It should be used for user's desired volume.
  // * ENABLED - The suffix 'ENABLED' means a true or false value. If it is true, it will enable some feature.
  //             Otherwise, the feature will be turned off.


  //-------------------------------------------------------------------------------
  // Server Side Only Variables
  //-------------------------------------------------------------------------------
  SESSION_ID(ConfVars.$EMPTY, "session variable", SERVER_SIDE_VAR),
  SESSION_LAST_ACCESS_TIME(ConfVars.$EMPTY, "last access time", SERVER_SIDE_VAR),

  USERNAME(ConfVars.USERNAME, "username", SERVER_SIDE_VAR),
  CLIENT_HOST(ConfVars.$EMPTY, "client hostname", SERVER_SIDE_VAR),

  CURRENT_DATABASE(ConfVars.$EMPTY, "current database", SERVER_SIDE_VAR),

  //-------------------------------------------------------------------------------
  // Client Side Variables
  //-------------------------------------------------------------------------------

  // Client --------------------------------------------------------
  SESSION_EXPIRY_TIME(ConfVars.$CLIENT_SESSION_EXPIRY_TIME, "", DEFAULT),

  // Command line interface and its behavior --------------------------------
  // TODO - they should be replaced by '\pset' meta command
  DISPLAY_WIDTH(ConfVars.$CLI_MAX_COLUMN, "", DEFAULT),
  DISPLAY_FORMATTER_CLASS(ConfVars.$CLI_OUTPUT_FORMATTER_CLASS, "", DEFAULT),
  DISPLAY_NULL_CHAR(ConfVars.$CLI_NULL_CHAR, "", DEFAULT),

  CLI_PRINT_PAUSE_NUM_RECORDS(ConfVars.$CLI_PRINT_PAUSE_NUM_RECORDS, "", DEFAULT),
  CLI_PRINT_PAUSE(ConfVars.$CLI_PRINT_PAUSE, "", DEFAULT),
  CLI_PRINT_ERROR_TRACE(ConfVars.$CLI_PRINT_ERROR_TRACE, "", DEFAULT),

  ON_ERROR_STOP(ConfVars.$CLI_ERROR_STOP, "tsql will exist if an error occurs.", DEFAULT),

  // Timezone & Date ----------------------------------------------------------
  TZ(ConfVars.$TIMEZONE, "Timezone", FROM_SHELL_ENV),
  DATE_ORDER(ConfVars.$DATE_ORDER, "YMD", FROM_SHELL_ENV),

  // Locales and Character set ------------------------------------------------
  // TODO - they are reserved variables, and we should support them.
  LANG(ConfVars.$EMPTY, "Language", FROM_SHELL_ENV),
  LC_ALL(ConfVars.$EMPTY, "String sort order", FROM_SHELL_ENV),
  LC_COLLATE(ConfVars.$EMPTY, "String sort order", FROM_SHELL_ENV),
  LC_CTYPE(ConfVars.$EMPTY, "Character classification (What is a letter? Its upper-case equivalent?)", FROM_SHELL_ENV),
  LC_MESSAGES(ConfVars.$EMPTY, "Language of messages", FROM_SHELL_ENV),
  LC_MONETARY(ConfVars.$EMPTY, "Formatting of currency amounts", FROM_SHELL_ENV),
  LC_NUMERIC(ConfVars.$EMPTY, "Formatting of numbers", FROM_SHELL_ENV),
  LC_TIME(ConfVars.$EMPTY, "Formatting of dates and times", FROM_SHELL_ENV),


  // Query and Optimization ---------------------------------------------------

  // for distributed query strategies
  BROADCAST_TABLE_SIZE_LIMIT(ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD, "", DEFAULT),

  JOIN_TASK_INPUT_SIZE(ConfVars.$DIST_QUERY_JOIN_TASK_VOLUME, "join task input size", DEFAULT),
  SORT_TASK_INPUT_SIZE(ConfVars.$DIST_QUERY_SORT_TASK_VOLUME, "sort task input size", DEFAULT),
  GROUPBY_TASK_INPUT_SIZE(ConfVars.$DIST_QUERY_GROUPBY_TASK_VOLUME, "group by task input size", DEFAULT),

  JOIN_PER_SHUFFLE_SIZE(ConfVars.$DIST_QUERY_JOIN_PARTITION_VOLUME, "shuffle output size for join", DEFAULT),
  GROUPBY_PER_SHUFFLE_SIZE(ConfVars.$DIST_QUERY_GROUPBY_PARTITION_VOLUME, "shuffle output size for sort", DEFAULT),
  TABLE_PARTITION_WRITE_PER_SHUFFLE_SIZE(ConfVars.$DIST_QUERY_TABLE_PARTITION_VOLUME, // TODO - rename
      "shuffle output size for partition table write", DEFAULT),

  // for physical Executors
  EXTSORT_BUFFER_SIZE(ConfVars.$EXECUTOR_EXTERNAL_SORT_BUFFER_SIZE, "sort buffer size for external sort", DEFAULT),
  HASH_JOIN_SIZE_LIMIT(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD, "hash join size limit", DEFAULT),
  HASH_GROUPBY_SIZE_LIMIT(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD, "hash groupby size limit", DEFAULT),
  MAX_OUTPUT_FILE_SIZE(ConfVars.$MAX_OUTPUT_FILE_SIZE, "Maximum per-output file size", DEFAULT),

  NULL_CHAR(ConfVars.$CSVFILE_NULL, "null char of text files", DEFAULT),

  // Behavior Control ---------------------------------------------------------
  ARITHABORT(ConfVars.$BEHAVIOR_ARITHMETIC_ABORT,
      "If true, a running query will be terminated when an overflow or divide-by-zero occurs.", DEFAULT),

  //-------------------------------------------------------------------------------
  // Only for Unit Testing
  //-------------------------------------------------------------------------------
  DEBUG_ENABLED(ConfVars.$DEBUG_ENABLED, "(debug only) debug mode enabled", DEFAULT),
  TEST_BROADCAST_JOIN_ENABLED(ConfVars.$TEST_BROADCAST_JOIN_ENABLED, "(test only) broadcast enabled", DEFAULT),
  TEST_JOIN_OPT_ENABLED(ConfVars.$TEST_JOIN_OPT_ENABLED, "(test only) join optimization enabled", DEFAULT),
  TEST_FILTER_PUSHDOWN_ENABLED(ConfVars.$TEST_FILTER_PUSHDOWN_ENABLED, "filter push down enabled", DEFAULT),
  TEST_MIN_TASK_NUM(ConfVars.$TEST_MIN_TASK_NUM, "(test only) min task num", DEFAULT),
  ;

  private static Map<String, SessionVars> SESSION_VARS = Maps.newHashMap();

  static {
    for (SessionVars var : SessionVars.values()) {
      SESSION_VARS.put(var.keyname(), var);
    }
  }

  private final ConfVars key;
  private final String description;
  private final VariableMode mode;

  public static enum VariableMode {
    DEFAULT,         // Client can set or change variables of this mode..
    FROM_SHELL_ENV,  // This is similar to DEFAULT mode. In addition, it tries to get values from shell env. variables.
    SERVER_SIDE_VAR, // only TajoMaster is able to set and change variables of this mode.
  }

  SessionVars(ConfVars key, String description, VariableMode mode) {
    this.key = key;
    this.description = description;
    this.mode = mode;
  }

  public String keyname() {
    return SESSION_PREFIX + name();
  }

  public ConfigType type() {
    return ConfigType.SESSION;
  }

  public ConfVars getConfVars() {
    return key;
  }

  public Class<?> getVarType() {
    return key.valClass;
  }

  public String getDescription() {
    return description;
  }

  public VariableMode getMode() {
    return mode;
  }

  public static boolean exists(String name) {
    return SESSION_VARS.containsKey(name.toUpperCase());
  }

  public static SessionVars get(String keyname) {
    if (exists(keyname)) {
      return SESSION_VARS.get(keyname.toUpperCase());
    } else {
      return null;
    }
  }
}
