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
import org.apache.tajo.validation.Validator;
import org.apache.tajo.validation.Validators;

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
  SESSION_ID(ConfVars.$EMPTY, "session variable", SERVER_SIDE_VAR, String.class, Validators.notNull()),
  SESSION_LAST_ACCESS_TIME(ConfVars.$EMPTY, "last access time", SERVER_SIDE_VAR, Long.class, Validators.min("0")),

  USERNAME(ConfVars.USERNAME, "username", SERVER_SIDE_VAR),
  CLIENT_HOST(ConfVars.$EMPTY, "client hostname", SERVER_SIDE_VAR),

  CURRENT_DATABASE(ConfVars.$EMPTY, "current database", SERVER_SIDE_VAR),

  //-------------------------------------------------------------------------------
  // Client Side Variables
  //-------------------------------------------------------------------------------

  // Client --------------------------------------------------------
  SESSION_EXPIRY_TIME(ConfVars.$CLIENT_SESSION_EXPIRY_TIME, "session expiry time (secs)", DEFAULT,
      Integer.class, Validators.min("0")),

  // Command line interface and its behavior --------------------------------
  CLI_COLUMNS(ConfVars.$CLI_MAX_COLUMN, "Sets the width for the wrapped format", CLI_SIDE_VAR),
  CLI_FORMATTER_CLASS(ConfVars.$CLI_OUTPUT_FORMATTER_CLASS, "Sets the output format class to display results",
      CLI_SIDE_VAR),
  CLI_NULL_CHAR(ConfVars.$CLI_NULL_CHAR, "Sets the string to be printed in place of a null value.", CLI_SIDE_VAR),

  CLI_PAGE_ROWS(ConfVars.$CLI_PRINT_PAUSE_NUM_RECORDS, "Sets the number of rows for paging", CLI_SIDE_VAR),
  CLI_PAGING_ENABLED(ConfVars.$CLI_PRINT_PAUSE, "Enable paging of result display", CLI_SIDE_VAR),
  CLI_DISPLAY_ERROR_TRACE(ConfVars.$CLI_PRINT_ERROR_TRACE, "Enable display of error trace", CLI_SIDE_VAR),

  ON_ERROR_STOP(ConfVars.$CLI_ERROR_STOP, "tsql will exist if an error occurs.", CLI_SIDE_VAR),

  // Timezone & Date ----------------------------------------------------------
  TIMEZONE(ConfVars.$TIMEZONE, "Sets timezone", DEFAULT),
  DATE_ORDER(ConfVars.$DATE_ORDER, "date order (default is YMD)", CLI_SIDE_VAR),

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
  BROADCAST_NON_CROSS_JOIN_THRESHOLD(ConfVars.$DIST_QUERY_BROADCAST_NON_CROSS_JOIN_THRESHOLD,
      "restriction for the total size of broadcasted table for non-cross join (kb)", DEFAULT, Long.class,
      Validators.min("0")),
  BROADCAST_CROSS_JOIN_THRESHOLD(ConfVars.$DIST_QUERY_BROADCAST_CROSS_JOIN_THRESHOLD,
      "restriction for the total size of broadcasted table for cross join (kb)", DEFAULT, Long.class,
      Validators.min("0")),

  JOIN_TASK_INPUT_SIZE(ConfVars.$DIST_QUERY_JOIN_TASK_VOLUME, "join task input size (mb) ", DEFAULT,
      Integer.class, Validators.min("1")),
  SORT_TASK_INPUT_SIZE(ConfVars.$DIST_QUERY_SORT_TASK_VOLUME, "sort task input size (mb)", DEFAULT,
      Integer.class, Validators.min("1")),
  GROUPBY_TASK_INPUT_SIZE(ConfVars.$DIST_QUERY_GROUPBY_TASK_VOLUME, "group by task input size (mb)", DEFAULT),

  JOIN_PER_SHUFFLE_SIZE(ConfVars.$DIST_QUERY_JOIN_PARTITION_VOLUME, "shuffle output size for join (mb)", DEFAULT,
      Integer.class, Validators.min("1")),
  GROUPBY_PER_SHUFFLE_SIZE(ConfVars.$DIST_QUERY_GROUPBY_PARTITION_VOLUME, "shuffle output size for sort (mb)", DEFAULT,
      Integer.class, Validators.min("1")),
  TABLE_PARTITION_PER_SHUFFLE_SIZE(ConfVars.$DIST_QUERY_TABLE_PARTITION_VOLUME,
      "shuffle output size for partition table write (mb)", DEFAULT, Integer.class, Validators.min("1")),

  GROUPBY_MULTI_LEVEL_ENABLED(ConfVars.$GROUPBY_MULTI_LEVEL_ENABLED, "Multiple level groupby enabled", DEFAULT,
      Boolean.class, Validators.bool()),

  QUERY_EXECUTE_PARALLEL(ConfVars.$QUERY_EXECUTE_PARALLEL_MAX, "Maximum parallel running of execution blocks for a query",
      DEFAULT, Integer.class, Validators.min("1")),

  // for physical Executors
  EXTSORT_BUFFER_SIZE(ConfVars.$EXECUTOR_EXTERNAL_SORT_BUFFER_SIZE, "sort buffer size for external sort (mb)", DEFAULT,
      Long.class, Validators.min("0")),
  HASH_JOIN_SIZE_LIMIT(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD, "limited size for hash join (mb)", DEFAULT,
      Long.class, Validators.min("0")),
  INNER_HASH_JOIN_SIZE_LIMIT(ConfVars.$EXECUTOR_INNER_HASH_JOIN_SIZE_THRESHOLD,
      "limited size for hash inner join (mb)", DEFAULT, Long.class, Validators.min("0")),
  OUTER_HASH_JOIN_SIZE_LIMIT(ConfVars.$EXECUTOR_OUTER_HASH_JOIN_SIZE_THRESHOLD, "limited size for hash outer join (mb)",
      DEFAULT, Long.class, Validators.min("0")),
  HASH_GROUPBY_SIZE_LIMIT(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD, "limited size for hash groupby (mb)",
      DEFAULT, Long.class, Validators.min("0")),
  MAX_OUTPUT_FILE_SIZE(ConfVars.$MAX_OUTPUT_FILE_SIZE, "Maximum per-output file size (mb). 0 means infinite.", DEFAULT,
      Long.class, Validators.min("0")),
  NULL_CHAR(ConfVars.$TEXT_NULL, "null char of text file output", DEFAULT),
  CODEGEN(ConfVars.$CODEGEN, "Runtime code generation enabled (experiment)", DEFAULT),

  // for index
  INDEX_ENABLED(ConfVars.$INDEX_ENABLED, "index scan enabled", DEFAULT),
  INDEX_SELECTIVITY_THRESHOLD(ConfVars.$INDEX_SELECTIVITY_THRESHOLD, "the selectivity threshold for index scan", DEFAULT),

  // for partition overwrite
  PARTITION_NO_RESULT_OVERWRITE_ENABLED(ConfVars.$PARTITION_NO_RESULT_OVERWRITE_ENABLED,
    "If True, a partitioned table is overwritten even if a sub query leads to no result. "
    + "Otherwise, the table data will be kept if there is no result", DEFAULT),

  // Behavior Control ---------------------------------------------------------
  ARITHABORT(ConfVars.$BEHAVIOR_ARITHMETIC_ABORT,
      "If true, a running query will be terminated when an overflow or divide-by-zero occurs.", DEFAULT),

  // ResultSet ----------------------------------------------------------------
  FETCH_ROWNUM(ConfVars.$RESULT_SET_FETCH_ROWNUM, "Sets the number of rows at a time from Master", DEFAULT,
      Integer.class, Validators.min("0")),
  BLOCK_ON_RESULT(ConfVars.$RESULT_SET_BLOCK_WAIT, "Whether to block result set on query execution", DEFAULT,
      Boolean.class, Validators.bool()),
  COMPRESSED_RESULT_TRANSFER(ConfVars.$COMPRESSED_RESULT_TRANSFER, "Use compression to optimize result transmission.",
      CLI_SIDE_VAR, Boolean.class, Validators.bool()),

  //-------------------------------------------------------------------------------
  // Only for Unit Testing
  //-------------------------------------------------------------------------------
  DEBUG_ENABLED(ConfVars.$DEBUG_ENABLED, "(debug only) debug mode enabled", DEFAULT),
  TEST_BROADCAST_JOIN_ENABLED(ConfVars.$TEST_BROADCAST_JOIN_ENABLED, "(test only) broadcast enabled", TEST_VAR),
  TEST_JOIN_OPT_ENABLED(ConfVars.$TEST_JOIN_OPT_ENABLED, "(test only) join optimization enabled", TEST_VAR),
  TEST_FILTER_PUSHDOWN_ENABLED(ConfVars.$TEST_FILTER_PUSHDOWN_ENABLED, "filter push down enabled", TEST_VAR),
  TEST_MIN_TASK_NUM(ConfVars.$TEST_MIN_TASK_NUM, "(test only) min task num", TEST_VAR),
  TEST_PLAN_SHAPE_FIX_ENABLED(ConfVars.$TEST_PLAN_SHAPE_FIX_ENABLED, "(test only) plan shape fix enabled", TEST_VAR),
  ;

  public static final Map<String, SessionVars> SESSION_VARS = Maps.newHashMap();
  public static final Map<String, SessionVars> DEPRECATED_SESSION_VARS = Maps.newHashMap();

  static {
    for (SessionVars var : SessionVars.values()) {
      SESSION_VARS.put(var.keyname(), var);
      DEPRECATED_SESSION_VARS.put(var.getConfVars().keyname(), var);
    }
  }

  private final ConfVars key;
  private final String description;
  private final VariableMode mode;
  
  private Class<?> valClass;
  private Validator validator;

  public static enum VariableMode {
    DEFAULT,         // Client can set or change variables of this mode..
    FROM_SHELL_ENV,  // This is similar to DEFAULT mode. In addition, it tries to get values from shell env. variables.
    SERVER_SIDE_VAR, // only TajoMaster is able to set and change variables of this mode.
    CLI_SIDE_VAR,    // This type variable is used in CLI.
    TEST_VAR         // Only used for unit tests
  }

  SessionVars(ConfVars key, String description, VariableMode mode) {
    this.key = key;
    this.description = description;
    this.mode = mode;
  }
  
  SessionVars(ConfVars key, String description, VariableMode mode, Class<?> valueClass, Validator validator) {
    this(key, description, mode);
    this.valClass = valueClass;
    this.validator = validator;
  }

  public String keyname() {
    return name();
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

  public static boolean exists(String keyname) {
    return SESSION_VARS.containsKey(keyname) || DEPRECATED_SESSION_VARS.containsKey(keyname);
  }

  public static boolean isDeprecated(String keyname) {
    return DEPRECATED_SESSION_VARS.containsKey(keyname);
  }

  public static boolean isPublic(SessionVars var) {
    return var.getMode() != SERVER_SIDE_VAR;
  }

  public static SessionVars get(String keyname) {
    if (SESSION_VARS.containsKey(keyname)) {
      return SESSION_VARS.get(keyname);
    } else if (DEPRECATED_SESSION_VARS.containsKey(keyname)) {
      return DEPRECATED_SESSION_VARS.get(keyname);
    } else {
      return null;
    }
  }

  /**
   * rename deprecated name to current name if the name is deprecated.
   *
   * @param keyname session variable name
   * @return The current session variable name
   */
  public static String handleDeprecatedName(String keyname) {
    return SessionVars.exists(keyname) ? SessionVars.get(keyname).keyname() : keyname;
  }

  @Override
  public Class<?> valueClass() {
    return valClass;
  }

  @Override
  public Validator validator() {
    return validator;
  }
}
