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

import org.apache.tajo.annotation.Nullable;

import static org.apache.tajo.SessionVars.VariableMode.*;
import static org.apache.tajo.conf.TajoConf.ConfVars;

public enum SessionVars implements InstantConfig {

  USER_NAME(ConfVars.USERNAME, "user name", SERVER_SIDE_VAR),

  // Client Connection
  CLIENT_SESSION_EXPIRY_TIME(ConfVars.CLIENT_SESSION_EXPIRY_TIME, "", DEFAULT),

  // Command line interface
  CLI_MAX_COLUMN(ConfVars.CLI_MAX_COLUMN, "", DEFAULT),
  CLI_PRINT_PAUSE_NUM_RECORDS(ConfVars.CLI_PRINT_PAUSE_NUM_RECORDS, "", DEFAULT),
  CLI_PRINT_PAUSE(ConfVars.CLI_PRINT_PAUSE, "", DEFAULT),
  CLI_PRINT_ERROR_TRACE(ConfVars.CLI_PRINT_ERROR_TRACE, "", DEFAULT),
  CLI_OUTPUT_FORMATTER_CLASS(ConfVars.CLI_OUTPUT_FORMATTER_CLASS, "", DEFAULT),
  CLI_NULL_CHAR(ConfVars.CLI_NULL_CHAR, "", DEFAULT),

  ON_ERROR_STOP(ConfVars.CLI_ERROR_STOP, "tsql will exist if an error occurs.", DEFAULT),

  // Timezone & Date
  TZ(ConfVars.TIMEZONE, "Timezone", FROM_SHELL_ENV),
  DATE_ORDER(ConfVars.DATE_ORDER, "YMD", FROM_SHELL_ENV),

  // Locales and Character set (reserved variables, which are currently not used.
  LANG(null, "Language", FROM_SHELL_ENV),
  LC_ALL(null, "String sort order", FROM_SHELL_ENV),
  LC_COLLATE(null, "String sort order", FROM_SHELL_ENV),
  LC_CTYPE(null, "Character classification (What is a letter? Its upper-case equivalent?)", FROM_SHELL_ENV),
  LC_MESSAGES(null, "Language of messages", FROM_SHELL_ENV),
  LC_MONETARY(null, "Formatting of currency amounts", FROM_SHELL_ENV),
  LC_NUMERIC(null, "Formatting of numbers", FROM_SHELL_ENV),
  LC_TIME(null, "Formatting of dates and times", FROM_SHELL_ENV),

  // Query and Optimization
  OUTPUT_PER_FILE_SIZE(null, "", DEFAULT)
  ;

  private final ConfVars key;
  private final String description;
  private final VariableMode mode;

  public static enum VariableMode {
    DEFAULT,
    SERVER_SIDE_VAR,
    FROM_SHELL_ENV
  }

  public static SessionVars get(String name) {
    return Enum.valueOf(SessionVars.class, name.toUpperCase());
  }

  SessionVars(@Nullable ConfVars key, String description, VariableMode mode) {
    this.key = key;
    this.description = description;
    this.mode = mode;
  }

  public String key() {
    return PREFIX + key.name();
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
}
