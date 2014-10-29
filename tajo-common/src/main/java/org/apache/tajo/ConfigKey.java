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

import org.apache.tajo.validation.Validator;

public interface ConfigKey {

  // Client can set or change variables of this mode.
  public static final int DEFAULT_MODE = 0;
  // This is similar to DEFAULT mode. In addition, it tries to get values from shell env. variables.
  public static final int FROM_SHELL_ENV_MODE = 1;
  // only TajoMaster is able to set and change variables of this mode.
  public static final int SERVER_SIDE_VAR_MODE = 2;
  // This type variable will be used only in cli side.
  public static final int CLI_SIDE_VAR_MODE = 3;

  public static enum ConfigType {
    SYSTEM(""),
    SESSION("$"),
    QUERY("@"),
    CLI("+");

    private String prefix;

    ConfigType(String prefix) {
      this.prefix = prefix;
    }

    public String getPrefix() {
      return prefix;
    }
  }

  public String keyname();

  public ConfigType type();
  
  public Class<?> valueClass();
  
  public Validator validator();
}
