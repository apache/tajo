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

package org.apache.tajo.cli.tsql.commands;

import jline.console.completer.ArgumentCompleter;
import jline.console.completer.NullCompleter;
import jline.console.completer.StringsCompleter;
import org.apache.tajo.SessionVars;
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.exception.NoSuchSessionVariableException;
import org.apache.tajo.util.StringUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tajo.SessionVars.TIMEZONE;
import static org.apache.tajo.SessionVars.VariableMode;

public class SetCommand extends TajoShellCommand {

  public SetCommand(TajoCli.TajoCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\set";
  }

  private void showAllSessionVars() throws SQLException {
    for (Map.Entry<String, String> entry: client.getAllSessionVariables().entrySet()) {
      context.getOutput().println(StringUtils.quote(entry.getKey()) + "=" + StringUtils.quote(entry.getValue()));
    }
  }

  private void updateSessionVariable(String key, String val) throws NoSuchSessionVariableException {
    Map<String, String> variables = new HashMap<>();
    variables.put(key, val);
    client.updateSessionVariables(variables);
  }

  public void set(String key, String val) throws NoSuchSessionVariableException {
    SessionVars sessionVar;

    if (TIMEZONE.name().equalsIgnoreCase(key)) {
      key = TIMEZONE.name();
    }

    if (SessionVars.exists(key)) { // if the variable is one of the session variables
      sessionVar = SessionVars.get(key);

      // is it cli-side variable?
      if (sessionVar.getMode() == VariableMode.CLI_SIDE_VAR) {
        context.setCliSideVar(key, val);
      } else {
        updateSessionVariable(key, val);
      }

      if (SessionVars.isDeprecated(key)) {
        context.getOutput().println("Warning: deprecated to directly use config key in TajoConf.ConfVars. " +
            "Please execute '\\help set'.");
      }
    } else {
      updateSessionVariable(key, val);
    }
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if (cmd.length == 1) {
      showAllSessionVars();
    } else if (cmd.length == 3) {
      set(cmd[1], cmd[2]);
    } else {
      context.getOutput().println("usage: \\set " + getUsage());
    }
  }

  @Override
  public String getUsage() {
    return "[[NAME] VALUE]";
  }

  @Override
  public String getDescription() {
    return "set session variable or shows all session variables";
  }

  @Override
  public void printHelp() {
    context.getOutput().println("\nAvailable Session Variables:\n");
    for (SessionVars var : SessionVars.values()) {

      if (var.getMode() == VariableMode.DEFAULT ||
          var.getMode() == VariableMode.CLI_SIDE_VAR ||
          var.getMode() == VariableMode.FROM_SHELL_ENV) {

        context.getOutput().println("\\set " + var.keyname() + " " + getDisplayType(var.getVarType()) + " - " + var
            .getDescription());
      }
    }
  }

  public static String getDisplayType(Class<?> clazz) {
    if (clazz == String.class) {
      return "[text value]";
    } else if (clazz == Integer.class) {
      return "[int value]";
    } else if (clazz == Long.class) {
      return "[long value]";
    } else if (clazz == Float.class) {
      return "[real value]";
    } else if (clazz == Boolean.class) {
      return "[true or false]";
    } else {
      return clazz.getSimpleName();
    }
  }

  @Override
  public ArgumentCompleter getArgumentCompleter() {
    return new ArgumentCompleter(
        new StringsCompleter(getCommand()),
        new SessionVarCompleter(),
        NullCompleter.INSTANCE);
  }
}
