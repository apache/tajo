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

package org.apache.tajo.cli;

import org.apache.tajo.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class SetCommand extends TajoShellCommand {

  public SetCommand(TajoCli.TajoCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\set";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if (cmd.length == 1) {
      for (Map.Entry<String, String> entry: client.getAllSessionVariables().entrySet()) {
        context.getOutput().println(StringUtils.quote(entry.getKey()) + "=" + StringUtils.quote(entry.getValue()));
      }
    } else if (cmd.length == 3) {
      Map<String, String> variables = new HashMap<String, String>();
      variables.put(cmd[1], cmd[2]);
      client.updateSessionVariables(variables);
      context.setVariable(cmd[1], cmd[2]);
    } else {
      context.getOutput().println("usage: \\set [[NAME] VALUE]");
    }
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "set session variable or shows all session variables";
  }
}
