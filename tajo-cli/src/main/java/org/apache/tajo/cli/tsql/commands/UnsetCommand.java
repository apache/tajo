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

import com.google.common.collect.Lists;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.NullCompleter;
import jline.console.completer.StringsCompleter;
import org.apache.tajo.cli.tsql.TajoCli;

public class UnsetCommand extends TajoShellCommand {

  public UnsetCommand(TajoCli.TajoCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\unset";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if (cmd.length == 2) {
      client.unsetSessionVariables(Lists.newArrayList(cmd[1]));
    } else {
      context.getOutput().println("usage: \\unset " + getUsage());
    }
  }

  @Override
  public String getUsage() {
    return "[NAME]";
  }

  @Override
  public String getDescription() {
    return "unset a session variable";
  }

  @Override
  public ArgumentCompleter getArgumentCompleter() {
    return new ArgumentCompleter(
        new StringsCompleter(getCommand()),
        new SessionVarCompleter(),
        NullCompleter.INSTANCE);
  }
}
