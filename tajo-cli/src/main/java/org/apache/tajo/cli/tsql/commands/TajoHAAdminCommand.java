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

import org.apache.tajo.cli.tools.TajoHAAdmin;
import org.apache.tajo.cli.tsql.TajoCli;

public class TajoHAAdminCommand extends TajoShellCommand {
  private TajoHAAdmin haAdmin;

  public TajoHAAdminCommand(TajoCli.TajoCliContext context) {
    super(context);
    haAdmin = new TajoHAAdmin(context.getConf(), context.getOutput(), context.getTajoClient());
  }

  @Override
  public String getCommand() {
    return "\\haadmin";
  }

  @Override
  public void invoke(String[] command) throws Exception {
    try {
      String[] haAdminCommands = new String[command.length - 1];
      System.arraycopy(command, 1, haAdminCommands, 0, haAdminCommands.length);

      haAdmin.runCommand(haAdminCommands);
    } catch (Exception e) {
      context.getError().println("ERROR: " + e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "<command> [options]";
  }

  @Override
  public String getDescription() {
    return "execute a tajo haAdminF command.";
  }
}
