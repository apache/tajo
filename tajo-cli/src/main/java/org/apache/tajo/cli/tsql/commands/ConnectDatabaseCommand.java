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

import com.google.common.base.Preconditions;
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.exception.TajoException;

public class ConnectDatabaseCommand extends TajoShellCommand {

  public ConnectDatabaseCommand(TajoCli.TajoCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\c";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {

    if (cmd.length == 1) { // no given database name

      context.getOutput().write(String.format("You are now connected to database \"%s\" as user \"%s\".%n",
          client.getCurrentDatabase(), client.getUserInfo().getUserName()));

    } else if (cmd.length >= 2) {

      StringBuilder databaseNameMaker = new StringBuilder();
      for (int i = 1; i < cmd.length; i++) {
        if (i != 1) {
          databaseNameMaker.append(" ");
        }
        databaseNameMaker.append(cmd[i]);
      }

      final String databaseName = databaseNameMaker.toString().replace("\"", "");

      try {
        client.selectDatabase(databaseName);
        Preconditions.checkState(databaseName.equals(client.getCurrentDatabase()));

        context.setCurrentDatabase(client.getCurrentDatabase());
        context.getOutput().write(String.format(
                "You are now connected to database \"%s\" as user \"%s\".%n",
                context.getCurrentDatabase(),
                client.getUserInfo().getUserName())
        );

      } catch (TajoException se) {
        context.getOutput().write(String.format("ERROR: %s%n", se.getMessage()));
      }
    }
  }

  @Override
  public String getUsage() {
    return "[database_name]";
  }

  @Override
  public String getDescription() {
    return "connect to new database";
  }
}
