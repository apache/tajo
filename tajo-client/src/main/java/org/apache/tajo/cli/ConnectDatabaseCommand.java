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

import com.google.protobuf.ServiceException;

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
    if (cmd.length == 1) {
      context.getOutput().write(String.format("You are now connected to database \"%s\" as user \"%s\".%n",
          client.getCurrentDatabase(), client.getUserInfo().getUserName()));
    } else if (cmd.length == 2) {
      String databaseName = cmd[1];
      databaseName = databaseName.replace("\"", "");
      if (!client.existDatabase(databaseName)) {
        context.getOutput().write("Database '" + databaseName + "'  not found\n");
      } else {
        try {
          if (client.selectDatabase(databaseName)) {
            context.setCurrentDatabase(client.getCurrentDatabase());
            context.getOutput().write(String.format("You are now connected to database \"%s\" as user \"%s\".%n",
                context.getCurrentDatabase(), client.getUserInfo().getUserName()));
          }
        } catch (ServiceException se) {
          if (se.getMessage() != null) {
            context.getOutput().write(se.getMessage());
          } else {
            context.getOutput().write(String.format("cannot connect the database \"%s\"", databaseName));
          }
        }
      }
    }
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "connect to new database";
  }
}
