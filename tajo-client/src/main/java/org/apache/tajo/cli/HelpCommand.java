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

import org.apache.tajo.client.TajoClient;

import java.io.PrintWriter;

public class HelpCommand extends TajoShellCommand {
  public HelpCommand(TajoClient client, PrintWriter sout) {
    super(client, sout);
  }

  @Override
  public String getCommand() {
    return "\\?";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    sout.println();

    sout.println("General");
    sout.println("  \\copyright  show Apache License 2.0");
    sout.println("  \\version    show Tajo version");
    sout.println("  \\?          show help");
    sout.println("  \\q          quit tsql");
    sout.println();
    sout.println();

    sout.println("Informational");
    sout.println("  \\d         list tables");
    sout.println("  \\d  NAME   describe table");
    sout.println("  \\df        list functions");
    sout.println("  \\df NAME   describe function");
    sout.println();
    sout.println();

    sout.println("Documentations");
    sout.println("  tsql guide        http://wiki.apache.org/tajo/tsql");
    sout.println("  Query language    http://wiki.apache.org/tajo/QueryLanguage");
    sout.println("  Functions         http://wiki.apache.org/tajo/Functions");
    sout.println("  Backup & restore  http://wiki.apache.org/tajo/BackupAndRestore");
    sout.println("  Configuration     http://wiki.apache.org/tajo/Configuration");
    sout.println();
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "show command lists and their usages";
  }
}
