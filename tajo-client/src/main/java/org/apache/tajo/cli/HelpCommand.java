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

import org.apache.tajo.TajoConstants;

import java.io.PrintWriter;

public class HelpCommand extends TajoShellCommand {
  public HelpCommand(TajoCli.TajoCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\?";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    String docVersion = getDocumentationVersion();
    PrintWriter sout = context.getOutput();
    sout.println();

    sout.println("General");
    sout.println("  \\copyright  show Apache License 2.0");
    sout.println("  \\version    show Tajo version");
    sout.println("  \\?          show help");
    sout.println("  \\q          quit tsql");
    sout.println();
    sout.println();

    sout.println("Informational");
    sout.println("  \\l           list databases");
    sout.println("  \\c           show current database");
    sout.println("  \\c [DBNAME]  connect to new database");
    sout.println("  \\d           list tables");
    sout.println("  \\d [TBNAME]  describe table");
    sout.println("  \\df          list functions");
    sout.println("  \\df NAME     describe function");
    sout.println();
    sout.println();

    sout.println("Variables");
    sout.println("  \\set [[NAME] [VALUE]  set session variable or list session variables");
    sout.println("  \\unset NAME           unset session variable");
    sout.println();
    sout.println();

    sout.println("Documentations");
    sout.println("  tsql guide        http://tajo.incubator.apache.org/docs/"+ docVersion +"/cli.html");
    sout.println("  Query language    http://tajo.incubator.apache.org/docs/"+ docVersion +"/sql_language.html");
    sout.println("  Functions         http://tajo.incubator.apache.org/docs/"+ docVersion +"/functions.html");
    sout.println("  Backup & restore  http://tajo.incubator.apache.org/docs/"+ docVersion +"/backup_and_restore.html");
    sout.println("  Configuration     http://tajo.incubator.apache.org/docs/"+ docVersion +"/configuration.html");
    sout.println();
  }

  private String getDocumentationVersion() {
    int delimiterIdx = TajoConstants.TAJO_VERSION.indexOf("-");
    if (delimiterIdx > -1) {
      return TajoConstants.TAJO_VERSION.substring(0, delimiterIdx);
    } else {
      return TajoConstants.TAJO_VERSION;
    }
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
