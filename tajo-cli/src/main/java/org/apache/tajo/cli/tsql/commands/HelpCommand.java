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
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.util.VersionInfo;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HelpCommand extends TajoShellCommand {
  private String targetDocVersion = "";

  public HelpCommand(TajoCli.TajoCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\?";
  }

  @Override
  public String [] getAliases() {
    return new String [] {"\\help"};
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if(targetDocVersion.equalsIgnoreCase("")) {
      targetDocVersion = getDocumentationVersion();
    }

    if (cmd.length == 1) {
      PrintWriter sout = context.getOutput();
      sout.println();

      sout.println("General");
      sout.println("  \\copyright    show Apache License 2.0");
      sout.println("  \\version      show Tajo version");
      sout.println("  \\?            show help");
      sout.println("  \\? [COMMAND]  show help of a given command");
      sout.println("  \\help         alias of \\?");
      sout.println("  \\q            quit tsql");
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

      sout.println("Tool");
      sout.println("  \\!           execute a linux shell command");
      sout.println("  \\dfs         execute a dfs command");
      sout.println("  \\admin       execute tajo admin command");
      sout.println();
      sout.println();

      sout.println("Variables");
      sout.println("  \\set [NAME] [VALUE]  set session variable or list session variables");
      sout.println("  \\unset NAME           unset session variable");
      sout.println();
      sout.println();

      sout.println("Documentations");
      sout.println("  tsql guide        http://tajo.apache.org/docs/" + targetDocVersion + "/tsql.html");
      sout.println("  Query language    http://tajo.apache.org/docs/" + targetDocVersion + "/sql_language.html");
      sout.println("  Functions         http://tajo.apache.org/docs/" + targetDocVersion + "/functions.html");
      sout.println("  Backup & restore  http://tajo.apache.org/docs/" + targetDocVersion + "/backup_and_restore.html");
      sout.println("  Configuration     http://tajo.apache.org/docs/" + targetDocVersion + "/configuration.html");
      sout.println();
    } else if (cmd.length == 2) {
      String slashCommand = "\\" + cmd[1];
      if (context.getCommands().containsKey(slashCommand)) {
        context.getCommands().get(slashCommand).printHelp();
      } else {
        context.getOutput().println("Command not found: " + cmd[1]);
      }
    }
  }

  private String getDocumentationVersion() {
    String tajoVersion = "", docVersion = "", docDefaultVersion = "current";
    String tajoFullVersion = VersionInfo.getVersion();

    int delimiterIdx = tajoFullVersion.indexOf("-");
    if (delimiterIdx > -1) {
      tajoVersion = tajoFullVersion.substring(0, delimiterIdx);
    } else {
      tajoVersion = tajoFullVersion;
    }
    
    if(tajoVersion.equalsIgnoreCase("") || tajoFullVersion.contains("SNAPSHOT")) {
      docVersion = docDefaultVersion;
    } else {
      docVersion = tajoVersion;
    }

    return docVersion;
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "show command lists and their usages";
  }

  @Override
  public ArgumentCompleter getArgumentCompleter() {
    List<String> cmds = new ArrayList<>(Arrays.asList(getAliases()));
    cmds.add(getCommand());

    return new ArgumentCompleter(
        new StringsCompleter(cmds.toArray(new String[cmds.size()])),
        new StringsCompleter("copyright", "version", "?", "help", "q", "l", "c", "d", "df", "!", "dfs", "admin",
            "set", "unset", "haadmin", "getconf"), // same order as help string
        NullCompleter.INSTANCE);
  }
}
