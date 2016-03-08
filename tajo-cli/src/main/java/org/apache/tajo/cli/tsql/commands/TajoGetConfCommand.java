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

import com.google.common.base.Splitter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.NullCompleter;
import jline.console.completer.StringsCompleter;
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.cli.tools.TajoGetConf;
import org.apache.tajo.conf.TajoConf;

import java.util.*;

public class TajoGetConfCommand extends TajoShellCommand {
  private TajoGetConf getconf;

  public TajoGetConfCommand(TajoCli.TajoCliContext context) {
    super(context);
    getconf = new TajoGetConf(context.getConf(), context.getOutput(), context.getTajoClient());
  }

  @Override
  public String getCommand() {
    return "\\getconf";
  }

  @Override
  public void invoke(String[] command) throws Exception {
    try {
      String[] getConfCommands = new String[command.length - 1];
      System.arraycopy(command, 1, getConfCommands, 0, getConfCommands.length);

      getconf.runCommand(getConfCommands);
    } catch (Exception e) {
      context.getOutput().println("ERROR: " + e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "<command> [options]";
  }

  @Override
  public String getDescription() {
    return "execute a tajo getconf command.";
  }

  @Override
  public ArgumentCompleter getArgumentCompleter() {
    TajoConf.ConfVars[] vars = TajoConf.ConfVars.values();
    List<String> confNames = new ArrayList<>();

    for(TajoConf.ConfVars varname: vars) {
      confNames.add(varname.varname);
    }

    return new ArgumentCompleter(
        new StringsCompleter(getCommand()),
        new ConfCompleter(confNames.toArray(new String[confNames.size()])),
        NullCompleter.INSTANCE);
  }

  private static class ConfCompleter extends StringsCompleter {
    ConfCompleter(String [] confs) {
      super(confs);
    }

    @Override
    public int complete(final String buf, final int cur, final List<CharSequence> candidates) {
      int result = super.complete(buf, cur, candidates);

      // it means just "if candidates are too many". 10 is arbitrary default.
      if (candidates.size() > 10) {
        Set<CharSequence> delimited = new LinkedHashSet<>();
        for (CharSequence candidate : candidates) {
          Iterator<String> it = Splitter.on(".").split(
              candidate.subSequence(cur, candidate.length())).iterator();
          if (it.hasNext()) {
            String next = it.next();
            if (next.isEmpty()) {
              next = ".";
            }
            candidate = buf != null ? buf.substring(0, cur) + next : next;
          }
          delimited.add(candidate);
        }

        candidates.clear();
        candidates.addAll(delimited);
      }

      return result;
    }
  }
}
