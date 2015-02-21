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

import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.cli.tsql.TajoCli;

public abstract class TajoShellCommand {
  public abstract String getCommand();
  public String [] getAliases() {
    return new String[] {};
  }
  public abstract void invoke(String [] command) throws Exception;
  public abstract String getUsage();
  public abstract String getDescription();
  public void printHelp() {
    context.getOutput().print(getCommand());
    context.getOutput().print(" - ");
    context.getOutput().println(getDescription());
  }

  protected TajoCli.TajoCliContext context;
  protected TajoClient client;
  protected int maxColumn;

  public TajoShellCommand(TajoCli.TajoCliContext context) {
    maxColumn = context.getConf().getIntVar(TajoConf.ConfVars.$CLI_MAX_COLUMN);
    this.context = context;
    client = context.getTajoClient();
  }

  protected void println() {
    context.getOutput().println();
  }

  protected void printLeft(String message, int columnWidth) {
    int messageLength = message.length();

    if(messageLength >= columnWidth) {
      context.getOutput().print(message.substring(0, columnWidth - 1));
    } else {
      context.getOutput().print(message);
      print(' ', columnWidth - messageLength - 1);
    }
  }

  protected void printCenter(String message, int columnWidth, boolean warp) {
    int messageLength = message.length();

    if(messageLength > columnWidth) {
      context.getOutput().print(message.substring(0, columnWidth - 1));
    } else {
      int numPadding = (columnWidth - messageLength)/2;

      print(' ', numPadding);
      context.getOutput().print(message);
      print(' ', numPadding);
    }
    if(warp) {
      println();
    }
  }

  protected void printCenter(String message) {
    printCenter(message, maxColumn, true);
  }

  protected void print(char c, int count) {
    for(int i = 0; i < count; i++) {
      context.getOutput().print(c);
    }
  }

  protected int[] printHeader(String[] headers, float[] columnWidthRates) {
    int[] columnWidths = new int[columnWidthRates.length];

    int columnWidthSum = 0;
    for(int i = 0; i < columnWidths.length; i++) {
      columnWidths[i] = (int)(maxColumn * columnWidthRates[i]);
      if(i > 0) {
        columnWidthSum += columnWidths[i - 1];
      }
    }

    columnWidths[columnWidths.length - 1] = maxColumn - columnWidthSum;

    String prefix = "";
    for(int i = 0; i < headers.length; i++) {
      context.getOutput().print(prefix);
      printLeft(" " + headers[i], columnWidths[i]);
      prefix = "|";
    }
    println();

    int index = 0;
    int printPos = columnWidths[index] - 1;
    for(int i = 0; i < maxColumn; i++) {
      if(i == printPos) {
        if(index < columnWidths.length - 1) {
          print('+', 1);
          index++;
          printPos += columnWidths[index];
        }
      } else {
        print('-', 1);
      }
    }

    println();
    return columnWidths;
  }
}
