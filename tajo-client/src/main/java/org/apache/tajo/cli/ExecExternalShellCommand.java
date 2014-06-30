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

import java.io.*;
import java.util.concurrent.CountDownLatch;

public class ExecExternalShellCommand extends TajoShellCommand {
  public ExecExternalShellCommand(TajoCli.TajoCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\!";
  }

  @Override
  public void invoke(String[] command) throws Exception {
    StringBuilder shellCommand = new StringBuilder();
    String prefix = "";
    for(int i = 1; i < command.length; i++) {
      shellCommand.append(prefix).append(command[i]);
      prefix = " ";
    }

    String builtCommand = shellCommand.toString();
    if (command.length < 2) {
      throw new IOException("ERROR: '" + builtCommand + "' is an invalid command.");
    }

    String[] execCommand = new String[3];
    execCommand[0] = "/bin/bash";
    execCommand[1] = "-c";
    execCommand[2] = builtCommand;

    PrintWriter sout = context.getOutput();

    CountDownLatch latch = new CountDownLatch(2);
    Process process = Runtime.getRuntime().exec(execCommand);
    try {
      InputStreamConsoleWriter inWriter = new InputStreamConsoleWriter(process.getInputStream(), sout, "", latch);
      InputStreamConsoleWriter errWriter = new InputStreamConsoleWriter(process.getErrorStream(), sout, "ERROR: ", latch);

      inWriter.start();
      errWriter.start();

      int processResult = process.waitFor();
      latch.await();
      if (processResult != 0) {
        throw new IOException("ERROR: Failed with exit code = " + processResult);
      }
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(process.getInputStream());
      org.apache.commons.io.IOUtils.closeQuietly(process.getOutputStream());
      org.apache.commons.io.IOUtils.closeQuietly(process.getErrorStream());
    }
  }

  @Override
  public String getUsage() {
    return "<command> [params]";
  }

  @Override
  public String getDescription() {
    return "executes external shell command in TAJO shell";
  }

  static class InputStreamConsoleWriter extends Thread {
    private InputStream in;
    private PrintWriter writer;
    private String prefix;
    private CountDownLatch latch;

    public InputStreamConsoleWriter(InputStream in, PrintWriter writer, String prefix, CountDownLatch latch) {
      this.in = in;
      this.writer = writer;
      this.prefix = prefix;
      this.latch = latch;
    }

    @Override
    public void run() {
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = reader.readLine()) != null) {
          writer.println(prefix + line);
          writer.flush();
        }
      } catch (Exception e) {
        writer.println("ERROR: " + e.getMessage());
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
          }
        }
        latch.countDown();
      }
    }
  }
}
