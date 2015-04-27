/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.plan.function.stream;

import org.apache.tajo.util.TUtil;

import java.io.File;
import java.util.List;
import java.util.Map;

public class StreamingUtil {

  private static final String BASH = "bash";
  private static final String PATH = "PATH";

  /**
   * Create an external process for StreamingCommand command.
   *
   * @param argv process arguments
   * @return
   */
  public static ProcessBuilder createProcess(String[] argv) {
    // Set the actual command to run with 'bash -c exec ...'
    List<String> cmdArgs = TUtil.newList();

    StringBuffer argBuffer = new StringBuffer();
    for (String arg : argv) {
      argBuffer.append(arg);
      argBuffer.append(" ");
    }
    String argvAsString = argBuffer.toString();

    if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
      cmdArgs.add("cmd");
      cmdArgs.add("/c");
      cmdArgs.add(argvAsString);
    } else {
      cmdArgs.add(BASH);
      cmdArgs.add("-c");
      StringBuffer sb = new StringBuffer();
      sb.append("exec ");
      sb.append(argvAsString);
      cmdArgs.add(sb.toString());
    }

    // Start the external process
    ProcessBuilder processBuilder = new ProcessBuilder(cmdArgs
        .toArray(new String[cmdArgs.size()]));
    setupEnvironment(processBuilder);
    return processBuilder;
  }

  /**
   * Set up the run-time environment of the managed process.
   *
   * @param pb {@link ProcessBuilder} used to exec the process
   */
  private static void setupEnvironment(ProcessBuilder pb) {
    String separator = ":";
    Map<String, String> env = pb.environment();

    // Add the current-working-directory to the $PATH
    File dir = pb.directory();
    String cwd = (dir != null) ? dir.getAbsolutePath() : System
        .getProperty("user.dir");

    String envPath = env.get(PATH);
    if (envPath == null) {
      envPath = cwd;
    } else {
      envPath = envPath + separator + cwd;
    }
    env.put(PATH, envPath);
  }
}
