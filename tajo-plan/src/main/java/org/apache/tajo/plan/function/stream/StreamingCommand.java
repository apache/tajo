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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * {@link StreamingCommand} represents the specification of an external
 * command to be executed in a Pig Query.
 *
 * <code>StreamingCommand</code> encapsulates all relevant details of the
 * command specified by the user either directly via the <code>STREAM</code>
 * operator or indirectly via a <code>DEFINE</code> operator. It includes
 * details such as input/output/error specifications and also files to be
 * shipped to the cluster and files to be cached.
 */
public class StreamingCommand implements Serializable, Cloneable {
  private static final long serialVersionUID = 1L;

  // External command to be executed and it's parsed components
  String executable;
  String[] argv;

  // Files to be shipped to the cluster in-order to be executed
  List<String> shipSpec = new LinkedList<String>();

  // Files to be shipped to the cluster in-order to be executed
  List<String> cacheSpec = new LinkedList<String>();

  /**
   * Handle to communicate with the external process.
   */
  public enum Handle {INPUT, OUTPUT}

  // Should the stderr of the process be persisted?
  boolean persistStderr = false;

  // Directory where the process's stderr logs should be persisted.
  String logDir;

  // Limit on the number of persisted log-files
  int logFilesLimit = 100;
  public static final int MAX_TASKS = 100;

  boolean shipFiles = true;

  /**
   * Create a new <code>StreamingCommand</code> with the given command.
   *
   * @param argv parsed arguments of the <code>command</code>
   */
  public StreamingCommand(String[] argv) {
    this.argv = argv;

    // Assume that argv[0] is the executable
    this.executable = this.argv[0];
  }

  /**
   * Get the command to be executed.
   *
   * @return the command to be executed
   */
  public String getExecutable() {
    return executable;
  }

  /**
   * Set the executable for the <code>StreamingCommand</code>.
   *
   * @param executable the executable for the <code>StreamingCommand</code>
   */
  public void setExecutable(String executable) {
    this.executable = executable;
  }

  /**
   * Set the command line arguments for the <code>StreamingCommand</code>.
   *
   * @param argv the command line arguments for the
   *             <code>StreamingCommand</code>
   */
  public void setCommandArgs(String[] argv) {
    this.argv = argv;
  }

  /**
   * Get the parsed command arguments.
   *
   * @return the parsed command arguments as <code>String[]</code>
   */
  public String[] getCommandArgs() {
    return argv;
  }

  /**
   * Get the list of files which need to be shipped to the cluster.
   *
   * @return the list of files which need to be shipped to the cluster
   */
  public List<String> getShipSpecs() {
    return shipSpec;
  }

  /**
   * Get the list of files which need to be cached on the execute nodes.
   *
   * @return the list of files which need to be cached on the execute nodes
   */
  public List<String> getCacheSpecs() {
    return cacheSpec;
  }

  /**
   * Add a file to be shipped to the cluster.
   *
   * Users can use this to distribute executables and other necessary files
   * to the clusters.
   *
   * @param path path of the file to be shipped to the cluster
   */
  public void addPathToShip(String path) throws IOException {
    // Validate
    File file = new File(path);
    if (!file.exists()) {
      throw new IOException("Invalid ship specification: '" + path +
          "' does not exist!");
    } else if (file.isDirectory()) {
      throw new IOException("Invalid ship specification: '" + path +
          "' is a directory and can't be shipped!");
    }
    shipSpec.add(path);
  }

  /**
   * Should the stderr of the managed process be persisted?
   *
   * @return <code>true</code> if the stderr of the managed process should be
   *         persisted, <code>false</code> otherwise.
   */
  public boolean getPersistStderr() {
    return persistStderr;
  }

  /**
   * Specify if the stderr of the managed process should be persisted.
   *
   * @param persistStderr <code>true</code> if the stderr of the managed
   *                      process should be persisted, else <code>false</code>
   */
  public void setPersistStderr(boolean persistStderr) {
    this.persistStderr = persistStderr;
  }

  /**
   * Get the directory where the log-files of the command are persisted.
   *
   * @return the directory where the log-files of the command are persisted
   */
  public String getLogDir() {
    return logDir;
  }

  /**
   * Set the directory where the log-files of the command are persisted.
   *
   * @param logDir the directory where the log-files of the command are persisted
   */
  public void setLogDir(String logDir) {
    this.logDir = logDir;
    if (this.logDir.startsWith("/")) {
      this.logDir = this.logDir.substring(1);
    }
    setPersistStderr(true);
  }

  /**
   * Get the maximum number of tasks whose stderr logs files are persisted.
   *
   * @return the maximum number of tasks whose stderr logs files are persisted
   */
  public int getLogFilesLimit() {
    return logFilesLimit;
  }

  /**
   * Set the maximum number of tasks whose stderr logs files are persisted.
   * @param logFilesLimit the maximum number of tasks whose stderr logs files
   *                      are persisted
   */
  public void setLogFilesLimit(int logFilesLimit) {
    this.logFilesLimit = Math.min(MAX_TASKS, logFilesLimit);
  }

  /**
   * Set whether files should be shipped or not.
   *
   * @param shipFiles <code>true</code> if files of this command should be
   *                  shipped, <code>false</code> otherwise
   */
  public void setShipFiles(boolean shipFiles) {
    this.shipFiles = shipFiles;
  }

  /**
   * Get whether files for this command should be shipped or not.
   *
   * @return <code>true</code> if files of this command should be shipped,
   *         <code>false</code> otherwise
   */
  public boolean getShipFiles() {
    return shipFiles;
  }

  public Object clone() {
    try {
      StreamingCommand clone = (StreamingCommand)super.clone();

      clone.shipSpec = new ArrayList<String>(shipSpec);
      clone.cacheSpec = new ArrayList<String>(cacheSpec);

      return clone;
    } catch (CloneNotSupportedException cnse) {
      // Shouldn't happen since we do implement Clonable
      throw new InternalError(cnse.toString());
    }
  }
}
