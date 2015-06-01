/*
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

package org.apache.tajo.plan.function.python;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.storage.Tuple;

import java.io.*;
import java.net.URI;

/**
 * Abstract class of script engine
 */
public abstract class TajoScriptEngine {

  protected boolean firstPhase = false;
  protected boolean lastPhase = false;

  /**
   * Open a stream load a script locally or in the classpath
   * @param scriptPath the path of the script
   * @return a stream (it is the responsibility of the caller to close it)
   * @throws IllegalStateException if we could not open a stream
   */
  protected static InputStream getScriptAsStream(URI scriptPath) {
    InputStream is = null;
    File file;
    try {
      file = new File(scriptPath);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("path: " + scriptPath, e);
    }
    if (file.exists()) {
      try {
        is = new FileInputStream(file);
      } catch (FileNotFoundException e) {
        throw new IllegalStateException("could not find existing file "+scriptPath, e);
      }
    }

    if (is == null) {
      throw new IllegalStateException(
          "Could not initialize interpreter (from file system or classpath) with " + scriptPath);
    }
    return is;
  }

  /**
   * Start TajoScriptEngine.
   *
   * @param systemConf
   * @throws IOException
   */
  public abstract void start(Configuration systemConf) throws IOException;

  /**
   * Shutdown TajoScriptEngine.
   * @throws IOException
   */
  public abstract void shutdown();

  /**
   * Evaluate the input tuple.
   *
   * @param input
   * @return
   */
  public abstract Datum callScalarFunc(Tuple input);

  public abstract void callAggFunc(FunctionContext functionContext, Tuple input);

  public abstract String getPartialResult(FunctionContext functionContext);

  public abstract Datum getFinalResult(FunctionContext functionContext);

  public void setFirstPhase(boolean flag) {
    this.firstPhase = flag;
  }

  public void setLastPhase(boolean flag) {
    this.lastPhase = flag;
  }
}
