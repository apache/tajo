///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.tajo.plan.function.python;
//
//import org.apache.hadoop.util.Shell;
//
//import javax.script.ScriptEngine;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.InputStream;
//import java.io.IOException;
//import java.util.Map;
//
//public abstract class TajoScriptEngine {
//
//  /**
//   * Open a stream load a script locally or in the classpath
//   * @param scriptPath the path of the script
//   * @return a stream (it is the responsibility of the caller to close it)
//   * @throws IllegalStateException if we could not open a stream
//   */
//  protected static InputStream getScriptAsStream(String scriptPath) {
//    InputStream is;
//    File file = new File(scriptPath);
//    if (file.exists()) {
//      try {
//        is = new FileInputStream(file);
//      } catch (FileNotFoundException e) {
//        throw new IllegalStateException("could not find existing file "+scriptPath, e);
//      }
//    } else {
//      if (Shell.WINDOWS && scriptPath.charAt(1)==':') {
//        scriptPath = scriptPath.charAt(0) + scriptPath.substring(2);
//      }
//      // Try system, current and context classloader.
//      is = ScriptEngine.class.getResourceAsStream(scriptPath);
//      if (is == null) {
//        is = getResourceUsingClassLoader(scriptPath, ScriptEngine.class.getClassLoader());
//      }
//      if (is == null) {
//        is = getResourceUsingClassLoader(scriptPath, Thread.currentThread().getContextClassLoader());
//      }
//      if (is == null && !file.isAbsolute()) {
//        String path = "/" + scriptPath;
//        is = ScriptEngine.class.getResourceAsStream(path);
//        if (is == null) {
//          is = getResourceUsingClassLoader(path, ScriptEngine.class.getClassLoader());
//        }
//        if (is == null) {
//          is = getResourceUsingClassLoader(path, Thread.currentThread().getContextClassLoader());
//        }
//      }
//    }
//
//    if (is == null) {
//      throw new IllegalStateException(
//          "Could not initialize interpreter (from file system or classpath) with " + scriptPath);
//    }
//    return is;
//  }
//
//  private static InputStream getResourceUsingClassLoader(String fullFilename, ClassLoader loader) {
//    if (loader != null) {
//      return loader.getResourceAsStream(fullFilename);
//    }
//    return null;
//  }
//
//  /**
//   * Gets ScriptEngine classname or keyword for the scripting language
//   */
//  protected abstract String getScriptingLang();
//
//  /**
//   * Returns a map from local variable names to their values
//   * @throws java.io.IOException
//   */
//  protected abstract Map<String, Object> getParamsFromVariables() throws IOException;
//}
