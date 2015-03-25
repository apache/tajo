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

package org.apache.tajo.plan.function;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

public class OptionalFunctionContext {

  private Map<String,File> aliasedScriptFiles = new LinkedHashMap<String,File>();

  /**
   * this method adds script files that must be added to the shipped jar
   * named differently from their local fs path.
   * @param name  name in the jar
   * @param path  path on the local fs
   */
  public void addScriptFile(String name, String path) {
    if (path != null) {
      aliasedScriptFiles.put(name.replaceFirst("^/", "").replaceAll(":", ""), new File(path));
    }
  }

  /**
   * calls: addScriptFile(path, new File(path)), ensuring that a given path is
   * added to the jar at most once.
   * @param path
   */
  public void addScriptFile(String path) {
    addScriptFile(path, path);
  }
}
