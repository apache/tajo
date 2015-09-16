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

package org.apache.tajo.ws.rs.netty.testapp2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DirectoriesDao {
  private static DirectoriesDao instance = new DirectoriesDao();

  private final Map<String, Directory> directoryMap = new ConcurrentHashMap<>();

  private DirectoriesDao() {
  }

  public static DirectoriesDao getInstance() {
    return instance;
  }

  public Map<String, Directory> getDirectoryMap() {
    return directoryMap;
  }
}
