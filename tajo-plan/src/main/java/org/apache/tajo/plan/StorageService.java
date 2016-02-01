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

package org.apache.tajo.plan;

import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Optional;

/**
 * TablespaceManager interface for loosely coupled usages
 */
public interface StorageService {

  /**
   * Get Table URI
   *
   * @param spaceName Tablespace name. If it is null, the default space will be used
   * @param databaseName Database name
   * @param tableName Table name
   * @return Table URI
   */
  URI getTableURI(@Nullable String spaceName, String databaseName, String tableName);

  long getTableVolumn(TableDesc table, Optional<EvalNode> filter) throws UnsupportedException;
}
