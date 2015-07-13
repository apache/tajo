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

package org.apache.tajo.engine.planner;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

public class SimpleProjector {

  private final Tuple outTuple;
  private final int projectIds[];

  public SimpleProjector(Schema inSchema, Column[] keyColumns) {
    outTuple = new VTuple(keyColumns.length);
    projectIds = new int[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      projectIds[i] = inSchema.getColumnId(keyColumns[i].getQualifiedName());
    }
  }

  public Tuple project(Tuple tuple) {
    outTuple.clear();
    RowStoreUtil.project(tuple, outTuple, projectIds);
    return outTuple;
  }
}
