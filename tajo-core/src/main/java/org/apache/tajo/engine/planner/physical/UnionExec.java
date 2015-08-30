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

/**
 * 
 */
package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public class UnionExec extends BinaryPhysicalExec {
  private boolean nextOuter = true;
  private Tuple tuple;

  public UnionExec(TaskAttemptContext context, PhysicalExec outer, PhysicalExec inner) {
    super(context, outer.getSchema(), inner.getSchema(), outer, inner);
    if (!outer.getSchema().equals(inner.getSchema())) {
      throw new TajoInternalError("the both schemas are not same");
    }
  }

  @Override
  public Tuple next() throws IOException {
    if (nextOuter) {
      tuple = leftChild.next();
      if (tuple == null) {
       nextOuter = false; 
      } else {
        return tuple;
      }
    }
    
    return rightChild.next();
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    nextOuter = true;
  }
}
