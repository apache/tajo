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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

public class PhysicalRootExec extends PhysicalExec {
  private final List<PhysicalExec> children;
  private final static Schema nullSchema = new Schema();
  private final BitSet trueSet;

  public PhysicalRootExec(TaskAttemptContext context, List<PhysicalExec> children) {
    super(context, nullSchema, nullSchema);
    this.children = children;
    trueSet = new BitSet(children.size());
    trueSet.clear();
    trueSet.flip(0, children.size());
  }

  @Override
  public void init() throws IOException {
    for (PhysicalExec child : children) {
      child.init();
    }
  }

  @Override
  public Tuple next() throws IOException {
    BitSet endFlags = new BitSet(children.size());
    endFlags.clear();

    do {
      for (int i = 0; i < children.size(); i++) {
        if (children.get(i).next() == null) {
          endFlags.set(i);
        }
      }
    } while (!endFlags.equals(trueSet));
    return null;
  }

  @Override
  public void rescan() throws IOException {
    for (PhysicalExec child : children) {
      child.rescan();
    }
  }

  @Override
  public void close() throws IOException {
    for (PhysicalExec child : children) {
      child.close();
    }
  }

  public PhysicalExec getChild(int i) {
    return children.get(i);
  }
}
