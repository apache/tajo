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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.catalog.Schema;

import java.io.IOException;

public abstract class BinaryPhysicalExec extends PhysicalExec {
  protected PhysicalExec leftChild;
  protected PhysicalExec rightChild;
  protected float progress;
  protected TableStats inputStats;

  public BinaryPhysicalExec(final TaskAttemptContext context,
                            final Schema inSchema, final Schema outSchema,
                            final PhysicalExec outer, final PhysicalExec inner) {
    super(context, inSchema, outSchema);
    this.leftChild = outer;
    this.rightChild = inner;
    this.inputStats = new TableStats();
  }

  public PhysicalExec getLeftChild() {
    return leftChild;
  }

  public PhysicalExec getRightChild() {
    return rightChild;
  }

  protected void init(boolean leftRescan, boolean rightRescan) throws IOException {
    leftChild.init(leftRescan);
    rightChild.init(rightRescan);
    progress = 0.0f;
    super.init(leftRescan || rightRescan);
  }

  @Override
  public void init(boolean needsRescan) throws IOException {
    init(needsRescan, needsRescan);
  }

  @Override
  public void rescan() throws IOException {
    leftChild.rescan();
    rightChild.rescan();
  }

  @Override
  public void close() throws IOException {
    leftChild.close();
    rightChild.close();

    getInputStats();
    
    leftChild = null;
    rightChild = null;
    
    progress = 1.0f;
  }

  @Override
  public float getProgress() {
    if (leftChild == null) {
      return progress;
    }
    return leftChild.getProgress() * 0.5f + rightChild.getProgress() * 0.5f;
  }

  @Override
  public TableStats getInputStats() {
    if (leftChild == null) {
      return inputStats;
    }
    TableStats leftInputStats = leftChild.getInputStats();
    inputStats.setNumBytes(0);
    inputStats.setReadBytes(0);
    inputStats.setNumRows(0);

    if (leftInputStats != null) {
      inputStats.setNumBytes(leftInputStats.getNumBytes());
      inputStats.setReadBytes(leftInputStats.getReadBytes());
      inputStats.setNumRows(leftInputStats.getNumRows());
    }

    TableStats rightInputStats = rightChild.getInputStats();
    if (rightInputStats != null) {
      inputStats.setNumBytes(inputStats.getNumBytes() + rightInputStats.getNumBytes());
      inputStats.setReadBytes(inputStats.getReadBytes() + rightInputStats.getReadBytes());
      inputStats.setNumRows(inputStats.getNumRows() + rightInputStats.getNumRows());
    }

    return inputStats;
  }
}
