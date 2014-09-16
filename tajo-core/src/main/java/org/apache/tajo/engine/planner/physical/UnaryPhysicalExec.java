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

import com.google.common.annotations.VisibleForTesting;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

public abstract class UnaryPhysicalExec extends PhysicalExec {
  protected PhysicalExec child;
  protected float progress;
  protected TableStats inputStats;

  public UnaryPhysicalExec(TaskAttemptContext context,
                           Schema inSchema, Schema outSchema,
                           PhysicalExec child) {
    super(context, inSchema, outSchema);
    this.child = child;
  }

  public <T extends PhysicalExec> T getChild() {
    return (T) this.child;
  }

  @VisibleForTesting
  public void setChild(PhysicalExec child){
    this.child = child;
  }

  @Override
  public void init() throws IOException {
    progress = 0.0f;
    if (child != null) {
      child.init();
    }

    super.init();
  }

  @Override
  public void rescan() throws IOException {
    progress = 0.0f;
    if (child != null) {
      child.rescan();
    }
  }

  @Override
  public void close() throws IOException {
    progress = 1.0f;
    if (child != null) {
      child.close();
      try {
        TableStats stat = child.getInputStats();
        if (stat != null) {
          inputStats = (TableStats)(stat.clone());
        }
      } catch (CloneNotSupportedException e) {
        e.printStackTrace();
      }
      child = null;
    }
  }

  @Override
  public float getProgress() {
    if (child != null) {
      return child.getProgress();
    } else {
      return progress;
    }
  }

  @Override
  public TableStats getInputStats() {
    if (child != null) {
      return child.getInputStats();
    } else {
      return inputStats;
    }
  }
}
