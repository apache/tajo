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

package org.apache.tajo.master.event;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.storage.fragment.FileFragment;

public class FragmentScheduleEvent extends TaskSchedulerEvent {
  private final FileFragment leftFragment;
  private final FileFragment rightFragment;

  public FragmentScheduleEvent(final EventType eventType, final ExecutionBlockId blockId,
                               final FileFragment fragment) {
    this(eventType, blockId, fragment, null);
  }

  public FragmentScheduleEvent(final EventType eventType,
                               final ExecutionBlockId blockId,
                               final FileFragment leftFragment,
                               final FileFragment rightFragment) {
    super(eventType, blockId);
    this.leftFragment = leftFragment;
    this.rightFragment = rightFragment;
  }

  public boolean hasRightFragment() {
    return this.rightFragment != null;
  }

  public FileFragment getLeftFragment() {
    return leftFragment;
  }

  public FileFragment getRightFragment() { return rightFragment; }

  @Override
  public String toString() {
    return "FragmentScheduleEvent{" +
        "leftFragment=" + leftFragment +
        ", rightFragment=" + rightFragment +
        '}';
  }
}
