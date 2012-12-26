/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.master.event;

import org.apache.hadoop.yarn.api.records.ContainerId;
import tajo.QueryUnitAttemptId;

public class TaskAttemptAssignedEvent extends TaskAttemptEvent {
  private final ContainerId cId;
  private final String hostName;
  private final int pullServerPort;

  public TaskAttemptAssignedEvent(QueryUnitAttemptId id, ContainerId cId,
                                  String hostname, int pullServerPort) {
    super(id, TaskAttemptEventType.TA_ASSIGNED);
    this.cId = cId;
    this.hostName = hostname;
    this.pullServerPort = pullServerPort;
  }

  public ContainerId getContainerId() {
    return cId;
  }

  public String getHostName() {
    return hostName;
  }

  public int getPullServerPort() {
    return pullServerPort;
  }
}
