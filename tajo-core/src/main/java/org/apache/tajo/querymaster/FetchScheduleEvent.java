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

package org.apache.tajo.querymaster;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.ResourceProtos.FetchProto;
import org.apache.tajo.master.event.TaskSchedulerEvent;
import org.apache.tajo.worker.FetchImpl;

import java.util.List;
import java.util.Map;

public class FetchScheduleEvent extends TaskSchedulerEvent {
  private final Map<String, List<FetchProto>> fetches;  // map of table name and fetch list

  public FetchScheduleEvent(final EventType eventType, final ExecutionBlockId blockId,
                            final Map<String, List<FetchProto>> fetches) {
    super(eventType, blockId);
    this.fetches = fetches;
  }

  public Map<String, List<FetchProto>> getFetches() {
    return fetches;
  }
}
