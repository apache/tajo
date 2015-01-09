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

/**
 * Event Types handled by Stage
 */
public enum StageEventType {

  // Producer: Query
  SQ_INIT,
  SQ_START,
  SQ_CONTAINER_ALLOCATED,
  SQ_KILL,
  SQ_LAUNCH,

  // Producer: Task
  SQ_TASK_COMPLETED,
  SQ_FAILED,

  // Producer: Stage
  SQ_SHUFFLE_REPORT,
  SQ_STAGE_COMPLETED,

  // Producer: Any component
  SQ_DIAGNOSTIC_UPDATE,
  SQ_INTERNAL_ERROR
}
