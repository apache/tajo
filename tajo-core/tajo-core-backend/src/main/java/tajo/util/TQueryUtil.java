/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.util;

import tajo.TajoProtos.TaskAttemptState;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.master.QueryUnit;

public class TQueryUtil {
  public static TaskStatusProto getInProgressStatusProto(QueryUnit unit) {
    TaskStatusProto.Builder builder = TaskStatusProto.newBuilder();
    builder.setId(unit.getLastAttempt().getId().getProto());

    // TODO - to be fixed
    // QueryState
    builder.setState(TaskAttemptState.TA_SUCCEEDED);
    builder.setProgress(1.0f);
    builder.addAllPartitions(unit.getPartitions());
    builder.setResultStats(unit.getStats().getProto());
    return builder.build();
  }
}
