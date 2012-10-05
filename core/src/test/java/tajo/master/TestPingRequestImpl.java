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

package tajo.master;

import org.junit.Test;
import tajo.QueryIdFactory;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.engine.query.StatusReportImpl;
import tajo.engine.utils.TUtil;
import tajo.ipc.StatusReport;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestPingRequestImpl {

  @Test
  public void test() {
    QueryIdFactory.reset();
    
    List<TaskStatusProto> list
      = new ArrayList<TaskStatusProto>();
    
    TaskStatusProto.Builder builder = TaskStatusProto.newBuilder()
        .setId(TUtil.newQueryUnitAttemptId().getProto())
        .setProgress(0.5f)
        .setStatus(QueryStatus.QUERY_FINISHED);
    list.add(builder.build());
    
    builder = TaskStatusProto.newBuilder()
        .setId(TUtil.newQueryUnitAttemptId().getProto())
        .setProgress(0.5f)
        .setStatus(QueryStatus.QUERY_FINISHED);
    list.add(builder.build());
    
    StatusReport r1 = new StatusReportImpl(System.currentTimeMillis(),
        "testserver", 0, list);
    StatusReport r2 = new StatusReportImpl(r1.getProto());
    
    assertEquals(r1.getProto().getStatusCount(), r2.getProto().getStatusCount());
    assertEquals(r1.getProto(), r2.getProto());
    assertEquals(r1.getProgressList().size(), r2.getProgressList().size());
  }
}
