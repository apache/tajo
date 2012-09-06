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
import tajo.engine.MasterWorkerProtos.InProgressStatusProto;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.ipc.PingRequest;
import tajo.engine.query.PingRequestImpl;
import tajo.engine.utils.TUtil;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestPingRequestImpl {

  @Test
  public void test() {
    QueryIdFactory.reset();
    
    List<InProgressStatusProto> list
      = new ArrayList<InProgressStatusProto>();
    
    InProgressStatusProto.Builder builder = InProgressStatusProto.newBuilder()
        .setId(TUtil.newQueryUnitAttemptId().getProto())
        .setProgress(0.5f)
        .setStatus(QueryStatus.QUERY_FINISHED);
    list.add(builder.build());
    
    builder = InProgressStatusProto.newBuilder()
        .setId(TUtil.newQueryUnitAttemptId().getProto())
        .setProgress(0.5f)
        .setStatus(QueryStatus.QUERY_FINISHED);
    list.add(builder.build());
    
    PingRequest r1 = new PingRequestImpl(System.currentTimeMillis(),
        "testserver", list);
    PingRequest r2 = new PingRequestImpl(r1.getProto());
    
    assertEquals(r1.getProto().getStatusCount(), r2.getProto().getStatusCount());
    assertEquals(r1.getProto(), r2.getProto());
    assertEquals(r1.getProgressList().size(), r2.getProgressList().size());
  }
}
