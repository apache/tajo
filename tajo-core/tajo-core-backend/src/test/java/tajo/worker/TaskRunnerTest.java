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

package tajo.worker;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.BuilderUtils;
import tajo.QueryConf;
import tajo.QueryId;
import tajo.SubQueryId;
import tajo.TestQueryUnitId;
import tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService;
import tajo.rpc.ProtoAsyncRpcClient;
import tajo.util.TajoIdUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskRunnerTest {
  long ts1 = 1315890136000l;
  QueryId q1 = TestQueryUnitId.createQueryId(ts1, 2, 5);
  SubQueryId sq1 = TajoIdUtils.createSubQueryId(q1, 5);

  //@Test
  public void testInit() throws Exception {
    ProtoAsyncRpcClient mockClient = mock(ProtoAsyncRpcClient.class);
    mockClient.close();

    MasterWorkerProtocolService.Interface mockMaster =
        mock(MasterWorkerProtocolService.Interface.class);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        q1.getApplicationId(), q1.getAttemptId());
    ContainerId cId = BuilderUtils.newContainerId(appAttemptId, 1);

    NodeId nodeId = RecordFactoryProvider.getRecordFactory(null).
        newRecordInstance(NodeId.class);
    nodeId.setHost("host1");
    nodeId.setPort(9001);
    UserGroupInformation mockTaskOwner = mock(UserGroupInformation.class);
    when(mockTaskOwner.getShortUserName()).thenReturn("hyunsik");
    TaskRunner runner = new TaskRunner(sq1, nodeId, mockTaskOwner, mockClient,
        mockMaster, cId);
    QueryConf conf = new QueryConf();
    conf.setOutputPath(new Path("/tmp/" + q1));
    runner.init(conf);
    runner.start();
    runner.stop();
  }
}
