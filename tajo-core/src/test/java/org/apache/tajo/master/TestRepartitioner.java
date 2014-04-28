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

package org.apache.tajo.master;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.TestTajoIds;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.querymaster.QueryUnit;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.FetchImpl;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

public class TestRepartitioner {
  @Test
  public void testCreateHashFetchURL() throws Exception {
    QueryId q1 = TestTajoIds.createQueryId(1315890136000l, 2);
    String hostName = "tajo1";
    int port = 1234;
    ExecutionBlockId sid = new ExecutionBlockId(q1, 2);
    int partitionId = 2;

    List<QueryUnit.IntermediateEntry> intermediateEntries = TUtil.newList();
    for (int i = 0; i < 1000; i++) {
      intermediateEntries.add(new QueryUnit.IntermediateEntry(i, 0, partitionId, new QueryUnit.PullHost(hostName, port)));
    }

    FetchImpl fetch = new FetchImpl(new QueryUnit.PullHost(hostName, port), TajoWorkerProtocol.ShuffleType.HASH_SHUFFLE,
        sid, partitionId, intermediateEntries);

    fetch.setName(sid.toString());

    TajoWorkerProtocol.FetchProto proto = fetch.getProto();
    fetch = new FetchImpl(proto);
    assertEquals(proto, fetch.getProto());
    List<URI> uris = fetch.getURIs();

    List<String> taList = TUtil.newList();
    for (URI uri : uris) {
      final Map<String, List<String>> params =
          new QueryStringDecoder(uri).getParameters();
      taList.addAll(splitMaps(params.get("ta")));
    }

    int checkTaskId = 0;
    for (String ta : taList) {
      assertEquals(checkTaskId++, Integer.parseInt(ta.split("_")[0]));
    }
  }

  private List<String> splitMaps(List<String> mapq) {
    if (null == mapq) {
      return null;
    }
    final List<String> ret = new ArrayList<String>();
    for (String s : mapq) {
      Collections.addAll(ret, s.split(","));
    }
    return ret;
  }
}
