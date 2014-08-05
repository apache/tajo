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

import com.google.common.collect.Maps;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.TestTajoIds;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.querymaster.QueryUnit;
import org.apache.tajo.master.querymaster.Repartitioner;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.FetchImpl;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.apache.tajo.master.querymaster.Repartitioner.FetchGroupMeta;
import static org.junit.Assert.assertTrue;

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
  }

//  private List<String> splitMaps(List<String> mapq) {
//    if (null == mapq) {
//      return null;
//    }
//    final List<String> ret = new ArrayList<String>();
//    for (String s : mapq) {
//      Collections.addAll(ret, s.split(","));
//    }
//    return ret;
//  }

  @Test
  public void testScheduleFetchesByEvenDistributedVolumes() {
    Map<Integer, FetchGroupMeta> fetchGroups = Maps.newHashMap();
    String tableName = "test1";


    fetchGroups.put(0, new FetchGroupMeta(100, new FetchImpl()));
    fetchGroups.put(1, new FetchGroupMeta(80, new FetchImpl()));
    fetchGroups.put(2, new FetchGroupMeta(70, new FetchImpl()));
    fetchGroups.put(3, new FetchGroupMeta(30, new FetchImpl()));
    fetchGroups.put(4, new FetchGroupMeta(10, new FetchImpl()));
    fetchGroups.put(5, new FetchGroupMeta(5, new FetchImpl()));

    Pair<Long [], Map<String, List<FetchImpl>>[]> results;

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 1);
    long expected [] = {100 + 80 + 70 + 30 + 10 + 5};
    assertFetchVolumes(expected, results.getFirst());

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 2);
    long expected0 [] = {130, 165};
    assertFetchVolumes(expected0, results.getFirst());

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 3);
    long expected1 [] = {100, 95, 100};
    assertFetchVolumes(expected1, results.getFirst());

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 4);
    long expected2 [] = {100, 80, 70, 45};
    assertFetchVolumes(expected2, results.getFirst());

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 5);
    long expected3 [] = {100, 80, 70, 30, 15};
    assertFetchVolumes(expected3, results.getFirst());

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 6);
    long expected4 [] = {100, 80, 70, 30, 10, 5};
    assertFetchVolumes(expected4, results.getFirst());
  }

  private static void assertFetchVolumes(long [] expected, Long [] results) {
    assertEquals("the lengths of volumes are mismatch", expected.length, results.length);

    for (int i = 0; i < expected.length; i++) {
      assertTrue(expected[i] + " is expected, but " + results[i], expected[i] == results[i]);
    }
  }
}
