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
import com.google.common.collect.Sets;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.QueryId;
import org.apache.tajo.TestTajoIds;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.querymaster.QueryUnit;
import org.apache.tajo.master.querymaster.Repartitioner;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.FetchImpl;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.junit.Test;

import java.net.URI;
import java.util.*;

import static junit.framework.Assert.assertEquals;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType;
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

    FetchImpl fetch = new FetchImpl(new QueryUnit.PullHost(hostName, port), ShuffleType.HASH_SHUFFLE,
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

  @Test
  public void testScheduleFetchesByEvenDistributedVolumes() {
    Map<Integer, FetchGroupMeta> fetchGroups = Maps.newHashMap();
    String tableName = "test1";


    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    FetchImpl [] fetches = new FetchImpl[12];
    for (int i = 0; i < 12; i++) {
      fetches[i] = new FetchImpl(new QueryUnit.PullHost("localhost", 10000 + i), ShuffleType.HASH_SHUFFLE, ebId, i / 2);
    }

    int [] VOLUMES = {100, 80, 70, 30, 10, 5};

    for (int i = 0; i < 12; i += 2) {
      fetchGroups.put(i, new FetchGroupMeta(VOLUMES[i / 2], fetches[i]).addFetche(fetches[i + 1]));
    }

    Pair<Long [], Map<String, List<FetchImpl>>[]> results;

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 1);
    long expected [] = {100 + 80 + 70 + 30 + 10 + 5};
    assertFetchVolumes(expected, results.getFirst());
    assertFetchImpl(fetches, results.getSecond());

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 2);
    long expected0 [] = {130, 165};
    assertFetchVolumes(expected0, results.getFirst());
    assertFetchImpl(fetches, results.getSecond());

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 3);
    long expected1 [] = {100, 95, 100};
    assertFetchVolumes(expected1, results.getFirst());
    assertFetchImpl(fetches, results.getSecond());

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 4);
    long expected2 [] = {100, 80, 70, 45};
    assertFetchVolumes(expected2, results.getFirst());
    assertFetchImpl(fetches, results.getSecond());

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 5);
    long expected3 [] = {100, 80, 70, 30, 15};
    assertFetchVolumes(expected3, results.getFirst());
    assertFetchImpl(fetches, results.getSecond());

    results = Repartitioner.makeEvenDistributedFetchImpl(fetchGroups, tableName, 6);
    long expected4 [] = {100, 80, 70, 30, 10, 5};
    assertFetchVolumes(expected4, results.getFirst());
    assertFetchImpl(fetches, results.getSecond());
  }

  private static void assertFetchVolumes(long [] expected, Long [] results) {
    assertEquals("the lengths of volumes are mismatch", expected.length, results.length);

    for (int i = 0; i < expected.length; i++) {
      assertTrue(expected[i] + " is expected, but " + results[i], expected[i] == results[i]);
    }
  }

  private static void assertFetchImpl(FetchImpl [] expected, Map<String, List<FetchImpl>>[] result) {
    Set<FetchImpl> expectedURLs = Sets.newHashSet();

    for (FetchImpl f : expected) {
      expectedURLs.add(f);
    }

    Set<FetchImpl> resultURLs = Sets.newHashSet();

    for (Map<String, List<FetchImpl>> e : result) {
      for (List<FetchImpl> list : e.values()) {
        resultURLs.addAll(list);
      }
    }

    assertEquals(expectedURLs.size(), resultURLs.size());
    assertEquals(expectedURLs, resultURLs);
  }
}
