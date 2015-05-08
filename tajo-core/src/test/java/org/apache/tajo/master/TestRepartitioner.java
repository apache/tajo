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
import org.apache.tajo.querymaster.Task;
import org.apache.tajo.querymaster.Task.IntermediateEntry;
import org.apache.tajo.querymaster.Repartitioner;
import org.apache.tajo.util.NumberUtil;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.FetchImpl;
import org.junit.Test;

import io.netty.handler.codec.http.QueryStringDecoder;

import java.net.URI;
import java.util.*;

import static junit.framework.Assert.assertEquals;
import static org.apache.tajo.querymaster.Repartitioner.FetchGroupMeta;
import static org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import static org.apache.tajo.plan.serder.PlanProto.ShuffleType.HASH_SHUFFLE;
import static org.apache.tajo.plan.serder.PlanProto.ShuffleType.SCATTERED_HASH_SHUFFLE;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestRepartitioner {
  @Test
  public void testCreateHashFetchURL() throws Exception {
    QueryId q1 = TestTajoIds.createQueryId(1315890136000l, 2);
    String hostName = "tajo1";
    int port = 1234;
    ExecutionBlockId sid = new ExecutionBlockId(q1, 2);
    int numPartition = 10;

    Map<Integer, List<IntermediateEntry>> intermediateEntries = new HashMap<Integer, List<IntermediateEntry>>();
    for (int i = 0; i < numPartition; i++) {
      intermediateEntries.put(i, new ArrayList<IntermediateEntry>());
    }
    for (int i = 0; i < 1000; i++) {
      int partitionId = i % numPartition;
      IntermediateEntry entry = new IntermediateEntry(i, 0, partitionId, new Task.PullHost(hostName, port));
      entry.setEbId(sid);
      entry.setVolume(10);
      intermediateEntries.get(partitionId).add(entry);
    }

    Map<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>> hashEntries =
        new HashMap<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>>();

    for (Map.Entry<Integer, List<IntermediateEntry>> eachEntry: intermediateEntries.entrySet()) {
      FetchImpl fetch = new FetchImpl(new Task.PullHost(hostName, port), ShuffleType.HASH_SHUFFLE,
          sid, eachEntry.getKey(), eachEntry.getValue());

      fetch.setName(sid.toString());

      TajoWorkerProtocol.FetchProto proto = fetch.getProto();
      fetch = new FetchImpl(proto);
      assertEquals(proto, fetch.getProto());

      Map<ExecutionBlockId, List<IntermediateEntry>> ebEntries = new HashMap<ExecutionBlockId, List<IntermediateEntry>>();
      ebEntries.put(sid, eachEntry.getValue());

      hashEntries.put(eachEntry.getKey(), ebEntries);

      List<URI> uris = fetch.getURIs();
      assertEquals(1, uris.size());   //In Hash Suffle, Fetcher return only one URI per partition.

      URI uri = uris.get(0);
      final Map<String, List<String>> params =
          new QueryStringDecoder(uri).parameters();

      assertEquals(eachEntry.getKey().toString(), params.get("p").get(0));
      assertEquals("h", params.get("type").get(0));
      assertEquals("" + sid.getId(), params.get("sid").get(0));
    }

    Map<Integer, Map<ExecutionBlockId, List<IntermediateEntry>>> mergedHashEntries =
        Repartitioner.mergeIntermediateByPullHost(hashEntries);

    assertEquals(numPartition, mergedHashEntries.size());
    for (int i = 0; i < numPartition; i++) {
      Map<ExecutionBlockId, List<IntermediateEntry>> eachEntry = mergedHashEntries.get(0);
      assertEquals(1, eachEntry.size());
      List<IntermediateEntry> interEntry = eachEntry.get(sid);
      assertEquals(1, interEntry.size());

      assertEquals(1000, interEntry.get(0).getVolume());
    }
  }

  @Test
  public void testScheduleFetchesByEvenDistributedVolumes() {
    Map<Integer, FetchGroupMeta> fetchGroups = Maps.newHashMap();
    String tableName = "test1";


    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    FetchImpl [] fetches = new FetchImpl[12];
    for (int i = 0; i < 12; i++) {
      fetches[i] = new FetchImpl(new Task.PullHost("localhost", 10000 + i), HASH_SHUFFLE, ebId, i / 2);
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

  @Test
  public void testMergeIntermediates() {
    //Test Merge
    List<IntermediateEntry> intermediateEntries = new ArrayList<IntermediateEntry>();

    int[] pageLengths = {10 * 1024 * 1024, 10 * 1024 * 1024, 10 * 1024 * 1024, 5 * 1024 * 1024};   //35 MB
    long expectedTotalLength = makeIntermediates(pageLengths, true, intermediateEntries);

    long splitVolume = 128 * 1024 * 1024;
    List<List<FetchImpl>> fetches = Repartitioner.splitOrMergeIntermediates(null, intermediateEntries,
        splitVolume, 10 * 1024 * 1024);
    assertEquals(6, fetches.size());

    int totalInterms = 0;
    int index = 0;
    int numZeroPosFetcher = 0;
    long totalLength = 0;
    for (List<FetchImpl> eachFetchList: fetches) {
      totalInterms += eachFetchList.size();
      long eachFetchVolume = 0;
      for (FetchImpl eachFetch: eachFetchList) {
        eachFetchVolume += eachFetch.getLength();
        if (eachFetch.getOffset() == 0) {
          numZeroPosFetcher++;
        }
        totalLength += eachFetch.getLength();
      }
      assertTrue(eachFetchVolume + " should be smaller than splitVolume", eachFetchVolume < splitVolume);
      if (index < fetches.size() - 1) {
        assertTrue(eachFetchVolume + " should be great than 100MB", eachFetchVolume >= 100 * 1024 * 1024);
      }
      index++;
    }
    assertEquals(23, totalInterms);
    assertEquals(20, numZeroPosFetcher);
    assertEquals(expectedTotalLength, totalLength);
  }

  private long makeIntermediates(int[] pageLengths, boolean uniqueHosts,
                                 List<IntermediateEntry> intermediateEntries) {
    long expectedTotalLength = 0;
    for (int i = 0; i < 20; i++) {
      NumberUtil.PrimitiveLongs pages = new NumberUtil.PrimitiveLongs(10);
      long offset = 0;
      for (int pageLength : pageLengths) {
        pages.add(offset);
        pages.add(pageLength);
        offset += pageLength;
        expectedTotalLength += pageLength;
      }
      IntermediateEntry interm = new IntermediateEntry(i, -1, -1,
          new Task.PullHost(uniqueHosts ? "" + i : "", uniqueHosts ? i : 0));
      interm.setPages(pages.toArray());
      interm.setVolume(offset);
      intermediateEntries.add(interm);
    }
    return expectedTotalLength;
  }

  @Test
  public void testSplitIntermediates() {
    List<IntermediateEntry> intermediateEntries = new ArrayList<IntermediateEntry>();

    int[] pageLengths = new int[20];  //195MB
    for (int i = 0 ; i < pageLengths.length; i++) {
      if (i < pageLengths.length - 1) {
        pageLengths[i] =  10 * 1024 * 1024;
      } else {
        pageLengths[i] =  5 * 1024 * 1024;
      }
    }

    long expectedTotalLength = makeIntermediates(pageLengths, true, intermediateEntries);

    long splitVolume = 128 * 1024 * 1024;
    List<List<FetchImpl>> fetches = Repartitioner.splitOrMergeIntermediates(null, intermediateEntries,
        splitVolume, 10 * 1024 * 1024);
    assertEquals(32, fetches.size());

    int index = 0;
    int numZeroPosFetcher = 0;
    long totalLength = 0;
    Set<String> uniqPullHost = new HashSet<String>();

    for (List<FetchImpl> eachFetchList: fetches) {
      long length = 0;
      for (FetchImpl eachFetch: eachFetchList) {
        if (eachFetch.getOffset() == 0) {
          numZeroPosFetcher++;
        }
        totalLength += eachFetch.getLength();
        length += eachFetch.getLength();
        uniqPullHost.add(eachFetch.getPullHost().toString());
      }
      assertTrue(length + " should be smaller than splitVolume", length < splitVolume);
      if (index < fetches.size() - 1) {
        assertTrue(length + " should be great than 100MB" + fetches.size() + "," + index, length >= 100 * 1024 * 1024);
      }
      index++;
    }
    assertEquals(20, numZeroPosFetcher);
    assertEquals(20, uniqPullHost.size());
    assertEquals(expectedTotalLength, totalLength);
  }

  @Test
  public void testSplitIntermediates2() {
    long[][] pageDatas = {
        {0, 10538717},
        {10538717, 10515884},
        {21054601, 10514343},
        {31568944, 10493988},
        {42062932, 10560639},
        {52623571, 10548486},
        {63172057, 10537811},
        {73709868, 10571060},
        {84280928, 10515062},
        {94795990, 10502964},
        {105298954, 10514011},
        {115812965, 10532154},
        {126345119, 10534133},
        {136879252, 10549749},
        {147429001, 10566547},
        {157995548, 10543700},
        {168539248, 10490324},
        {179029572, 10500720},
        {189530292, 10505425},
        {200035717, 10548418},
        {210584135, 10562887},
        {221147022, 10554967},
        {231701989, 10507297},
        {242209286, 10515612},
        {252724898, 10491274},
        {263216172, 10512956},
        {273729128, 10490736},
        {284219864, 10501878},
        {294721742, 10564568},
        {305286310, 10488896},
        {315775206, 10516308},
        {326291514, 10517965},
        {336809479, 10487038},
        {347296517, 10603472},
        {357899989, 10507330},
        {368407319, 10549429},
        {378956748, 10533443},
        {389490191, 10530852},
        {400021043, 11036431},
        {411057474, 10541007},
        {421598481, 10600477},
        {432198958, 10519805},
        {442718763, 10500769},
        {453219532, 10507192},
        {463726724, 10540424},
        {474267148, 10509129},
        {484776277, 10527100},
        {495303377, 10720789},
        {506024166, 10568542},
        {516592708, 11046886},
        {527639594, 10580358},
        {538219952, 10508940},
        {548728892, 10523968},
        {559252860, 10580626},
        {569833486, 10539361},
        {580372847, 10496662},
        {590869509, 10505280},
        {601374789, 10564655},
        {611939444, 10505842},
        {622445286, 10523889},
        {632969175, 10553186},
        {643522361, 10535866},
        {654058227, 10501796},
        {664560023, 10530358},
        {675090381, 10585340},
        {685675721, 10602017},
        {696277738, 10546614},
        {706824352, 10511511},
        {717335863, 11019221},
        {728355084, 10558143},
        {738913227, 10516245},
        {749429472, 10502613},
        {759932085, 10522145},
        {770454230, 10489373},
        {780943603, 10520973},
        {791464576, 11021218},
        {802485794, 10496362},
        {812982156, 10502354},
        {823484510, 10515932},
        {834000442, 10591044},
        {844591486, 5523957}
    };

    List<IntermediateEntry> entries = new ArrayList<IntermediateEntry>();
    for (int i = 0; i < 2; i++) {
      NumberUtil.PrimitiveLongs pages = new NumberUtil.PrimitiveLongs(10);
      for (long[] pageData : pageDatas) {
        pages.add(pageData);
      }
      IntermediateEntry entry = new IntermediateEntry(-1, -1, 1, new Task.PullHost("host" + i , 9000));
      entry.setPages(pages.toArray());

      entries.add(entry);
    }

    long splitVolume = 256 * 1024 * 1024;
    List<List<FetchImpl>> fetches = Repartitioner.splitOrMergeIntermediates(null, entries, splitVolume,
        10 * 1024 * 1024);


    long[][] expected = {
        {0,263216172},
        {263216172,264423422},
        {527639594,263824982},
        {791464576,58650867},
        {0,200035717},
        {200035717,263691007},
        {463726724,264628360},
        {728355084,121760359},
    };
    int index = 0;
    for (List<FetchImpl> eachFetchList: fetches) {
      if (index == 3) {
        assertEquals(2, eachFetchList.size());
      } else {
        assertEquals(1, eachFetchList.size());
      }
      for (FetchImpl eachFetch: eachFetchList) {
        assertEquals(expected[index][0], eachFetch.getOffset());
        assertEquals(expected[index][1], eachFetch.getLength());
        index++;
      }
    }
  }

  @Test
  public void testSplitIntermediatesWithUniqueHost() {
    List<IntermediateEntry> intermediateEntries = new ArrayList<IntermediateEntry>();

    int[] pageLengths = new int[20];  //195MB
    for (int i = 0 ; i < pageLengths.length; i++) {
      if (i < pageLengths.length - 1) {
        pageLengths[i] =  10 * 1024 * 1024;
      } else {
        pageLengths[i] =  5 * 1024 * 1024;
      }
    }

    long expectedTotalLength = makeIntermediates(pageLengths, false, intermediateEntries);

    long splitVolume = 128 * 1024 * 1024;
    List<List<FetchImpl>> fetches = Repartitioner.splitOrMergeIntermediates(null, intermediateEntries,
        splitVolume, 10 * 1024 * 1024);
    assertEquals(32, fetches.size());

    int expectedSize = 0;
    Set<FetchImpl> fetchSet = TUtil.newHashSet();
    for(List<FetchImpl> list : fetches){
      expectedSize += list.size();
      fetchSet.addAll(list);
    }
    assertEquals(expectedSize, fetchSet.size());


    int index = 0;
    int numZeroPosFetcher = 0;
    long totalLength = 0;
    Set<String> uniqPullHost = new HashSet<String>();

    for (List<FetchImpl> eachFetchList: fetches) {
      long length = 0;
      for (FetchImpl eachFetch: eachFetchList) {
        if (eachFetch.getOffset() == 0) {
          numZeroPosFetcher++;
        }
        totalLength += eachFetch.getLength();
        length += eachFetch.getLength();
        uniqPullHost.add(eachFetch.getPullHost().toString());
      }
      assertTrue(length + " should be smaller than splitVolume", length < splitVolume);
      if (index < fetches.size() - 1) {
        assertTrue(length + " should be great than 100MB" + fetches.size() + "," + index, length >= 100 * 1024 * 1024);
      }
      index++;
    }
    assertEquals(20, numZeroPosFetcher);
    assertEquals(1, uniqPullHost.size());
    assertEquals(expectedTotalLength, totalLength);
  }

  @Test
  public void testFetchImpl() {
    ExecutionBlockId ebId = new ExecutionBlockId(LocalTajoTestingUtility.newQueryId(), 0);
    Task.PullHost pullHost = new Task.PullHost("localhost", 0);

    FetchImpl expected = new FetchImpl(pullHost, SCATTERED_HASH_SHUFFLE, ebId, 1);
    FetchImpl fetch2 = new FetchImpl(pullHost, SCATTERED_HASH_SHUFFLE, ebId, 1);
    assertEquals(expected, fetch2);
    fetch2.setOffset(5);
    fetch2.setLength(10);
    assertNotEquals(expected, fetch2);
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
