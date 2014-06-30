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

package org.apache.tajo.engine.util;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.utils.TupleCache;
import org.apache.tajo.engine.utils.TupleCacheKey;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;

public class TestTupleCache {
  @Test
  public void testTupleCcaheBasicFunction() throws Exception {
    List<Tuple> tupleData = new ArrayList<Tuple>();
    for (int i = 0; i < 100; i++) {
      Datum[] datums = new Datum[5];
      for (int j = 0; j < 5; j++) {
        datums[j] = new TextDatum(i + "_" + j);
      }
      Tuple tuple = new VTuple(datums);
      tupleData.add(tuple);
    }

    ExecutionBlockId ebId = QueryIdFactory.newExecutionBlockId(
        QueryIdFactory.newQueryId(System.currentTimeMillis(), 0));

    TupleCacheKey cacheKey = new TupleCacheKey(ebId.toString(), "TestTable", "test");
    TupleCache tupleCache = TupleCache.getInstance();

    assertFalse(tupleCache.isBroadcastCacheReady(cacheKey));
    assertTrue(tupleCache.lockBroadcastScan(cacheKey));
    assertFalse(tupleCache.lockBroadcastScan(cacheKey));

    tupleCache.addBroadcastCache(cacheKey, tupleData);
    assertTrue(tupleCache.isBroadcastCacheReady(cacheKey));

    Scanner scanner = tupleCache.openCacheScanner(cacheKey, null);
    assertNotNull(scanner);

    int count = 0;

    while (true) {
      Tuple tuple = scanner.next();
      if (tuple == null) {
        break;
      }

      assertEquals(tupleData.get(count), tuple);
      count++;
    }

    assertEquals(tupleData.size(), count);

    tupleCache.removeBroadcastCache(ebId);
    assertFalse(tupleCache.isBroadcastCacheReady(cacheKey));
    assertTrue(tupleCache.lockBroadcastScan(cacheKey));

    tupleCache.removeBroadcastCache(ebId);
  }
}
