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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHashPartitioner {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public final void testGetPartition() {   
    Tuple tuple1 = new VTuple(3);    
    tuple1.put(new Datum[] {
        DatumFactory.createInt4(1),
        DatumFactory.createInt4(2),
        DatumFactory.createInt4(3)
    });
    Tuple tuple2 = new VTuple(3);    
    tuple2.put(new Datum[] {
        DatumFactory.createInt4(1),
        DatumFactory.createInt4(2),
        DatumFactory.createInt4(4)
    });
    Tuple tuple3 = new VTuple(3);    
    tuple3.put(new Datum[] {
        DatumFactory.createInt4(1),
        DatumFactory.createInt4(2),
        DatumFactory.createInt4(5)
    });
    Tuple tuple4 = new VTuple(3);    
    tuple4.put(new Datum[] {
        DatumFactory.createInt4(2),
        DatumFactory.createInt4(2),
        DatumFactory.createInt4(3)
    });
    Tuple tuple5 = new VTuple(3);    
    tuple5.put(new Datum[] {
        DatumFactory.createInt4(2),
        DatumFactory.createInt4(2),
        DatumFactory.createInt4(4)
    });
    
    int [] partKeys = {0,1};
    Partitioner p = new HashPartitioner(partKeys, 2);
    
    int part1 = p.getPartition(tuple1);
    assertEquals(part1, p.getPartition(tuple2));
    assertEquals(part1, p.getPartition(tuple3));
    
    int part2 = p.getPartition(tuple4);
    assertEquals(part2, p.getPartition(tuple5));    
  }

  @Test
  public final void testGetPartition2() {
    // https://issues.apache.org/jira/browse/TAJO-976
    Random rand = new Random();
    String[][] data = new String[1000][];

    for (int i = 0; i < 1000; i++) {
      data[i] = new String[]{ String.valueOf(rand.nextInt(1000)), String.valueOf(rand.nextInt(1000)), String.valueOf(rand.nextInt(1000))};
    }

    int[] testNumPartitions = new int[]{31, 62, 124, 32, 63, 125};
    for (int index = 0; index <  testNumPartitions.length; index++) {
      Partitioner p = new HashPartitioner(new int[]{0, 1, 2}, testNumPartitions[index]);

      Set<Integer> ids = new TreeSet<Integer>();
      for (int i = 0; i < data.length; i++) {
        Tuple tuple = new VTuple(
            new Datum[]{new TextDatum(data[i][0]), new TextDatum(data[i][1]), new TextDatum(data[i][2])});

        ids.add(p.getPartition(tuple));
      }

      // The number of partitions isn't exactly matched.
      assertTrue(ids.size() + 5 >= testNumPartitions[index]);
    }
  }
}
