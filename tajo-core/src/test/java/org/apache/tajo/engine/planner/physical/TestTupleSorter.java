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

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.BaseTupleComparator;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;

public class TestTupleSorter {

  private static final Log LOG = LogFactory.getLog(TestTupleSorter.class);

  private static final Random rnd = new Random(-1);

  @Test
  public final void testSortBench() {
    final int MAX_SORT_KEY = 3;
    final int ITERATION = 10;
    final int LENGTH = 1000000;
    final int SAMPLING = 100;

    Tuple[] tuples = new Tuple[LENGTH];
    for (int i = 0; i < LENGTH; i++) {
      Datum[] datums = new Datum[]{
          DatumFactory.createInt4(rnd.nextInt(Short.MAX_VALUE)),
          DatumFactory.createInt4(rnd.nextInt()),
          DatumFactory.createText("dept_" + rnd.nextInt()),
          DatumFactory.createBool(rnd.nextBoolean()),
          DatumFactory.createInt8(rnd.nextLong()),
          DatumFactory.createInterval(rnd.nextInt(), rnd.nextLong())};
      tuples[i] = new VTuple(datums);
    }

    Column col0 = new Column("col0", Type.INT2);
    Column col1 = new Column("col1", Type.INT4);
    Column col2 = new Column("col2", Type.TEXT);
    Column col3 = new Column("col3", Type.BOOLEAN);
    Column col4 = new Column("col4", Type.INT8);
    Column col5 = new Column("col5", Type.INTERVAL);

    Schema schema = new Schema(new Column[] {col0, col1, col2, col3, col4, col5});

    long[] time1 = new long[ITERATION];
    long[] time2 = new long[ITERATION];
    for(int iteration = 0; iteration < ITERATION; iteration++) {
      List<Tuple> target = Arrays.asList(Arrays.copyOf(tuples, tuples.length));
      Set<Integer> keys = new TreeSet<Integer>();
      for (int i = 0; i < MAX_SORT_KEY; i++) {
        keys.add(rnd.nextInt(schema.size()));
      }
      int[] keyIndices = Ints.toArray(keys);
      SortSpec[] sortKeys = new SortSpec[keyIndices.length];
      for (int i = 0; i < keyIndices.length; i++) {
        sortKeys[i] = new SortSpec(schema.getColumn(keyIndices[i]), rnd.nextBoolean(), rnd.nextBoolean());
      }

      long start = System.currentTimeMillis();
      VectorizedSorter sorter = new VectorizedSorter(target, sortKeys, keyIndices);
      Iterator<Tuple> iterator = sorter.sort().iterator();

      String[] result1 = new String[SAMPLING];
      for (int i = 0; i < result1.length; i++) {
        Tuple tuple = Iterators.get(iterator, LENGTH / result1.length - 1);
        StringBuilder builder = new StringBuilder();
        for (int keyIndex : keyIndices) {
          builder.append(tuple.get(keyIndex).asChars());
        }
        result1[i] = builder.toString();
      }
      time1[iteration] = System.currentTimeMillis() - start;

      BaseTupleComparator comparator = new BaseTupleComparator(schema, sortKeys);

      start = System.currentTimeMillis();
      Collections.sort(target, comparator);
      iterator = target.iterator();

      String[] result2 = new String[SAMPLING];
      for (int i = 0; i < result2.length; i++) {
        Tuple tuple = Iterators.get(iterator, LENGTH / result2.length - 1);
        StringBuilder builder = new StringBuilder();
        for (int keyIndex : keyIndices) {
          builder.append(tuple.get(keyIndex).asChars());
        }
        result2[i] = builder.toString();
      }
      time2[iteration] = System.currentTimeMillis() - start;

      LOG.info("Sort on keys " + Arrays.toString(keyIndices) +
          ": Vectorized " + time1[iteration]+ " msec, Original " + time2[iteration] + " msec");

      assertArrayEquals(result1, result2);
    }
  }
}
