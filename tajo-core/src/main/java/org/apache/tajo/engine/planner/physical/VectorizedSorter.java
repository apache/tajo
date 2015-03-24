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

import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

/**
 * Extract raw level values (primitive or String/byte[]) from each of key columns before sorting
 * Uses indirection for efficient swapping
 */
public class VectorizedSorter implements IndexedSortable, TupleSorter {

  private final Tuple[] tuples;         // source tuples
  private final TupleVector[] vectors;  // values of key columns
  private final int[] mappings;         // index indirection

  public VectorizedSorter(List<Tuple> source, SortSpec[] sortKeys, int[] keyIndex) {
    this.tuples = source.toArray(new Tuple[source.size()]);
    vectors = new TupleVector[sortKeys.length];
    mappings = new int[tuples.length];
    for (int i = 0; i < vectors.length; i++) {
      TajoDataTypes.Type type = sortKeys[i].getSortKey().getDataType().getType();
      boolean nullFirst = sortKeys[i].isNullFirst();
      boolean ascending = sortKeys[i].isAscending();
      boolean nullInvert = nullFirst && ascending || !nullFirst && !ascending;
      vectors[i] = new TupleVector(TupleVector.getType(type), tuples.length, nullInvert, ascending);
    }
    for (int i = 0; i < tuples.length; i++) {
      for (int j = 0; j < keyIndex.length; j++) {
        vectors[j].add(tuples[i].get(keyIndex[j]));
      }
      mappings[i] = i;
    }
  }

  @Override
  public int compare(int i1, int i2) {
    final int index1 = mappings[i1];
    final int index2 = mappings[i2];
    for (TupleVector vector : vectors) {
      int compare = vector.compare(index1, index2);
      if (compare != 0) {
        return compare;
      }
    }
    return 0;
  }

  @Override
  public void swap(int i1, int i2) {
    int v1 = mappings[i1];
    mappings[i1] = mappings[i2];
    mappings[i2] = v1;
  }

  @Override
  public Iterator<Tuple> sort() {
    new QuickSort().sort(VectorizedSorter.this, 0, mappings.length);
    return new Iterator<Tuple>() {
      int index;
      public boolean hasNext() { return index < mappings.length; }
      public Tuple next() { return tuples[mappings[index++]]; }
      public void remove() { throw new UnsupportedException(); }
    };
  }

  private static class TupleVector {

    private final int type;
    private final BitSet nulls;
    private final boolean nullInvert;
    private final boolean ascending;

    private boolean[] booleans;
    private byte[] bits;
    private short[] shorts;
    private int[] ints;
    private long[] longs;
    private float[] floats;
    private double[] doubles;
    private byte[][] bytes;

    private int index;

    private TupleVector(int type, int length, boolean nullInvert, boolean ascending) {
      this.type = type;
      this.nulls = new BitSet(length);
      this.nullInvert = nullInvert;
      this.ascending = ascending;
      switch (type) {
        case 0: booleans = new boolean[length]; break;
        case 1: bits = new byte[length]; break;
        case 2: shorts = new short[length]; break;
        case 3: ints = new int[length]; break;
        case 4: longs = new long[length]; break;
        case 5: floats = new float[length]; break;
        case 6: doubles = new double[length]; break;
        case 7: bytes = new byte[length][]; break;
        default:
          throw new IllegalArgumentException();
      }
    }

    private void add(Datum datum) {
      if (datum.isNull()) {
        nulls.set(index++);
        return;
      }
      switch (type) {
        case 0: booleans[index] = datum.asBool(); break;
        case 1: bits[index] = datum.asByte(); break;
        case 2: shorts[index] = datum.asInt2(); break;
        case 3: ints[index] = datum.asInt4(); break;
        case 4: longs[index] = datum.asInt8(); break;
        case 5: floats[index] = datum.asFloat4(); break;
        case 6: doubles[index] = datum.asFloat8(); break;
        case 7: bytes[index] = datum.asByteArray(); break;
        default:
          throw new IllegalArgumentException();
      }
      index++;
    }

    private int compare(int index1, int index2) {
      final boolean n1 = nulls.get(index1);
      final boolean n2 = nulls.get(index2);
      if (n1 && n2) {
        return 0;
      }
      if (n1 ^ n2) {
        int compVal = n1 ? 1 : -1;
        return nullInvert ? -compVal : compVal;
      }
      int compare;
      switch (type) {
        case 0: compare = Booleans.compare(booleans[index1], booleans[index2]); break;
        case 1: compare = bits[index1] - bits[index2]; break;
        case 2: compare = Shorts.compare(shorts[index1], shorts[index2]); break;
        case 3: compare = Ints.compare(ints[index1], ints[index2]); break;
        case 4: compare = Longs.compare(longs[index1], longs[index2]); break;
        case 5: compare = Floats.compare(floats[index1], floats[index2]); break;
        case 6: compare = Doubles.compare(doubles[index1], doubles[index2]); break;
        case 7: compare = TextDatum.COMPARATOR.compare(bytes[index1], bytes[index2]); break;
        default:
          throw new IllegalArgumentException();
      }
      return ascending ? compare : -compare;
    }

    public static int getType(TajoDataTypes.Type type) {
      switch (type) {
        case BOOLEAN: return 0;
        case BIT: case INT1: return 1;
        case INT2: return 2;
        case INT4: case DATE: case INET4: return 3;
        case INT8: case TIME: case TIMESTAMP: case INTERVAL: return 4;
        case FLOAT4: return 5;
        case FLOAT8: return 6;
        case TEXT: case CHAR: case BLOB: return 7;
      }
      // todo
      throw new UnsupportedException(type.name());
    }
  }
}
