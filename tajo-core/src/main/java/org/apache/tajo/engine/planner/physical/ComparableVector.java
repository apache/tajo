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
import com.google.common.primitives.UnsignedInts;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Extract raw level values (primitive or String/byte[]) from each of key columns for compare
 */
public class ComparableVector {

  protected final Tuple[] tuples;         // source tuples
  protected final TupleVector[] vectors;  // values of key columns
  protected final int[] keyIndex;

  public ComparableVector(int length, SortSpec[] sortKeys, int[] keyIndex) {
    tuples = new Tuple[length];
    vectors = new TupleVector[sortKeys.length];
    for (int i = 0; i < vectors.length; i++) {
      TajoDataTypes.Type type = sortKeys[i].getSortKey().getDataType().getType();
      boolean nullFirst = sortKeys[i].isNullFirst();
      boolean ascending = sortKeys[i].isAscending();
      boolean nullInvert = nullFirst && ascending || !nullFirst && !ascending;
      vectors[i] = new TupleVector(getType(type), tuples.length, nullInvert, ascending);
    }
    this.keyIndex = keyIndex;
  }

  public int compare(final int i1, final int i2) {
    for (TupleVector vector : vectors) {
      int compare = vector.compare(i1, i2);
      if (compare != 0) {
        return compare;
      }
    }
    return 0;
  }

  public void set(int index, Tuple tuple) {
    for (int i = 0; i < vectors.length; i++) {
      vectors[i].set(index, tuple, keyIndex[i]);
    }
  }

  protected static class TupleVector {

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
        case 8: ints = new int[length]; break;
        default:
          throw new IllegalArgumentException();
      }
    }

    protected final void append(Tuple tuple, int field) {
      set(index++, tuple, field);
    }

    protected final void set(int index, Tuple tuple, int field) {
      if (tuple.isNull(field)) {
        nulls.set(index);
        return;
      }
      nulls.clear(index);
      switch (type) {
        case 0: booleans[index] = tuple.getBool(field); break;
        case 1: bits[index] = tuple.getByte(field); break;
        case 2: shorts[index] = tuple.getInt2(field); break;
        case 3: ints[index] = tuple.getInt4(field); break;
        case 4: longs[index] = tuple.getInt8(field); break;
        case 5: floats[index] = tuple.getFloat4(field); break;
        case 6: doubles[index] = tuple.getFloat8(field); break;
        case 7: bytes[index] = tuple.getBytes(field); break;
        case 8: ints[index] = tuple.getInt4(field); break;
        default:
          throw new IllegalArgumentException();
      }
    }

    protected final int compare(int index1, int index2) {
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
        case 8: compare = UnsignedInts.compare(ints[index1], ints[index2]); break;
        default:
          throw new IllegalArgumentException();
      }
      return ascending ? compare : -compare;
    }
  }

  public static class ComparableTuple {

    private final int[] keyTypes;
    private final int[] keyIndex;
    private final Object[] keys;

    public ComparableTuple(int[] keyTypes, int[] keyIndex) {
      this.keyTypes = new int[keyIndex.length];
      this.keyIndex = keyIndex;
      this.keys = new Object[keyIndex.length];
    }

    public void set(Tuple tuple) {
      for (int i = 0; i < keyTypes.length; i++) {
        final int field = keyIndex[i];
        if (tuple.isNull(field)) {
          keys[i] = null;
          continue;
        }
        switch (keyTypes[i]) {
          case 0: keys[i] = tuple.getBool(field); break;
          case 1: keys[i] = tuple.getByte(field); break;
          case 2: keys[i] = tuple.getInt2(field); break;
          case 3: keys[i] = tuple.getInt4(field); break;
          case 4: keys[i] = tuple.getInt8(field); break;
          case 5: keys[i] = tuple.getFloat4(field); break;
          case 6: keys[i] = tuple.getFloat8(field); break;
          case 7: keys[i] = tuple.getBytes(field); break;
          case 8: keys[i] = tuple.getInt4(field); break;
          default:
            throw new IllegalArgumentException();
          }
      }
    }

    @Override
    public boolean equals(Object obj) {
      ComparableTuple other = (ComparableTuple)obj;
      for (int i = 0; i < keys.length; i++) {
        final boolean n1 = keys[i] == null;
        final boolean n2 = other.keys[i] == null;
        if (n1 && n2) {
          continue;
        }
        if (n1 ^ n2) {
          return false;
        }
        if (keyTypes[i] == 7 && !Arrays.equals((byte[])keys[i], (byte[])other.keys[i])) {
          return false;
        }
        if (!keys[i].equals(other.keys[i])) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(keys);
    }

    public ComparableTuple copy() {
      ComparableTuple copy = new ComparableTuple(keyTypes, keyIndex);
      System.arraycopy(keys, 0, copy.keys, 0, keys.length);
      return copy;
    }
  }

  public static boolean isApplicable(SortSpec[] sortKeys) {
    if (sortKeys.length == 0) {
      return false;
    }
    for (SortSpec spec : sortKeys) {
      try {
        getType(spec.getSortKey().getDataType().getType());
      } catch (Exception e) {
        return false;
      }
    }
    return true;
  }

  public static int getType(TajoDataTypes.Type type) {
    switch (type) {
      case BOOLEAN: return 0;
      case BIT: case INT1: return 1;
      case INT2: return 2;
      case INT4: case DATE: return 3;
      case INT8: case TIME: case TIMESTAMP: case INTERVAL: return 4;
      case FLOAT4: return 5;
      case FLOAT8: return 6;
      case TEXT: case CHAR: case BLOB: return 7;
      case INET4: return 8;
    }
    // todo
    throw new UnsupportedException(type.name());
  }

  public static int[] toTypes(Schema schema, int[] keyIndex) {
    int[] types = new int[keyIndex.length];
    for (int i = 0; i < keyIndex.length; i++) {
      types[i] = getType(schema.getColumn(keyIndex[i]).getDataType().getType());
    }
    return types;
  }
}
