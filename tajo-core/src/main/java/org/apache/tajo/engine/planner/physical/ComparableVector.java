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
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

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
      vectors[i] = new TupleVector(vectorType(type), tuples.length, nullInvert, ascending);
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
        case -1: break;
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

    private final TupleType[] keyTypes;
    private final int[] keyIndex;
    private final Object[] keys;

    public ComparableTuple(Schema schema, int[] keyIndex) {
      this(tupleTypes(schema, keyIndex), keyIndex);
    }

    public ComparableTuple(Schema schema, int start, int end) {
      this(schema, toKeyIndex(start, end));
    }

    private ComparableTuple(TupleType[] keyTypes, int[] keyIndex) {
      this.keyTypes = keyTypes;
      this.keyIndex = keyIndex;
      this.keys = new Object[keyIndex.length];
    }

    public int size() {
      return keyIndex.length;
    }

    public void set(Tuple tuple) {
      for (int i = 0; i < keyTypes.length; i++) {
        final int field = keyIndex[i];
        if (tuple.isNull(field)) {
          keys[i] = null;
          continue;
        }
        switch (keyTypes[i]) {
          case BOOLEAN: keys[i] = tuple.getBool(field); break;
          case BIT: keys[i] = tuple.getByte(field); break;
          case INT1:
          case INT2: keys[i] = tuple.getInt2(field); break;
          case INT4:
          case DATE:
          case INET4: keys[i] = tuple.getInt4(field); break;
          case INT8:
          case TIME:
          case TIMESTAMP: keys[i] = tuple.getInt8(field); break;
          case FLOAT4: keys[i] = tuple.getFloat4(field); break;
          case FLOAT8: keys[i] = tuple.getFloat8(field); break;
          case TEXT:
          case CHAR:
          case BLOB: keys[i] = tuple.getBytes(field); break;
          case DATUM: keys[i] = tuple.get(field); break;
          default:
            throw new IllegalArgumentException();
        }
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ComparableTuple) {
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
          switch (keyTypes[i]) {
            case TEXT:
            case CHAR:
            case BLOB: if (!Arrays.equals((byte[])keys[i], (byte[])other.keys[i])) return false; continue;
            default: if (!keys[i].equals(other.keys[i])) return false; continue;
          }
        }
        return true;
      }
      return false;
    }

    public boolean equals(Tuple tuple) {
      for (int i = 0; i < keys.length; i++) {
        final int field = keyIndex[i];
        final boolean n1 = keys[i] == null;
        final boolean n2 = tuple.isNull(field);
        if (n1 && n2) {
          continue;
        }
        if (n1 ^ n2) {
          return false;
        }
        switch (keyTypes[i]) {
          case BOOLEAN: if ((Boolean)keys[i] != tuple.getBool(field)) return false; continue;
          case BIT: if ((Byte)keys[i] != tuple.getByte(field)) return false; continue;
          case INT1:
          case INT2: if ((Short)keys[i] != tuple.getInt2(field)) return false; continue;
          case INT4:
          case DATE:
          case INET4: if ((Integer)keys[i] != tuple.getInt4(field)) return false; continue;
          case INT8:
          case TIME:
          case TIMESTAMP: if ((Long)keys[i] != tuple.getInt8(field)) return false; continue;
          case FLOAT4: if ((Float)keys[i] != tuple.getFloat4(field)) return false; continue;
          case FLOAT8: if ((Double)keys[i] != tuple.getFloat8(field)) return false; continue;
          case TEXT:
          case CHAR:
          case BLOB: if (!Arrays.equals((byte[])keys[i], tuple.getBytes(field))) return false; continue;
          case DATUM: if (!keys[i].equals(tuple.get(field))) return false; continue;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      int result = 1;
      for (Object key : keys) {
        int hash = key == null ? 0 :
            key instanceof byte[] ? Arrays.hashCode((byte[])key) : key.hashCode();
        result = 31 * result + hash;
      }
      return result;
    }

    public ComparableTuple copy() {
      ComparableTuple copy = emptyCopy();
      System.arraycopy(keys, 0, copy.keys, 0, keys.length);
      return copy;
    }

    public ComparableTuple emptyCopy() {
      return new ComparableTuple(keyTypes, keyIndex);
    }

    public VTuple toVTuple() {
      VTuple vtuple = new VTuple(keyIndex.length);
      for (int i = 0; i < keyIndex.length; i++) {
        vtuple.put(i, toDatum(i));
      }
      return vtuple;
    }

    public Datum toDatum(int i) {
      if (keys[i] == null) {
        return NullDatum.get();
      }
      switch (keyTypes[i]) {
        case NULL_TYPE: return NullDatum.get();
        case BOOLEAN: return DatumFactory.createBool((Boolean) keys[i]);
        case BIT: return DatumFactory.createBit((Byte)keys[i]);
        case INT1:
        case INT2: return DatumFactory.createInt2((Short) keys[i]);
        case INT4: return DatumFactory.createInt4((Integer) keys[i]);
        case DATE: return DatumFactory.createDate((Integer) keys[i]);
        case INET4: return DatumFactory.createInet4((Integer) keys[i]);
        case INT8: return DatumFactory.createInt8((Long) keys[i]);
        case TIME: return DatumFactory.createTime((Long) keys[i]);
        case TIMESTAMP: return DatumFactory.createTimestamp((Long) keys[i]);
        case FLOAT4: return DatumFactory.createFloat4((Float) keys[i]);
        case FLOAT8: return DatumFactory.createFloat8((Double) keys[i]);
        case TEXT: return DatumFactory.createText((byte[]) keys[i]);
        case CHAR: return DatumFactory.createChar((byte[]) keys[i]);
        case BLOB: return DatumFactory.createBlob((byte[]) keys[i]);
        case DATUM: return (Datum)keys[i];
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  public static boolean isVectorizable(SortSpec[] sortKeys) {
    if (sortKeys.length == 0) {
      return false;
    }
    for (SortSpec spec : sortKeys) {
      try {
        vectorType(spec.getSortKey().getDataType().getType());
      } catch (Exception e) {
        return false;
      }
    }
    return true;
  }

  private static int vectorType(TajoDataTypes.Type type) {
    switch (type) {
      case BOOLEAN: return 0;
      case BIT: return 1;
      case INT1: case INT2: return 2;
      case INT4: case DATE: return 3;
      case INT8: case TIME: case TIMESTAMP: case INTERVAL: return 4;
      case FLOAT4: return 5;
      case FLOAT8: return 6;
      case TEXT: case CHAR: case BLOB: return 7;
      case INET4: return 8;
      case NULL_TYPE: return -1;
    }
    // todo
    throw new UnsupportedException(type.name());
  }

  private static TupleType[] tupleTypes(Schema schema, int[] keyIndex) {
    TupleType[] types = new TupleType[keyIndex.length];
    for (int i = 0; i < keyIndex.length; i++) {
      types[i] = tupleType(schema.getColumn(keyIndex[i]).getDataType().getType());
    }
    return types;
  }

  private static TupleType tupleType(TajoDataTypes.Type type) {
    switch (type) {
      case BOOLEAN: return TupleType.BOOLEAN;
      case BIT: return TupleType.BIT;
      case INT1: return TupleType.INT1;
      case INT2: return TupleType.INT2;
      case INT4: return TupleType.INT4;
      case DATE: return TupleType.DATE;
      case INT8: return TupleType.INT8;
      case TIME: return TupleType.TIME;
      case TIMESTAMP: return TupleType.TIMESTAMP;
      case FLOAT4: return TupleType.FLOAT4;
      case FLOAT8: return TupleType.FLOAT8;
      case TEXT: return TupleType.TEXT;
      case CHAR: return TupleType.CHAR;
      case BLOB: return TupleType.BLOB;
      case INET4: return TupleType.INET4;
      case NULL_TYPE: return TupleType.NULL_TYPE;
      default: return TupleType.DATUM;
    }
  }

  private static int[] toKeyIndex(int start, int end) {
    int[] keyIndex = new int[end - start];
    for (int i = 0; i < keyIndex.length; i++) {
      keyIndex[i] = start + i;
    }
    return keyIndex;
  }

  private static enum TupleType {
    NULL_TYPE, BOOLEAN, BIT, INT1, INT2, INT4, DATE, INET4, INT8, TIME, TIMESTAMP,
    FLOAT4, FLOAT8, TEXT, CHAR, BLOB, DATUM
  }
}
