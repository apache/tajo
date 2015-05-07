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

package org.apache.tajo.util;

import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.UnsignedInts;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.storage.ReadableTuple;
import org.apache.tajo.storage.VTuple;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Extract raw level values (primitive or String/byte[]) from each of key columns for compare
 */
public class ComparableVector {

  protected final TupleVector[] vectors;  // values of key columns
  protected final int[] setIndex;
  protected final int[] compareIndex;

  public ComparableVector(int rows, Schema schema, SortSpec[] sortKeys, int[] keyIndex) {
    vectors = new TupleVector[schema.size()];

    for (int i = 0; i < vectors.length; i++) {
      Column column = schema.getColumn(i);
      TajoDataTypes.Type type = column.getDataType().getType();
      int index = Ints.indexOf(keyIndex, i);
      if (index >= 0) {
        boolean nullFirst = sortKeys[index].isNullFirst();
        boolean ascending = sortKeys[index].isAscending();
        boolean nullInvert = nullFirst && ascending || !nullFirst && !ascending;
        vectors[i] = new TupleVector(tupleType(type), rows, nullInvert, ascending);
      } else {
        vectors[i] = new TupleVector(tupleType(type), rows);
      }
    }
    this.setIndex = toIndex(0, vectors.length);
    this.compareIndex = keyIndex;
  }

  public ComparableVector(int rows, SortSpec[] sortKeys, int[] keyIndex) {
    vectors = new TupleVector[sortKeys.length];
    for (int i = 0; i < vectors.length; i++) {
      TajoDataTypes.Type type = sortKeys[i].getSortKey().getDataType().getType();
      boolean nullFirst = sortKeys[i].isNullFirst();
      boolean ascending = sortKeys[i].isAscending();
      boolean nullInvert = nullFirst && ascending || !nullFirst && !ascending;
      vectors[i] = new TupleVector(tupleType(type), rows, nullInvert, ascending);
    }
    this.setIndex = keyIndex;
    this.compareIndex = toIndex(0, vectors.length);
  }

  public ComparableVector(int rows, Schema schema) {
    vectors = new TupleVector[schema.size()];
    for (int i = 0; i < vectors.length; i++) {
      Column column = schema.getColumn(i);
      TajoDataTypes.Type type = column.getDataType().getType();
      vectors[i] = new TupleVector(tupleType(type), rows, false, true);
    }
    this.setIndex = this.compareIndex = toIndex(0, vectors.length);
  }

  public int compare(final int i1, final int i2) {
    for (int index : compareIndex) {
      int compare = vectors[index].compare(i1, i2);
      if (compare != 0) {
        return compare;
      }
    }
    return 0;
  }

  public void set(int index, ReadableTuple tuple) {
    for (int i = 0; i < vectors.length; i++) {
      vectors[i].set(index, tuple, setIndex[i]);
    }
  }

  public void append(ReadableTuple tuple) {
    for (int i = 0; i < vectors.length; i++) {
      vectors[i].append(tuple, setIndex[i]);
    }
  }

  public void setNull(int index) {
    for (TupleVector vector : vectors) {
      vector.nulls.set(index);
    }
  }

  public void reset() {
    for (TupleVector vector : vectors) {
      vector.index = 0;
      vector.nulls.clear();
      if (vector.bytes != null) {
        Arrays.fill(vector.bytes, null);
      }
      if (vector.data != null) {
        Arrays.fill(vector.data, null);
      }
    }
  }

  public boolean isNull(int index, int fieldId) {
    return vectors[fieldId].isNull(index);
  }

  public boolean getBool(int index, int fieldId) {
    return vectors[fieldId].booleans[index];
  }

  public byte getByte(int index, int fieldId) {
    return vectors[fieldId].bits[index];
  }

  public char getChar(int index, int fieldId) {
    return new String(vectors[fieldId].bytes[index], TextDatum.DEFAULT_CHARSET).charAt(0);
  }

  public byte[] getBytes(int index, int fieldId) {
    return vectors[fieldId].bytes[index];
  }

  public short getInt2(int index, int fieldId) {
    return vectors[fieldId].shorts[index];
  }

  public int getInt4(int index, int fieldId) {
    return vectors[fieldId].ints[index];
  }

  public long getInt8(int index, int fieldId) {
    return vectors[fieldId].longs[index];
  }

  public float getFloat4(int index, int fieldId) {
    return vectors[fieldId].floats[index];
  }

  public double getFloat8(int index, int fieldId) {
    return vectors[fieldId].doubles[index];
  }

  public String getText(int index, int fieldId) {
    return new String(vectors[fieldId].bytes[index], TextDatum.DEFAULT_CHARSET);
  }

  public Datum getProtobufDatum(int index, int fieldId) {
    return vectors[fieldId].data[fieldId];
  }

  public Datum getInterval(int index, int fieldId) {
    return vectors[fieldId].data[fieldId];
  }

  public char[] getUnicodeChars(int index, int fieldId) {
    return StringUtils.convertBytesToChars(vectors[fieldId].bytes[index], TextDatum.DEFAULT_CHARSET);
  }

  public Datum get(int index, int fieldId) {
    return vectors[fieldId].toDatum(index);
  }

  public static class TupleVector {

    private final TupleType type;
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
    private Datum[] data;

    private int index;  // appending index

    private TupleVector(TupleType type, int length) {
      this(type, length, false, true);
    }

    private TupleVector(TupleType type, int length, boolean nullInvert, boolean ascending) {
      this.type = type;
      this.nulls = new BitSet(length);
      this.nullInvert = nullInvert;
      this.ascending = ascending;
      switch (type) {
        case NULL_TYPE: break;
        case BOOLEAN: booleans = new boolean[length]; break;
        case BIT: bits = new byte[length]; break;
        case INT1:
        case INT2: shorts = new short[length]; break;
        case INT4:
        case DATE:
        case INET4: ints = new int[length]; break;
        case INT8:
        case TIME:
        case TIMESTAMP: longs = new long[length]; break;
        case FLOAT4: floats = new float[length]; break;
        case FLOAT8: doubles = new double[length]; break;
        case CHAR:
        case TEXT:
        case BLOB: bytes = new byte[length][]; break;
        case DATUM: data = new Datum[length]; break;
        default:
          throw new IllegalArgumentException();
      }
    }

    public final void append(ReadableTuple tuple, int field) {
      set(index++, tuple, field);
    }

    public final void set(int index, ReadableTuple tuple, int field) {
      if (tuple.isNull(field)) {
        nulls.set(index);
        return;
      }
      nulls.clear(index);
      switch (type) {
        case BOOLEAN: booleans[index] = tuple.getBool(field); break;
        case BIT: bits[index] = tuple.getByte(field); break;
        case INT1:
        case INT2: shorts[index] = tuple.getInt2(field); break;
        case INT4:
        case DATE:
        case INET4: ints[index] = tuple.getInt4(field); break;
        case INT8:
        case TIME:
        case TIMESTAMP: longs[index] = tuple.getInt8(field); break;
        case FLOAT4: floats[index] = tuple.getFloat4(field); break;
        case FLOAT8: doubles[index] = tuple.getFloat8(field); break;
        case TEXT:
        case CHAR:
        case BLOB: bytes[index] = tuple.getBytes(field); break;
        case DATUM: data[index] = tuple.get(field); break;
        default:
          throw new IllegalArgumentException();
      }
    }

    public final int compare(int index1, int index2) {
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
        case BOOLEAN: compare = Booleans.compare(booleans[index1], booleans[index2]); break;
        case BIT: compare = bits[index1] - bits[index2]; break;
        case INT1:
        case INT2: compare = Shorts.compare(shorts[index1], shorts[index2]); break;
        case INT4:
        case DATE: compare = Ints.compare(ints[index1], ints[index2]); break;
        case INET4: compare = UnsignedInts.compare(ints[index1], ints[index2]); break;
        case INT8:
        case TIME:
        case TIMESTAMP: compare = Longs.compare(longs[index1], longs[index2]); break;
        case FLOAT4: compare = Floats.compare(floats[index1], floats[index2]); break;
        case FLOAT8: compare = Doubles.compare(doubles[index1], doubles[index2]); break;
        case CHAR:
        case TEXT:
        case BLOB: compare = TextDatum.COMPARATOR.compare(bytes[index1], bytes[index2]); break;
        case DATUM: compare = data[index1].compareTo(data[index2]); break;
        default:
          throw new IllegalArgumentException();
      }
      return ascending ? compare : -compare;
    }

    public boolean isNull(int index) {
      return nulls.get(index);
    }

    public Datum toDatum(int index) {
      if (nulls.get(index)) {
        return NullDatum.get();
      }
      switch (type) {
        case NULL_TYPE: return NullDatum.get();
        case BOOLEAN: return DatumFactory.createBool(booleans[index]);
        case BIT: return DatumFactory.createBit(bits[index]);
        case INT1:
        case INT2: return DatumFactory.createInt2(shorts[index]);
        case INT4: return DatumFactory.createInt4(ints[index]);
        case DATE: return DatumFactory.createDate(ints[index]);
        case INET4: return DatumFactory.createInet4(ints[index]);
        case INT8: return DatumFactory.createInt8(longs[index]);
        case TIME: return DatumFactory.createTime(longs[index]);
        case TIMESTAMP: return DatumFactory.createTimestamp(longs[index]);
        case FLOAT4: return DatumFactory.createFloat4(floats[index]);
        case FLOAT8: return DatumFactory.createFloat8(doubles[index]);
        case TEXT: return DatumFactory.createText(bytes[index]);
        case CHAR: return DatumFactory.createChar(bytes[index]);
        case BLOB: return DatumFactory.createBlob(bytes[index]);
        case DATUM: return data[index];
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  public VTuple toVTuple(int index) {
    VTuple vtuple = new VTuple(vectors.length);
    for (int i = 0; i < vectors.length; i++) {
      vtuple.put(i, vectors[i].toDatum(index));
    }
    return vtuple;
  }

  public static boolean isVectorizable(SortSpec[] sortKeys) {
    return sortKeys.length != 0;
  }

  static TupleType[] tupleTypes(Schema schema, int[] keyIndex) {
    TupleType[] types = new TupleType[keyIndex.length];
    for (int i = 0; i < keyIndex.length; i++) {
      types[i] = tupleType(schema.getColumn(keyIndex[i]).getDataType().getType());
    }
    return types;
  }

  static TupleType tupleType(TajoDataTypes.Type type) {
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

  static TajoDataTypes.Type dataType(TupleType type) {
    switch (type) {
      case BOOLEAN: return TajoDataTypes.Type.BOOLEAN;
      case BIT: return TajoDataTypes.Type.BIT;
      case INT1: return TajoDataTypes.Type.INT1;
      case INT2: return TajoDataTypes.Type.INT2;
      case INT4: return TajoDataTypes.Type.INT4;
      case DATE: return TajoDataTypes.Type.DATE;
      case INT8: return TajoDataTypes.Type.INT8;
      case TIME: return TajoDataTypes.Type.TIME;
      case TIMESTAMP: return TajoDataTypes.Type.TIMESTAMP;
      case FLOAT4: return TajoDataTypes.Type.FLOAT4;
      case FLOAT8: return TajoDataTypes.Type.FLOAT8;
      case TEXT: return TajoDataTypes.Type.TEXT;
      case CHAR: return TajoDataTypes.Type.CHAR;
      case BLOB: return TajoDataTypes.Type.BLOB;
      case INET4: return TajoDataTypes.Type.INET4;
      case NULL_TYPE: return TajoDataTypes.Type.NULL_TYPE;
      default: return null;
    }
  }

  static int[] toIndex(int start, int end) {
    int[] keyIndex = new int[end - start];
    for (int i = 0; i < keyIndex.length; i++) {
      keyIndex[i] = start + i;
    }
    return keyIndex;
  }

  static enum TupleType {
    NULL_TYPE, BOOLEAN, BIT, INT1, INT2, INT4, DATE, INET4, INT8, TIME, TIMESTAMP,
    FLOAT4, FLOAT8, TEXT, CHAR, BLOB, DATUM
  }
}
