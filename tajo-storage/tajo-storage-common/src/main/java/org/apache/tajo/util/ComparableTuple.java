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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.storage.ReadableTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.ComparableVector.TupleType;

import java.util.Arrays;

public class ComparableTuple implements ReadableTuple, Comparable<ComparableTuple> {

  private final TupleType[] keyTypes;
  private final int[] keyIndex;
  private final Object[] keys;

  public ComparableTuple(Schema schema, int[] keyIndex) {
    this(ComparableVector.tupleTypes(schema, keyIndex), keyIndex);
  }

  public ComparableTuple(Schema schema, int start, int end) {
    this(schema, ComparableVector.toIndex(start, end));
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
    ComparableTuple other = (ComparableTuple) obj;
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
        case BLOB: if (!Arrays.equals((byte[]) keys[i], (byte[]) other.keys[i])) return false; continue;
        default: if (!keys[i].equals(other.keys[i])) return false; continue;
      }
    }
    return true;
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
        case BOOLEAN: if ((Boolean) keys[i] != tuple.getBool(field)) return false; continue;
        case BIT: if ((Byte) keys[i] != tuple.getByte(field)) return false; continue;
        case INT1:
        case INT2: if ((Short) keys[i] != tuple.getInt2(field)) return false; continue;
        case INT4:
        case DATE:
        case INET4: if ((Integer) keys[i] != tuple.getInt4(field)) return false; continue;
        case INT8:
        case TIME:
        case TIMESTAMP: if ((Long) keys[i] != tuple.getInt8(field)) return false; continue;
        case FLOAT4: if ((Float) keys[i] != tuple.getFloat4(field)) return false; continue;
        case FLOAT8: if ((Double) keys[i] != tuple.getFloat8(field)) return false; continue;
        case TEXT:
        case CHAR:
        case BLOB: if (!Arrays.equals((byte[]) keys[i], tuple.getBytes(field))) return false; continue;
        case DATUM: if (!keys[i].equals(tuple.get(field))) return false; continue;
      }
    }
    return true;
  }

  public int compareTo(ComparableTuple o) {
    for (int i = 0; i < keys.length; i++) {
      final boolean n1 = keys[i] == null;
      final boolean n2 = o.keys[i] == null;
      if (n1 && n2) {
        continue;
      }
      if (n1 ^ n2) {
        return n1 ? 1 : -1;
      }
      int compare;
      switch (keyTypes[i]) {
        case BOOLEAN: compare = Booleans.compare((Boolean) keys[i], (Boolean) o.keys[i]); break;
        case BIT: compare = (Byte)keys[i] - (Byte)o.keys[i]; break;
        case INT1:
        case INT2: compare = Shorts.compare((Short)keys[i], (Short)o.keys[i]); break;
        case INT4:
        case DATE: compare = Ints.compare((Integer)keys[i], (Integer)o.keys[i]); break;
        case INET4: compare = UnsignedInts.compare((Integer)keys[i], (Integer)o.keys[i]); break;
        case INT8:
        case TIME:
        case TIMESTAMP: compare = Longs.compare((Long)keys[i], (Long)o.keys[i]); break;
        case FLOAT4: compare = Floats.compare((Float)keys[i], (Float)o.keys[i]); break;
        case FLOAT8: compare = Doubles.compare((Double)keys[i], (Double)o.keys[i]); break;
        case CHAR:
        case TEXT:
        case BLOB: compare = TextDatum.COMPARATOR.compare((byte[])keys[i], (byte[])o.keys[i]); break;
        case DATUM: compare = ((Datum)keys[i]).compareTo((Datum)o.keys[i]); break;
        default:
          throw new IllegalArgumentException();
      }
      if (compare != 0) {
        return compare;
      }
    }
    return 0;
  }

  @Override
  public int hashCode() {
    int result = 1;
    for (Object key : keys) {
      int hash = key == null ? 0 :
          key instanceof byte[] ? Arrays.hashCode((byte[]) key) : key.hashCode();
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
      case BIT: return DatumFactory.createBit((Byte) keys[i]);
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
      case DATUM: return (Datum) keys[i];
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public TajoDataTypes.Type type(int fieldId) {
     TajoDataTypes.Type type = ComparableVector.dataType(keyTypes[fieldId]);
    return type == null ? ((Datum) keys[fieldId]).type() : type;
  }

  @Override
  public boolean isNull(int fieldId) {
    return keys[fieldId] == null;
  }

  @Override
  public boolean getBool(int fieldId) {
    return (Boolean)keys[fieldId];
  }

  @Override
  public byte getByte(int fieldId) {
    return (Byte)keys[fieldId];
  }

  @Override
  public char getChar(int fieldId) {
    return getText(fieldId).charAt(0);
  }

  @Override
  public byte[] getBytes(int fieldId) {
    return (byte[])keys[fieldId];
  }

  @Override
  public short getInt2(int fieldId) {
    return (Short)keys[fieldId];
  }

  @Override
  public int getInt4(int fieldId) {
    return (Integer)keys[fieldId];
  }

  @Override
  public long getInt8(int fieldId) {
    return (Long)keys[fieldId];
  }

  @Override
  public float getFloat4(int fieldId) {
    return (Float)keys[fieldId];
  }

  @Override
  public double getFloat8(int fieldId) {
    return (Double)keys[fieldId];
  }

  @Override
  public String getText(int fieldId) {
    return new String((byte[])keys[fieldId], TextDatum.DEFAULT_CHARSET);
  }

  @Override
  public Datum getProtobufDatum(int fieldId) {
    return (Datum)keys[fieldId];
  }

  @Override
  public Datum getInterval(int fieldId) {
    return (Datum)keys[fieldId];
  }

  @Override
  public char[] getUnicodeChars(int fieldId) {
    return StringUtils.convertBytesToChars((byte[]) keys[fieldId], TextDatum.DEFAULT_CHARSET);
  }

  @Override
  public Datum get(int fieldId) {
    return toDatum(fieldId);
  }
}


