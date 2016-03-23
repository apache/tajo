/*
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

package org.apache.tajo.tuple.memory;

import com.google.common.collect.Lists;
import com.google.common.primitives.*;
import io.netty.util.internal.PlatformDependent;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.exception.ValueOutOfRangeException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.RowBlockReader;

import java.util.*;

public class OffHeapRowBlockUtils {
  private static TupleConverter tupleConverter;

  static {
    tupleConverter = new TupleConverter();
  }

  public static List<Tuple> sort(MemoryRowBlock rowBlock, Comparator<Tuple> comparator) {
    List<Tuple> tupleList = Lists.newArrayList();

    ZeroCopyTuple zcTuple;
    if(rowBlock.getMemory().hasAddress()) {
      zcTuple = new UnSafeTuple();
    } else {
      zcTuple = new HeapTuple();
    }

    RowBlockReader reader = rowBlock.getReader();
    while(reader.next(zcTuple)) {
      tupleList.add(zcTuple);

      if(rowBlock.getMemory().hasAddress()) {
        zcTuple = new UnSafeTuple();
      } else {
        zcTuple = new HeapTuple();
      }
    }
    Collections.sort(tupleList, comparator);
    return tupleList;
  }

  public static List<UnSafeTuple> radixSort(UnSafeTupleList list, int[] sortKeyIds, Type[] sortKeyTypes,
                                            boolean[] asc, boolean[] nullFirst) {
    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);
    UnSafeTuple[] out = new UnSafeTuple[list.size()];
    int[] positions = new int[65536];

//    UnSafeTuple[] sorted = longRadixSortRecur(in, out, positions, 0, sortKeyIds, sortKeyTypes, asc, nullFirst, 0);
    UnSafeTuple[] sorted = longRadixSort(in, out, positions, 8, sortKeyIds, sortKeyTypes, asc, nullFirst, 0);
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : sorted) {
      it.next();
      it.set(t);
    }
    return list;
  }

  static UnSafeTuple[] longRadixSort(UnSafeTuple[] in, UnSafeTuple[] out, int[] positions, int maxPass,
                                     int[] sortKeyIds, Type[] sortKeyTypes,
                                     boolean[] asc, boolean[] nullFirst, int curSortKeyIdx) {
    UnSafeTuple[] tmp;
    for (int pass = 0; pass < maxPass - 1; pass += 2) {
      // Make histogram
      for (UnSafeTuple eachTuple : in) {
        int key = 65535; // for null
        if (!eachTuple.isBlankOrNull(sortKeyIds[curSortKeyIdx])) {
          // TODO: consider sign
          key = PlatformDependent.getShort(eachTuple.getFieldAddr(sortKeyIds[curSortKeyIdx]) + (pass));
          if (key < 0) key = (65536 + key);
        }
        positions[key] += 1;
      }

      int nonZeroCnt = 0;
      for (int i = 0; i < positions.length - 1; i++) {
        positions[i + 1] += positions[i];
      }

      if (positions[0] != in.length) {
        for (int i = in.length - 1; i >= 0; i--) {
          int key = 65535; // for null
          if (!in[i].isBlankOrNull(sortKeyIds[curSortKeyIdx])) {
            // TODO: consider sign
            key = PlatformDependent.getShort(in[i].getFieldAddr(sortKeyIds[curSortKeyIdx]) + (pass));
            if (key < 0) key = (65536 + key);
          }
          out[positions[key] - 1] = in[i];
          positions[key] -= 1;
        }

        // directly go to the next pass
        if (pass < maxPass - 1) {
          tmp = in;
          in = out;
          out = tmp;
        }
      }
      Arrays.fill(positions, 0);
    }
    return in;
  }

  static UnSafeTuple[] longRadixSortRecur(UnSafeTuple[] in, UnSafeTuple[] out, int[] positions, int pass,
                                          int[] sortKeyIds, Type[] sortKeyTypes,
                                          boolean[] asc, boolean[] nullFirst, int curSortKeyIdx) {
    // Make histogram
    for (UnSafeTuple eachTuple : in) {
      short key = 255; // for null
      if (!eachTuple.isBlankOrNull(sortKeyIds[curSortKeyIdx])) {
        // TODO: consider sign
        key = PlatformDependent.getByte(eachTuple.getFieldAddr(sortKeyIds[curSortKeyIdx]) + (pass));
        if (key < 0) key = (short) (256 + key);
      }
      positions[key] += 1;
    }

    int nonZeroCnt = 0;
    for (int i = 0; i < positions.length - 1; i++) {
      positions[i + 1] += positions[i];
    }

    if (positions[0] != in.length) {
      for (int i = in.length - 1; i >= 0; i--) {
        short key = 255;
        if (!in[i].isBlankOrNull(sortKeyIds[curSortKeyIdx])) {
          // TODO: consider sign
          key = PlatformDependent.getByte(in[i].getFieldAddr(sortKeyIds[curSortKeyIdx]) + (pass));
          if (key < 0) key = (short) (256 + key);
        }
        out[positions[key] - 1] = in[i];
        positions[key] -= 1;
      }

      if (pass < 7) {
        Arrays.fill(positions, 0);
        return longRadixSortRecur(out, in, positions, pass + 1, sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);
      } else {
        return out;
      }

    } else {
      // directly go to the next pass
      if (pass < 7) {
        Arrays.fill(positions, 0);
        return longRadixSortRecur(out, in, positions, pass + 1, sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);
      } else {
        return in;
      }
    }
  }

  static UnSafeTuple[] intRadixSort(UnSafeTuple[] in, UnSafeTuple[] out, int[] positions, int pass,
                                        int[] sortKeyIds, Type[] sortKeyTypes,
                                        boolean[] asc, boolean[] nullFirst, int curSortKeyIdx) {
    // Make histogram
    for (UnSafeTuple eachTuple : in) {
      // TODO: consider sign
//      int key = eachTuple.getInt4(sortKeyIds[curSortKeyIdx]) >> (8 * pass);
      short key = PlatformDependent.getByte(eachTuple.getFieldAddr(sortKeyIds[curSortKeyIdx]) + (pass));
      if (key < 0) key = (short) (256 + key);
      positions[key] += 1;
    }

    int nonZeroCnt = 0;
    for (int i = 0; i < positions.length - 1; i++) {
      positions[i + 1] += positions[i];
//      if (positions[i] > 0) {
//        nonZeroCnt++;
//      }
    }

    if (positions[0] != in.length) {
      for (int i = in.length - 1; i >= 0; i--) {
//        int key = in[i].getInt4(sortKeyIds[curSortKeyIdx]) >> (8 * pass);
        short key = PlatformDependent.getByte(in[i].getFieldAddr(sortKeyIds[curSortKeyIdx]) + (pass));
        if (key < 0) key = (short) (256 + key);
        out[positions[key] - 1] = in[i];
        positions[key] -= 1;
      }

      if (pass < 3) {
        Arrays.fill(positions, 0);
        return intRadixSort(out, in, positions, pass + 1, sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);
      } else {
        return out;
      }

    } else {
      // directly go to the next pass
      if (pass < 3) {
        Arrays.fill(positions, 0);
        return intRadixSort(out, in, positions, pass + 1, sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);
      } else {
        System.arraycopy(in, 0, out, 0, in.length);
        return in;
      }
    }
  }

  public static List<UnSafeTuple> sort(UnSafeTupleList list, Comparator<UnSafeTuple> comparator, int[] sortKeyIds, Type[] sortKeyTypes,
                                       boolean[] asc, boolean[] nullFirst) {
//    Collections.sort(list, comparator);
//    return list;
    return radixSort(list, sortKeyIds, sortKeyTypes, asc, nullFirst);
  }

  public static Tuple[] sortToArray(MemoryRowBlock rowBlock, Comparator<Tuple> comparator) {
    Tuple[] tuples = new Tuple[rowBlock.rows()];

    ZeroCopyTuple zcTuple;
    if(rowBlock.getMemory().hasAddress()) {
      zcTuple = new UnSafeTuple();
    } else {
      zcTuple = new HeapTuple();
    }

    RowBlockReader reader = rowBlock.getReader();
    for (int i = 0; i < rowBlock.rows() && reader.next(zcTuple); i++) {
      tuples[i] = zcTuple;
      if(rowBlock.getMemory().hasAddress()) {
        zcTuple = new UnSafeTuple();
      } else {
        zcTuple = new HeapTuple();
      }
    }
    Arrays.sort(tuples, comparator);
    return tuples;
  }

  public static final int compareColumn(UnSafeTuple tuple1, UnSafeTuple tuple2, int index, TajoDataTypes.Type type,
                                         boolean ascending, boolean nullFirst) {
    final boolean n1 = tuple1.isBlankOrNull(index);
    final boolean n2 = tuple2.isBlankOrNull(index);
    if (n1 && n2) {
      return 0;
    }

    if (n1 ^ n2) {
      return nullFirst ? (n1 ? -1 : 1) : (n1 ? 1 : -1);
    }

    int compare;
    switch (type) {
    case BOOLEAN:
      compare = Booleans.compare(tuple1.getBool(index), tuple2.getBool(index));
      break;
    case BIT:
      compare = tuple1.getByte(index) - tuple2.getByte(index);
      break;
    case INT1:
    case INT2:
      compare = Shorts.compare(tuple1.getInt2(index), tuple2.getInt2(index));
      break;
    case DATE:
    case INT4:
      compare = Ints.compare(tuple1.getInt4(index), tuple2.getInt4(index));
      break;
    case INET4:
      compare = UnsignedInts.compare(tuple1.getInt4(index), tuple2.getInt4(index));
      break;
    case TIME:
    case TIMESTAMP:
    case INT8:
      compare = Longs.compare(tuple1.getInt8(index), tuple2.getInt8(index));
      break;
    case FLOAT4:
      compare = Floats.compare(tuple1.getFloat4(index), tuple2.getFloat4(index));
      break;
    case FLOAT8:
      compare = Doubles.compare(tuple1.getFloat8(index), tuple2.getFloat8(index));
      break;
    case CHAR:
    case TEXT:
    case BLOB:
      compare = UnSafeTupleBytesComparator.compare(tuple1.getFieldAddr(index), tuple2.getFieldAddr(index));
      break;
    default:
      throw new TajoRuntimeException(
          new UnsupportedException("unknown data type '" + type.name() + "'"));
    }
    return ascending ? compare : -compare;
  }
  /**
   * This class is tuple converter to the RowBlock
   */
  public static class TupleConverter {

    public void convert(Tuple tuple, RowWriter writer) {
      try {
        writer.startRow();
        for (int i = 0; i < writer.dataTypes().length; i++) {
          writeField(i, tuple, writer);
        }
      } catch (ValueOutOfRangeException e) {
        writer.cancelRow();
        throw e;
      }
      writer.endRow();
    }

    protected void writeField(int colIdx, Tuple tuple, RowWriter writer) {

      if (tuple.isBlankOrNull(colIdx)) {
        writer.skipField();
      } else {
        switch (writer.dataTypes()[colIdx].getType()) {
        case BOOLEAN:
          writer.putBool(tuple.getBool(colIdx));
          break;
        case BIT:
          writer.putByte(tuple.getByte(colIdx));
          break;
        case INT1:
        case INT2:
          writer.putInt2(tuple.getInt2(colIdx));
          break;
        case INT4:
          writer.putInt4(tuple.getInt4(colIdx));
          break;
        case DATE:
          writer.putDate(tuple.getInt4(colIdx));
          break;
        case INT8:
          writer.putInt8(tuple.getInt8(colIdx));
          break;
        case TIMESTAMP:
          writer.putTimestamp(tuple.getInt8(colIdx));
          break;
        case TIME:
          writer.putTime(tuple.getInt8(colIdx));
          break;
        case FLOAT4:
          writer.putFloat4(tuple.getFloat4(colIdx));
          break;
        case FLOAT8:
          writer.putFloat8(tuple.getFloat8(colIdx));
          break;
        case CHAR:
        case TEXT:
          writer.putText(tuple.getBytes(colIdx));
          break;
        case BLOB:
          writer.putBlob(tuple.getBytes(colIdx));
          break;
        case INTERVAL:
          writer.putInterval((IntervalDatum) tuple.getInterval(colIdx));
          break;
        case PROTOBUF:
          writer.putProtoDatum((ProtobufDatum) tuple.getProtobufDatum(colIdx));
          break;
        case INET4:
          writer.putInet4(tuple.getInt4(colIdx));
          break;
        case NULL_TYPE:
          writer.skipField();
          break;
        default:
          throw new TajoRuntimeException(
              new UnsupportedException("unknown data type '" + writer.dataTypes()[colIdx].getType().name() + "'"));
        }
      }
    }
  }

  public static void convert(Tuple tuple, RowWriter writer) {
    tupleConverter.convert(tuple, writer);
  }
}
