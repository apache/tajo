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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.exception.ValueOutOfRangeException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.RowBlockReader;

import java.util.*;

public class OffHeapRowBlockUtils {

  private final static Log LOG = LogFactory.getLog(OffHeapRowBlockUtils.class);
  private static TupleConverter tupleConverter;

  static {
    tupleConverter = new TupleConverter();
  }

  public enum SortAlgorithm{
    TIM_SORT,
    LSD_RADIX_SORT,
    MSD_RADIX_SORT,
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

    longLsdRadixSort(in, out, 8, sortKeyIds, sortKeyTypes, asc, nullFirst, 0);
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : in) {
      it.next();
      it.set(t);
    }
    return list;
  }

  public static List<UnSafeTuple> msdRadixSort(UnSafeTupleList list, int[] sortKeyIds, Type[] sortKeyTypes,
                                            boolean[] asc, boolean[] nullFirst) {
    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);

    longMsdRadixSort(in, 0, in.length, 7, sortKeyIds, sortKeyTypes, asc, nullFirst, 0);
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : in) {
      it.next();
      it.set(t);
    }
    return list;
  }

  static int get8RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 255; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // TODO: consider sign & improve this
      key = PlatformDependent.getByte(tuple.getFieldAddr(sortKeyId) + (pass));
      if (key < 0) key = (256 + key);
    }
    return key;
  }

  static int get16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 65535; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // TODO: consider sign & improve this
      key = PlatformDependent.getShort(tuple.getFieldAddr(sortKeyId) + (pass));
      if (key < 0) key = (65536 + key);
    }
    return key;
  }

  static void build8Histogram(UnSafeTuple[] in, int start, int length,
                               int[] positions, int pass,
                               int[] sortKeyIds, Type[] sortKeyTypes,
                               boolean[] asc, boolean[] nullFirst, int curSortKeyIdx) {
    int end = start + length;
    for (int i = start; i < end; i++) {
      int key = get8RadixKey(in[i], sortKeyIds[curSortKeyIdx], pass);
      positions[key] += 1;
    }

    positions[0] += start;
    for (int i = 0; i < positions.length - 1; i++) {
      positions[i + 1] += positions[i];
    }
  }

  static void build16Histogram(UnSafeTuple[] in, int start, int length,
                               int[] positions, int pass,
                               int[] sortKeyIds, Type[] sortKeyTypes,
                               boolean[] asc, boolean[] nullFirst, int curSortKeyIdx) {
    int end = start + length;
    for (int i = start; i < end; i++) {
      int key = get16RadixKey(in[i], sortKeyIds[curSortKeyIdx], pass);
      positions[key] += 1;
    }

    positions[0] += start;
    for (int i = 0; i < positions.length - 1; i++) {
      positions[i + 1] += positions[i];
    }
  }

  static UnSafeTuple[] longLsdRadixSort(UnSafeTuple[] in, UnSafeTuple[] out, int maxPass,
                                        int[] sortKeyIds, Type[] sortKeyTypes,
                                        boolean[] asc, boolean[] nullFirst, int curSortKeyIdx) {
    int[] positions = new int[65536];
    for (int pass = 0; pass < maxPass - 1; pass += 2) {
      // Make histogram
      build16Histogram(in, 0, in.length,
          positions, pass, sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);

      if (positions[0] != in.length) {
        for (int i = in.length - 1; i >= 0; i--) {
          int key = get16RadixKey(in[i], sortKeyIds[curSortKeyIdx], pass);
          out[positions[key] - 1] = in[i];
          positions[key] -= 1;
        }
      }
      Arrays.fill(positions, 0);
      UnSafeTuple[] tmp = in;
      in = out;
      out = tmp;
    }
    return out;
  }

  private final static int BIN_SIZE = 256; // 65536
  private final static int MAX_BIN_IDX = 255; //65535

  static void longMsdRadixSort(UnSafeTuple[] in, int start, int length, int pass,
                               int[] sortKeyIds, Type[] sortKeyTypes,
                               boolean[] asc, boolean[] nullFirst, int curSortKeyIdx) {
    if (length < 2) {
      return;
    }
    // TODO: the total size of arrays is 1 MB. Consider 65536 -> 256
    // TODO: should they be created for each call longMsdRadixSort()?
    int[] binEndIdx = new int[BIN_SIZE];
    int[] binNextElemIdx = new int [BIN_SIZE];

    // Make histogram
    build8Histogram(in, start, length,
        binEndIdx, pass, sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);

    // Initialize bins
    binNextElemIdx[0] = start;
    System.arraycopy(binEndIdx, 0, binNextElemIdx, 1, MAX_BIN_IDX);

    int end = start + length;
    for (int i = 0; i < MAX_BIN_IDX && binNextElemIdx[i] < end; i++) {
      while (binNextElemIdx[i] < binEndIdx[i]) {
        for (int key = get8RadixKey(in[binNextElemIdx[i]], sortKeyIds[curSortKeyIdx], pass);
             key != i;
             key = get8RadixKey(in[binNextElemIdx[i]], sortKeyIds[curSortKeyIdx], pass)) {
          UnSafeTuple tmp = in[binNextElemIdx[i]];
          in[binNextElemIdx[i]] = in[binNextElemIdx[key]];
          in[binNextElemIdx[key]] = tmp;
          binNextElemIdx[key]++;
        }

        binNextElemIdx[i]++;
      }
    }

    // Since every other bin is already fixed, bin[65535] should also be.

    if (pass > 0) {
      int nextPass = pass - 1;
      longMsdRadixSort(in, 0, binEndIdx[0], nextPass,
          sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);
      for (int i = 0; i < MAX_BIN_IDX; i++) {
        longMsdRadixSort(in, binEndIdx[i], binEndIdx[i + 1] - binEndIdx[i], nextPass,
            sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);
      }
      longMsdRadixSort(in, binEndIdx[MAX_BIN_IDX], in.length - binEndIdx[MAX_BIN_IDX], nextPass,
          sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);
    }
  }

  public static List<UnSafeTuple> sort(UnSafeTupleList list, Comparator<UnSafeTuple> comparator, int[] sortKeyIds, Type[] sortKeyTypes,
                                       boolean[] asc, boolean[] nullFirst, SortAlgorithm algorithm) {
    LOG.info(algorithm.name() + " is used for sort");
    switch (algorithm) {
      case TIM_SORT:
        Collections.sort(list, comparator);
        return list;
      case LSD_RADIX_SORT:
        return radixSort(list, sortKeyIds, sortKeyTypes, asc, nullFirst);
      case MSD_RADIX_SORT:
        return msdRadixSort(list, sortKeyIds, sortKeyTypes, asc, nullFirst);
      default:
        throw new TajoRuntimeException(new NotImplementedException(algorithm.name()));
    }
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
