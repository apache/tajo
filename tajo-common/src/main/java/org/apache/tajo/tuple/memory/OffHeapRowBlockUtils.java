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
import org.apache.tajo.util.SizeOf;
import sun.misc.Contended;

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

  public static List<UnSafeTuple> lsdRadixSort(UnSafeTupleList list, int[] sortKeyIds, Type[] sortKeyTypes,
                                               boolean[] asc, boolean[] nullFirst) {
    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);
    UnSafeTuple[] out = new UnSafeTuple[list.size()];

    longLsdRadixSort(new RadixSortContext(in), out, 8, sortKeyIds, sortKeyTypes, asc, nullFirst, 0);
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : in) {
      it.next();
      it.set(t);
    }
    return list;
  }

  public static List<UnSafeTuple> msdRadixSort(UnSafeTupleList list, int[] sortKeyIds, Type[] sortKeyTypes,
                                            boolean[] asc, boolean[] nullFirst, Comparator<UnSafeTuple> comp) {
    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);

//    longMsdRadixSort(in, 0, in.length, 7, sortKeyIds, sortKeyTypes, asc, nullFirst, 0);
    longMsdRadixSort(new RadixSortContext(in), 0, in.length, 6, sortKeyIds, sortKeyTypes, asc, nullFirst, 0, comp);
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : in) {
      it.next();
      it.set(t);
    }
    return list;
  }

  private static int getFieldOffset(long address, int fieldId) {
    return PlatformDependent.getInt(address + (long)(SizeOf.SIZE_OF_INT + (fieldId * SizeOf.SIZE_OF_INT)));
  }

  public static long getFieldAddr(long address, int fieldId) {
    return address + getFieldOffset(address, fieldId);
  }

  static int get8RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 255; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // TODO: consider sign
      key = PlatformDependent.getByte(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFF;
    }
    return key;
  }

  static int get16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 65535; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // TODO: consider sign
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
    }
    return key;
  }

  static void build8Histogram(RadixSortContext context, int start, int exclusiveEnd,
                               int[] positions, int pass,
                               int[] sortKeyIds, Type[] sortKeyTypes,
                               boolean[] asc, boolean[] nullFirst, int curSortKeyIdx) {
    for (int i = start; i < exclusiveEnd; i++) {
      int key = get8RadixKey(context.in[i], sortKeyIds[curSortKeyIdx], pass);
      positions[key] += 1;
    }

    positions[0] += start;
    for (int i = 0; i < positions.length - 1; i++) {
      positions[i + 1] += positions[i];
    }
  }

  static void build16Histogram(RadixSortContext context, int start, int exclusiveEnd,
                               int[] positions, int pass, int[] keys,
                               int[] sortKeyIds, Type[] sortKeyTypes,
                               boolean[] asc, boolean[] nullFirst, int curSortKeyIdx) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = get16RadixKey(context.in[i], sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }

    positions[0] += start;
    for (int i = 0; i < positions.length - 1; i++) {
      positions[i + 1] += positions[i];
    }
  }

  private static class RadixSortContext {
    @Contended UnSafeTuple[] in;
    @Contended int[] binEndIdx = new int[BIN_SIZE];
    @Contended int[] binNextElemIdx = new int [BIN_SIZE];

    public RadixSortContext(UnSafeTuple[] in) {
      this.in = in;
    }
  }

  static UnSafeTuple[] longLsdRadixSort(RadixSortContext context, UnSafeTuple[] out, int maxPass,
                                        int[] sortKeyIds, Type[] sortKeyTypes,
                                        boolean[] asc, boolean[] nullFirst, int curSortKeyIdx) {
    int[] positions = new int[65536];
    for (int pass = 0; pass < maxPass - 1; pass += 2) {
      // Make histogram
      int [] keys = new int[context.in.length];
      build16Histogram(context, 0, context.in.length,
          positions, pass, keys, sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);

      if (positions[0] != context.in.length) {
        for (int i = context.in.length - 1; i >= 0; i--) {
//          int key = get16RadixKey(context.in[i], sortKeyIds[curSortKeyIdx], pass);
          out[positions[keys[i]] - 1] = context.in[i];
          positions[keys[i]] -= 1;
        }
      }
      Arrays.fill(positions, 0);
      UnSafeTuple[] tmp = context.in;
      context.in = out;
      out = tmp;
    }
    return out;
  }

  private final static int BIN_SIZE = 65536; // 65536
  private final static int MAX_BIN_IDX = 65535; //65535
  private final static int TIM_SORT_THRESHOLD = 128;

  static void longMsdRadixSort(RadixSortContext context, int start, int exclusiveEnd, int pass,
                               int[] sortKeyIds, Type[] sortKeyTypes,
                               boolean[] asc, boolean[] nullFirst, int curSortKeyIdx,
                               Comparator<UnSafeTuple> comp) {
    // TODO: the total size of arrays is 1 MB. Consider 65536 -> 256
    // TODO: should they be created for each call longMsdRadixSort()?
//    int[] binEndIdx = new int[BIN_SIZE];
//    int[] binNextElemIdx = new int [BIN_SIZE];
    int[] binEndIdx = context.binEndIdx;
    int[] binNextElemIdx = context.binNextElemIdx;
    Arrays.fill(binEndIdx, 0);

    // Make histogram
    int[] keys = new int[context.in.length];
    build16Histogram(context, start, exclusiveEnd,
        binEndIdx, pass, keys, sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx);

    // Initialize bins
    binNextElemIdx[0] = start;
    System.arraycopy(binEndIdx, 0, binNextElemIdx, 1, MAX_BIN_IDX);

    // Swap tuples
    for (int i = 0; binNextElemIdx[i] < exclusiveEnd && i < MAX_BIN_IDX; i++) {
      while (binNextElemIdx[i] < binEndIdx[i]) {
//        for (int key = get16RadixKey(context.in[binNextElemIdx[i]], sortKeyIds[curSortKeyIdx], pass);
//             key != i;
//             key = get16RadixKey(context.in[binNextElemIdx[i]], sortKeyIds[curSortKeyIdx], pass)) {
        for (int key = keys[binNextElemIdx[i]]; key != i; key = keys[binNextElemIdx[i]]) {
          UnSafeTuple tmp = context.in[binNextElemIdx[i]];
          context.in[binNextElemIdx[i]] = context.in[binNextElemIdx[key]];
          context.in[binNextElemIdx[key]] = tmp;
          int tmpKey = keys[binNextElemIdx[i]];
          keys[binNextElemIdx[i]] = keys[binNextElemIdx[key]];
          keys[binNextElemIdx[key]] = tmpKey;
          binNextElemIdx[key]++;
        }

        binNextElemIdx[i]++;
      }
    }

    // Since every other bin is already fixed, the last bin should also be. So, skip it.

    if (pass > 0) {
      int nextPass = pass - 2;
      int len = binEndIdx[0] - start;
      if (len > 1) {
        if (len < TIM_SORT_THRESHOLD) {
          Arrays.sort(context.in, start, binEndIdx[0], comp);
        } else {
          longMsdRadixSort(context, start, binEndIdx[0], nextPass,
              sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx, comp);
        }
      }
      for (int i = 0; i < MAX_BIN_IDX && binEndIdx[i] < exclusiveEnd; i++) {
        len = binEndIdx[i + 1] - binEndIdx[i];
        if (len > 1) {
          if (len < TIM_SORT_THRESHOLD) {
            Arrays.sort(context.in, binEndIdx[i], binEndIdx[i + 1], comp);
          } else {
            longMsdRadixSort(context, binEndIdx[i], binEndIdx[i + 1], nextPass,
                sortKeyIds, sortKeyTypes, asc, nullFirst, curSortKeyIdx, comp);
          }
        }
      }
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
        return lsdRadixSort(list, sortKeyIds, sortKeyTypes, asc, nullFirst);
      case MSD_RADIX_SORT:
        return msdRadixSort(list, sortKeyIds, sortKeyTypes, asc, nullFirst, comparator);
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
