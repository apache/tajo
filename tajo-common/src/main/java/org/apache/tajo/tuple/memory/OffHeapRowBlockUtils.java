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
    MSD_RADIX_SORT,
    LSD_RADIX_SORT,
    HYBRID_RADIX_SORT,
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

  public static List<UnSafeTuple> hybridRadixSort(UnSafeTupleList list, int[] sortKeyIds, Type[] sortKeyTypes,
                                                  boolean[] asc, boolean[] nullFirst, Comparator<UnSafeTuple> comp) {
    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);
    RadixSortContext context = new RadixSortContext(in, sortKeyIds, sortKeyTypes, asc, nullFirst, comp);

    long before = System.currentTimeMillis();
    msdRadixSort(context, 0, in.length, 0, calculateInitialPass(sortKeyTypes[0]));
    context.msdRadixSortTime += System.currentTimeMillis() - before;
    context.printMsdStat();
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : context.in) {
      it.next();
      it.set(t);
    }
    return list;
  }

  public static List<UnSafeTuple> msdRadixSort(UnSafeTupleList list, int[] sortKeyIds, Type[] sortKeyTypes,
                                               boolean[] asc, boolean[] nullFirst, Comparator<UnSafeTuple> comp) {
    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);
    RadixSortContext context = new RadixSortContext(in, sortKeyIds, sortKeyTypes, asc, nullFirst, comp);

    long before = System.currentTimeMillis();
    msdRadixSort(context, 0, in.length, 0, calculateInitialPass(sortKeyTypes[0]));
    context.msdRadixSortTime += System.currentTimeMillis() - before;
    context.printMsdStat();
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : context.in) {
      it.next();
      it.set(t);
    }
    return list;
  }

  public static List<UnSafeTuple> lsdRadixSort(UnSafeTupleList list, int[] sortKeyIds, Type[] sortKeyTypes,
                                               boolean[] asc, boolean[] nullFirst, Comparator<UnSafeTuple> comp) {
    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);

    RadixSortContext context = new RadixSortContext(in, sortKeyIds, sortKeyTypes, asc, nullFirst, comp);
    long before = System.currentTimeMillis();
    lsdRadixSort(context);
    context.lsdRadixSortTime += System.currentTimeMillis() - before;
    context.printLsdStat();
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : context.in) {
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

  static int integer8RadixKey(UnSafeTuple tuple, int sortKeyId, boolean asc, boolean nullFirst, int pass) {
    int key = nullFirst ? 0 : _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // TODO: consider sign
      key = PlatformDependent.getByte(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFF;
      if (!asc) key = _16B_MAX_BIN_IDX - key;
    }
    return key;
  }

  static int integer8AscNullLastRadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _8B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // TODO: consider sign
      key = PlatformDependent.getByte(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFF;
    }
    return key;
  }

//  static void build8Histogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
//                               int[] positions, int[] keys) {
//    for (int i = start; i < exclusiveEnd; i++) {
//      keys[i] = integer8RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx],
//          context.asc[curSortKeyIdx], context.nullFirst[curSortKeyIdx], pass);
//      positions[keys[i]] += 1;
//    }
//
//    positions[0] += start;
//    for (int i = 0; i < positions.length - 1; i++) {
//      positions[i + 1] += positions[i];
//    }
//  }

  static void build8AscNullLastHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
                                         int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = integer8AscNullLastRadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }

    positions[0] += start;
    for (int i = 0; i < positions.length - 1; i++) {
      positions[i + 1] += positions[i];
    }
  }

  static int integer16RadixKey(UnSafeTuple tuple, int sortKeyId, boolean asc, boolean nullFirst, int pass) {
    int key = nullFirst ? 0 : _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // TODO: consider sign
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
      if (!asc) key = _16B_MAX_BIN_IDX - key;
    }
    return key;
  }

  static int integer16AscNullLastRadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // TODO: consider sign
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
    }
    return key;
  }

//  static void build16Histogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
//                               int[] positions, int[] keys) {
//    for (int i = start; i < exclusiveEnd; i++) {
//      keys[i] = integer16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx],
//          context.asc[curSortKeyIdx], context.nullFirst[curSortKeyIdx], pass);
//      positions[keys[i]] += 1;
//    }
//
//    positions[0] += start;
//    for (int i = 0; i < positions.length - 1; i++) {
//      positions[i + 1] += positions[i];
//    }
//  }

  static void build16AscNullLastHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
                                          int[] positions, int[] keys) {
    long before = System.currentTimeMillis();
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = integer16AscNullLastRadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
    context.getKeyTime += System.currentTimeMillis() - before;

    before = System.currentTimeMillis();
    positions[0] += start;
    for (int i = 0; i < positions.length - 1; i++) {
      positions[i + 1] += positions[i];
    }
    context.addPosTime += System.currentTimeMillis() - before;
  }

//  static void build16AscNullLastHistogram(RadixSortContext context, int[] prevEndIdx, int curSortKeyIdx, int pass,
//                                          int[] positions, int[] keys) {
//    for (int i = 0; i <prevEndIdx.length; i++) {
//      int start = prevEndIdx[i];
//      int exclusiveEnd = prevEndIdx[i + 1];
//      if (exclusiveEnd - start > 1) {
//        for (int j = start; j < exclusiveEnd; j++) {
//          keys[j] = integer16AscNullLastRadixKey(context.in[j], context.sortKeyIds[curSortKeyIdx], pass);
//          positions[keys[j]] += 1;
//        }
//      }
//    }
//
//    positions[0] += start;
//    for (int j = 0; j < positions.length - 1; j++) {
//      positions[j + 1] += positions[j];
//    }
//  }

  private static class RadixSortContext {
    @Contended UnSafeTuple[] in;
    @Contended UnSafeTuple[] out;
    @Contended final int[] keys;

    final int[] sortKeyIds;
    final int maxSortKeyId;
    final Type[] sortKeyTypes;
    final boolean[] asc;
    final boolean[] nullFirst;
    final Comparator<UnSafeTuple> comparator;

    long hybridRadixSortTime = 0;
    long msdRadixSortTime = 0;
    long lsdRadixSortTime = 0;
    long histogramBuildTime = 0;
    long swapTime = 0;
    long getKeyTime = 0;
    long addPosTime = 0;
    int msdRadixSortCall = 0;
    int lsdRadixSortLoop = 0;

    public RadixSortContext(UnSafeTuple[] in, int[] sortKeyIds, Type[] sortKeyTypes, boolean[] asc, boolean[] nullFirst,
                            Comparator<UnSafeTuple> comparator) {
      this.in = in;
      this.out = new UnSafeTuple[in.length];
      this.keys = new int[in.length];
      this.sortKeyIds = sortKeyIds;
      this.maxSortKeyId = sortKeyIds.length - 1;
      this.sortKeyTypes = sortKeyTypes;
      this.asc = asc;
      this.nullFirst = nullFirst;
      this.comparator = comparator;
    }

    public void printMsdStat() {
      LOG.info("- msdRadixSortTime: " + msdRadixSortTime + " ms");
      LOG.info("\t|- histogramBuildTime: " + histogramBuildTime + " ms");
      LOG.info("\t\t|- getKeyTime: " + getKeyTime + " ms");
      LOG.info("\t\t|- addPosTime: " + addPosTime + " ms");
      LOG.info("\t|- swapTime: " + swapTime + " ms");
      LOG.info("- msdRadixSortCall: " + msdRadixSortCall + " times");
    }

    public void printLsdStat() {
      LOG.info("- lsdRadixSortTime: " + lsdRadixSortTime + " ms");
      LOG.info("\t|- histogramBuildTime: " + histogramBuildTime + " ms");
      LOG.info("\t\t|- getKeyTime: " + getKeyTime + " ms");
      LOG.info("\t\t|- addPosTime: " + addPosTime + " ms");
      LOG.info("\t|- swapTime: " + swapTime + " ms");
      LOG.info("- lsdRadixSortLoop: " + lsdRadixSortLoop + " times");
    }

    public void printHybridStat() {
      LOG.info("- hybridRadixSortTime: " + hybridRadixSortTime + " ms");
      LOG.info("\t|- histogramBuildTime: " + histogramBuildTime + " ms");
      LOG.info("\t\t|- getKeyTime: " + getKeyTime + " ms");
      LOG.info("\t\t|- addPosTime: " + addPosTime + " ms");
      LOG.info("\t|- swapTime: " + swapTime + " ms");
      LOG.info("- msdRadixSortCall: " + msdRadixSortCall + " times");
      LOG.info("- lsdRadixSortLoop: " + lsdRadixSortLoop + " times");
    }
  }

  private final static int _16B_BIN_NUM = 65536;
  private final static int _16B_MAX_BIN_IDX = 65535;

  private final static int _8B_BIN_NUM = 256;
  private final static int _8B_MAX_BIN_IDX = 255;

  private final static int TIM_SORT_THRESHOLD = 65536;
  private final static int LSD_RADIX_SORT_THRESHOLD = 10_000;

  static void lsdRadixSort(RadixSortContext context) {
    int[] positions = new int[_16B_BIN_NUM];
    int[] keys = context.keys;

    for (int curSortKeyIdx = context.sortKeyIds.length - 1; curSortKeyIdx >= 0; curSortKeyIdx--) {
      int maxPass = typeByteSize(context.sortKeyTypes[curSortKeyIdx]);

      for (int pass = 0; pass < maxPass; pass += 2) {
        context.lsdRadixSortLoop++;
        long before = System.currentTimeMillis();
        build16AscNullLastHistogram(context, 0, context.in.length, curSortKeyIdx, pass, positions, keys);
        context.histogramBuildTime += System.currentTimeMillis() - before;

        if (positions[0] < context.in.length) {
          before = System.currentTimeMillis();
          for (int i = context.in.length - 1; i >= 0; i--) {
            context.out[--positions[keys[i]]] = context.in[i];
          }
          UnSafeTuple[] tmp = context.in;
          context.in = context.out;
          context.out = tmp;
          context.swapTime += System.currentTimeMillis() - before;
        }
//        LOG.info("pass: " + pass);
//        for (int i = 0; i < context.in.length; i++) {
//          LOG.info(context.out[i]);
//        }
        Arrays.fill(positions, 0);
      }
    }
  }

  static void hybridRadixSort(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass) {
    context.msdRadixSortCall++;
    final int[] binEndIdx = new int[_16B_BIN_NUM];
    final int[] keys = context.keys;

    // Make histogram
    long before = System.currentTimeMillis();
    build16AscNullLastHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
    context.histogramBuildTime += System.currentTimeMillis() - before;


    if (binEndIdx[0] < exclusiveEnd) {
      before = System.currentTimeMillis();
      final int[] binNextElemIdx = new int[_16B_BIN_NUM];
      System.arraycopy(binEndIdx, 0, binNextElemIdx, 0, _16B_BIN_NUM);
      for (int i = start; i < exclusiveEnd; i++) {
        context.out[--binNextElemIdx[keys[i]]] = context.in[i];
      }

      System.arraycopy(context.out, start, context.in, start, exclusiveEnd - start);
      context.swapTime += System.currentTimeMillis() - before;
    }

//    LOG.info("pass: " + pass + ", curKey: " + curSortKeyIdx + ", start: " + start + ", end: " + exclusiveEnd);
//    for (int i = start; i < exclusiveEnd; i++) {
//      LOG.info(context.in[i]);
//    }

    if (pass > 0 || curSortKeyIdx < context.maxSortKeyId) {
      int nextPass;
      if (pass > 0) {
        nextPass = pass - 2;
      } else {
        nextPass = typeByteSize(context.sortKeyTypes[++curSortKeyIdx]) - 2;
      }

      int len = binEndIdx[0] - start;

      if (len > 1) {
        if (len < LSD_RADIX_SORT_THRESHOLD) {

        } else {
          msdRadixSort(context, start, binEndIdx[0], curSortKeyIdx, nextPass);
        }
      }
      for (int i = 0; i < _16B_MAX_BIN_IDX && binEndIdx[i] < exclusiveEnd; i++) {
        len = binEndIdx[i + 1] - binEndIdx[i];
        if (len > 1) {
          msdRadixSort(context, binEndIdx[i], binEndIdx[i + 1], curSortKeyIdx, nextPass);
        }
      }
    }
  }

  static void msdRadixSort(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass) {
    context.msdRadixSortCall++;
    final int[] binEndIdx = new int[_16B_BIN_NUM];
    final int[] keys = context.keys;

    // Make histogram
    long before = System.currentTimeMillis();
    build16AscNullLastHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
//    build8AscNullLastHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
    context.histogramBuildTime += System.currentTimeMillis() - before;


    if (binEndIdx[0] < exclusiveEnd) {
      before = System.currentTimeMillis();
      final int[] binNextElemIdx = new int[_16B_BIN_NUM];
      System.arraycopy(binEndIdx, 0, binNextElemIdx, 0, _16B_BIN_NUM);
      for (int i = start; i < exclusiveEnd; i++) {
        context.out[--binNextElemIdx[keys[i]]] = context.in[i];
      }

      System.arraycopy(context.out, start, context.in, start, exclusiveEnd - start);
      context.swapTime += System.currentTimeMillis() - before;
    }

//    LOG.info("pass: " + pass + ", curKey: " + curSortKeyIdx + ", start: " + start + ", end: " + exclusiveEnd);
//    for (int i = start; i < exclusiveEnd; i++) {
//      LOG.info(context.in[i]);
//    }

    if (pass > 0 || curSortKeyIdx < context.maxSortKeyId) {
      int nextPass;
      if (pass > 0) {
        nextPass = pass - 2;
      } else {
        nextPass = typeByteSize(context.sortKeyTypes[++curSortKeyIdx]) - 2;
      }

      int len = binEndIdx[0] - start;

      if (len > 1) {
        if (len < TIM_SORT_THRESHOLD) {
          Arrays.sort(context.in, start, binEndIdx[0], context.comparator);
        } else {
          msdRadixSort(context, start, binEndIdx[0], curSortKeyIdx, nextPass);
        }
      }
      for (int i = 0; i < _16B_MAX_BIN_IDX && binEndIdx[i] < exclusiveEnd; i++) {
        len = binEndIdx[i + 1] - binEndIdx[i];
        if (len > 1) {
          if (len < TIM_SORT_THRESHOLD) {
            Arrays.sort(context.in, binEndIdx[i], binEndIdx[i + 1], context.comparator);
          } else {
            msdRadixSort(context, binEndIdx[i], binEndIdx[i + 1], curSortKeyIdx, nextPass);
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
      case MSD_RADIX_SORT:
        return msdRadixSort(list, sortKeyIds, sortKeyTypes, asc, nullFirst, comparator);
      case LSD_RADIX_SORT:
        return lsdRadixSort(list, sortKeyIds, sortKeyTypes, asc, nullFirst, comparator);
      default:
        throw new TajoRuntimeException(new UnsupportedException(algorithm.name()));
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

  static int calculateTotalPass(Type[] types) {
    int pass = 0;
    for (Type eachType: types) {
      pass += calculateInitialPass(eachType);
    }
    return pass;
  }

  static int calculateInitialPass(Type type) {
    int initialPass = typeByteSize(type) - 2;
//    int initialPass = typeByteSize(type) - 1;
    return initialPass < 0 ? 0 : initialPass;
  }

  static int typesByteSize(Type[] types) {
    int totalSize = 0;
    for (Type eachType : types) {
      totalSize += typeByteSize(eachType);
    }
    return totalSize;
  }

  static int typeByteSize(Type type) {
    switch (type) {
      case BOOLEAN:
        return 1;
      case CHAR:
        return 1;
      case BIT:
        return 1;
      case INT2:
        return 2;
      case INT4:
        return 4;
      case INT8:
        return 8;
      case FLOAT4:
        return 4;
      case FLOAT8:
        return 8;
      case INET4:
        return 4;
      case INET6:
        return 32;
      case DATE:
        return 4;
      case TIME:
        return 8;
      case TIMESTAMP:
        return 8;
      case TEXT:
      case BLOB:
      default:
        return -1;
    }
  }
}
