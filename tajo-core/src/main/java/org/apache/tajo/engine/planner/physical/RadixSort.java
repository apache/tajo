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

package org.apache.tajo.engine.planner.physical;

import io.netty.util.internal.PlatformDependent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.tuple.memory.UnSafeTuple;
import org.apache.tajo.tuple.memory.UnSafeTupleList;
import org.apache.tajo.util.SizeOf;
import sun.misc.Contended;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

/**
 *
 * Radix sort implementation (https://en.wikipedia.org/wiki/Radix_sort).
 * This implementation uses the hybrid approach which consists of MSD radix sort and Tim sort.
 *
 * It first organizes the given tuples into several bins which have the same radix key. For each bin, it groups
 * it again using MSD radix sort if the length of the bin is sufficiently large. Otherwise, it simply sorts that bin
 * using Tim sort.
 *
 */
public class RadixSort {

  private static final Log LOG = LogFactory.getLog(RadixSort.class);

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

    long msdRadixSortTime = 0;
    long histogramPrepareTime = 0;
    long swapTime = 0;
    long histogramBuildTime = 0;
    int msdRadixSortCall = 0;

    public RadixSortContext(UnSafeTuple[] in, Schema schema, SortSpec[] sortSpecs, Comparator<UnSafeTuple> comparator) {
      this.in = in;
      this.out = new UnSafeTuple[in.length];
      this.keys = new int[in.length];
      this.maxSortKeyId = sortSpecs.length - 1;
      this.sortKeyIds = new int[sortSpecs.length];
      sortKeyTypes = new Type[sortSpecs.length];
      asc = new boolean[sortSpecs.length];
      nullFirst = new boolean[sortSpecs.length];
      for (int i = 0; i < sortSpecs.length; i++) {
        if (sortSpecs[i].getSortKey().hasQualifier()) {
          this.sortKeyIds[i] = schema.getColumnId(sortSpecs[i].getSortKey().getQualifiedName());
        } else {
          this.sortKeyIds[i] = schema.getColumnIdByName(sortSpecs[i].getSortKey().getSimpleName());
        }
        this.asc[i] = sortSpecs[i].isAscending();
        this.nullFirst[i] = sortSpecs[i].isNullsFirst();
        this.sortKeyTypes[i] = sortSpecs[i].getSortKey().getDataType().getType();
      }
      this.comparator = comparator;
    }

    public void printMsdStat() {
      LOG.info("- msdRadixSortTime: " + msdRadixSortTime + " ms");
      LOG.info("\t|- histogramPrepareTime: " + histogramPrepareTime + " ms");
      LOG.info("\t\t|- histogramBuildTime: " + histogramBuildTime + " ms");
      LOG.info("\t|- swapTime: " + swapTime + " ms");
      LOG.info("- msdRadixSortCall: " + msdRadixSortCall + " times");
    }
  }

  private final static int _16B_BIN_NUM = 65536;
  private final static int _16B_MAX_BIN_IDX = 65535;
  private static int TIM_SORT_THRESHOLD = -1;

  /**
   * Entry method.
   *
   * @param list
   * @param schema input schema
   * @param sortSpecs sort specs
   * @param comp comparator for Tim sort
   * @return a sorted list of tuples
   */
  public static List<UnSafeTuple> sort(QueryContext queryContext, UnSafeTupleList list, Schema schema, SortSpec[] sortSpecs,
                                       Comparator<UnSafeTuple> comp) {
    TIM_SORT_THRESHOLD = queryContext.getInt(SessionVars.TEST_TIM_SORT_THRESHOLD_FOR_RADIX_SORT);
    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);
    RadixSortContext context = new RadixSortContext(in, schema, sortSpecs, comp);

    long before = System.currentTimeMillis();
    recursiveCallForNextKey(context, 0, context.in.length, 0);
    context.msdRadixSortTime += System.currentTimeMillis() - before;
    context.printMsdStat();
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : context.in) {
      it.next();
      it.set(t);
    }
    return list;
  }

  static void recursiveCallForNextKey(RadixSortContext context, int start, int exlusiveEnd, int curSortKeyIdx) {
    if (needConsiderSign(context.sortKeyTypes[curSortKeyIdx])) {
      signedIntegerMsdRadixSort(context, start, exlusiveEnd, curSortKeyIdx,
          calculateInitialPass(context.sortKeyTypes[curSortKeyIdx]), true);
    } else {
      signedIntegerMsdRadixSort(context, start, exlusiveEnd, curSortKeyIdx,
          calculateInitialPass(context.sortKeyTypes[curSortKeyIdx]), false);
    }
  }

  static boolean needConsiderSign(Type type) {
    switch (type) {
      case INT2:
      case INT4:
      case INT8:
      case TIME:
      case TIMESTAMP:
        return true;
      case DATE:
      case INET4:
        return false;
      default:
        throw new TajoInternalError(new UnsupportedException(type.name()));
    }
  }

  private static int getFieldOffset(long address, int fieldId) {
    return PlatformDependent.getInt(address + (long)(SizeOf.SIZE_OF_INT + (fieldId * SizeOf.SIZE_OF_INT)));
  }

  private static long getFieldAddr(long address, int fieldId) {
    return address + getFieldOffset(address, fieldId);
  }

  /**
   * Get a radix key from a column values of the given tuple.
   * The sign of the column value should be considered.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int ascNullLastSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // For negative values, the key should be 0 ~ 32767. For positive values, the key should be 32768 ~ 65535.
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) + 32768;
    }
    return key;
  }

  /**
   * Get a radix key from a column values of the given tuple.
   * The sign of the column value should be considered.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int ascNullFirstSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 0; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // For negative values, the key should be 0 ~ 32767. For positive values, the key should be 32768 ~ 65535.
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) + 32768;
    }
    return key;
  }

  /**
   * Get a radix key from a column values of the given tuple.
   * The sign of the column value should be considered.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int descNullLastSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // For positive values, the key should be 0 ~ 32767. For negative values, the key should be 32768 ~ 65535.
      key = 32767 - PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass));
    }
    return key;
  }

  /**
   * Get a radix key from a column values of the given tuple.
   * The sign of the column value should be considered.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int descNullFirstSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 0; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // For positive values, the key should be 0 ~ 32767. For negative values, the key should be 32768 ~ 65535.
      key = 32767 - PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass));
    }
    return key;
  }

  /**
   * Get a radix key from a column values of the given tuple.
   * The return key is an unsigned short value.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int ascNullLast16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
    }
    return key;
  }

  /**
   * Get a radix key from a column values of the given tuple.
   * The return key is an unsigned short value.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int ascNullFirst16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 0; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
    }
    return key;
  }

  /**
   * Get a radix key from a column values of the given tuple.
   * The return key is an unsigned short value.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int descNullLast16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = _16B_MAX_BIN_IDX - PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
    }
    return key;
  }

  /**
   * Get a radix key from a column values of the given tuple.
   * The return key is an unsigned short value.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int descNullFirst16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 0; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = _16B_MAX_BIN_IDX - PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
    }
    return key;
  }

  /**
   * Calculate positions of the input tuples, and then build a histogram.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare16AscNullLastSignConsideredHistogram(RadixSortContext context, int start, int exclusiveEnd,
                                                          int curSortKeyIdx, int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = ascNullLastSignConsidered16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  /**
   * Calculate positions of the input tuples, and then build a histogram.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare16AscNullFirstSignConsideredHistogram(RadixSortContext context, int start, int exclusiveEnd,
                                                           int curSortKeyIdx, int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = ascNullFirstSignConsidered16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  /**
   * Calculate positions of the input tuples, and then build a histogram.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare16DescNullLastSignConsideredHistogram(RadixSortContext context, int start, int exclusiveEnd,
                                                           int curSortKeyIdx, int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = descNullLastSignConsidered16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  /**
   * Calculate positions of the input tuples, and then build a histogram.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare16DescNullFirstSignConsideredHistogram(RadixSortContext context, int start, int exclusiveEnd,
                                                            int curSortKeyIdx, int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = descNullFirstSignConsidered16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  /**
   * Calculate positions of the input tuples, and then build a histogram.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare16AscNullLastHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx,
                                            int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = ascNullLast16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  /**
   * Calculate positions of the input tuples, and then build a histogram.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare16AscNullFirstHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx,
                                             int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = ascNullFirst16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  /**
   * Calculate positions of the input tuples, and then build a histogram.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare16DescNullLastHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx,
                                             int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = descNullLast16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  /**
   * Calculate positions of the input tuples, and then build a histogram.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare16DescNullFirstHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx,
                                              int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = descNullFirst16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  static void buildHistogram(RadixSortContext context, int start, int[] positions) {
    long before = System.currentTimeMillis();
    positions[0] += start;
    for (int i = 0; i < positions.length - 1; i++) {
      positions[i + 1] += positions[i];
    }
    context.histogramBuildTime += System.currentTimeMillis() - before;
  }

  /**
   * Sort a specified part of the input tuples when the current sort key has a signed integer type.
   * If the length of the part is sufficiently large, recursively call msdRadixSort(). Otherwise, call Arrays.sort().
   *
   * @param context radix sort context
   * @param start start position of the part will be sorted.
   * @param exclusiveEnd end position of the part will be sorted.
   * @param curSortKeyIdx current sort key index
   * @param pass current pass
   * @param considerSign a flag to represent that the sign must be considered
   */
  static void signedIntegerMsdRadixSort(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx,
                                        int pass, boolean considerSign) {
    context.msdRadixSortCall++;

    // This array contains the end positions of bins. For example, if an array of the length of 100 is organized into
    // 10 bins which have the equal length of 10, this array will contain the below value.
    //
    // [0]  [1]  [2]  [3]  [4]  [5]  [6]  [7]  [8]  [9]
    //  10   20   30   40   50   60   70   80   90  100
    //
    // If the considerSign flag is set, this array is used for both negative and positive values.
    // The positions of both values are determined by the asc flag.
    // When the asc flag is set, the first half of the array is used for negative values.
    // Otherwise, the second half of the array is used for negative values.
    //
    // Note: too many recursive calls to msdRadixSort() can incur a lot of memory overhead because this array should be
    // always newly created when it is called.
    final int[] binEndIdx = new int[_16B_BIN_NUM];

    // An array to cache radix keys which are gotten while building the histogram.
    // Since getting keys is the most expensive part of this implementation, keys should be cached once they are gotten.
    final int[] keys = context.keys;

    // TODO: consider the current key type
    long before = System.currentTimeMillis();
    // Build a histogram.
    // Call different methods depending on the sort spec of the current key. This is to avoid frequent branch misses.
    // This is effective because the below code block is the most expensive part of this implementation.
    // TODO: code generation can simplify the below codes.
    if (considerSign) {
      if (context.asc[curSortKeyIdx]) {
        if (context.nullFirst[curSortKeyIdx]) {
          prepare16AscNullFirstSignConsideredHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx,
              keys);
        } else {
          prepare16AscNullLastSignConsideredHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx,
              keys);
        }
      } else {
        if (context.nullFirst[curSortKeyIdx]) {
          prepare16DescNullFirstSignConsideredHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx,
              keys);
        } else {
          prepare16DescNullLastSignConsideredHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx,
              keys);
        }
      }
    } else {
      if (context.asc[curSortKeyIdx]) {
        if (context.nullFirst[curSortKeyIdx]) {
          prepare16AscNullFirstHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        } else {
          prepare16AscNullLastHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        }
      } else {
        if (context.nullFirst[curSortKeyIdx]) {
          prepare16DescNullFirstHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        } else {
          prepare16DescNullLastHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        }
      }
    }
    context.histogramPrepareTime += System.currentTimeMillis() - before;

    // Swap tuples if necessary.
    // If every tuple has the same radix key, tuples don't have to be swapped.

//    if (binEndIdx[0] != exclusiveEnd &&
//        (binEndIdx[32767] != 0 || binEndIdx[32768] != exclusiveEnd)) {

    buildHistogram(context, start, binEndIdx);
    if (Arrays.stream(binEndIdx).filter(eachCount -> eachCount > 0).count() > 1) {
      before = System.currentTimeMillis();
      final int[] binNextElemIdx = new int[_16B_BIN_NUM];
      System.arraycopy(binEndIdx, 0, binNextElemIdx, 0, _16B_BIN_NUM);
      for (int i = start; i < exclusiveEnd; i++) {
        context.out[--binNextElemIdx[keys[i]]] = context.in[i];
      }
      System.arraycopy(context.out, start, context.in, start, exclusiveEnd - start);
      context.swapTime += System.currentTimeMillis() - before;
    }

//    LOG.info("start: " + start + " end: " + exclusiveEnd + " pass: " + pass);
//    final int sortKeyId = context.sortKeyIds[curSortKeyIdx];
//    Arrays.stream(context.in).forEach(t -> LOG.info(t));

    // Recursive call radix sort if necessary.
    if (pass > 0 || curSortKeyIdx < context.maxSortKeyId) {
      boolean nextKey = pass == 0;
      int len = binEndIdx[0] - start;

      if (len > 1) {
        // Use the tim sort when the array length is sufficiently small.
        if (len < TIM_SORT_THRESHOLD) {
          Arrays.sort(context.in, start, binEndIdx[0], context.comparator);
        } else {
          if (nextKey) {
            recursiveCallForNextKey(context, start, binEndIdx[0], curSortKeyIdx++);
          } else {
            signedIntegerMsdRadixSort(context, start, binEndIdx[0], curSortKeyIdx, pass - 2, false);
          }
        }
      }

      for (int i = 0; i < _16B_MAX_BIN_IDX && binEndIdx[i] < exclusiveEnd; i++) {
        len = binEndIdx[i + 1] - binEndIdx[i];
        if (len > 1) {
          // Use the tim sort when the array length is sufficiently small.
          if (len < TIM_SORT_THRESHOLD) {
            Arrays.sort(context.in, binEndIdx[i], binEndIdx[i + 1], context.comparator);
          } else {
            if (nextKey) {
              recursiveCallForNextKey(context, binEndIdx[i], binEndIdx[i + 1], curSortKeyIdx++);
            } else {
              signedIntegerMsdRadixSort(context, binEndIdx[i], binEndIdx[i + 1], curSortKeyIdx, pass - 2, false);
            }
          }
        }
      }
    }
  }

  static int calculateInitialPass(Type type) {
    int initialPass = typeByteSize(type) - 2;
    return initialPass < 0 ? 0 : initialPass;
  }

  static int typeByteSize(Type type) {
    switch (type) {
      case INT2:
        return 2;
      case INT4:
        return 4;
      case INT8:
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
      default:
        throw new TajoRuntimeException(new UnsupportedException(type.name()));
    }
  }

  /**
   * Returns whether this implementation supports the given type or not.
   *
   * @param sortSpec
   * @return
   */
  public static boolean isApplicableType(SortSpec sortSpec) {
    switch (sortSpec.getSortKey().getDataType().getType()) {
      // Variable length types are not supported.
      case CHAR:
      case TEXT:
      case BLOB:
        return false;
      // 1 byte types are not supported.
      case BOOLEAN:
      case BIT:
        return false;
      // float types are not supported because the implementation can cause too many branch misses,
      // so it is difficult to expect a better performance than Tim sort.
      case FLOAT4:
      case FLOAT8:
        return false;
      default:
        return true;
    }
  }
}
