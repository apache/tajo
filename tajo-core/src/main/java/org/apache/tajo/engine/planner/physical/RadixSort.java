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
import org.apache.tajo.common.type.TajoTypeUtil;
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

    // If the number of tuples to be sorted does not exceed this value, Tim sort is used.
    // The default value is 65536 which is got from some experiments.
    final int timSortThreshold;

    long msdRadixSortTime = 0;
    long histogramPrepareTime = 0;
    long swapTime = 0;
    long histogramBuildTime = 0;
    int msdRadixSortCall = 0;

    public RadixSortContext(UnSafeTuple[] in, Schema schema, SortSpec[] sortSpecs, Comparator<UnSafeTuple> comparator,
                            int timSortThreshold) {
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
      this.timSortThreshold = timSortThreshold;
    }

    public void printStat() {
      LOG.info("- msdRadixSortTime: " + msdRadixSortTime + " ms");
      LOG.info("\t|- histogramPrepareTime: " + histogramPrepareTime + " ms");
      LOG.info("\t\t|- histogramBuildTime: " + histogramBuildTime + " ms");
      LOG.info("\t|- swapTime: " + swapTime + " ms");
      LOG.info("- msdRadixSortCall: " + msdRadixSortCall + " times");
    }
  }

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
    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);
    RadixSortContext context = new RadixSortContext(in, schema, sortSpecs, comp,
        queryContext.getInt(SessionVars.TEST_TIM_SORT_THRESHOLD_FOR_RADIX_SORT));

    long before = System.currentTimeMillis();
    recursiveCallForNextKey(context, 0, context.in.length, 0);
    context.msdRadixSortTime += System.currentTimeMillis() - before;
    context.printStat();
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : context.in) {
      it.next();
      it.set(t);
    }
    return list;
  }

  static void recursiveCallForNextKey(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx) {
    if (needConsiderSign(context.sortKeyTypes[curSortKeyIdx])) {
      if (TajoTypeUtil.isReal(context.sortKeyTypes[curSortKeyIdx])) {
        msdTernaryRadixSort(context, start, exclusiveEnd, curSortKeyIdx, context.asc[curSortKeyIdx],
            calculateInitialPass(context.sortKeyTypes[curSortKeyIdx]));
      } else {
        msdRadixSort(context, start, exclusiveEnd, curSortKeyIdx, context.asc[curSortKeyIdx],
            calculateInitialPass(context.sortKeyTypes[curSortKeyIdx]), true);
      }
    } else {
      msdRadixSort(context, start, exclusiveEnd, curSortKeyIdx, context.asc[curSortKeyIdx],
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
      case FLOAT4:
      case FLOAT8:
        return true;
      case DATE:
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
    int key = _16BIT_NULL_LAST_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // For negative values, the key should be 1 ~ 32768. For positive values, the key should be 32769 ~ 65536.
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) + _16BIT_SECOND_HALF_START_IDX;
    }
    return key;
  }

  /**
   * Get a 16-bit radix key from a column values of the given tuple.
   * The sign of the column value should be considered.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int ascNullFirstSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16BIT_NULL_FIRST_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // For negative values, the key should be 1 ~ 32768. For positive values, the key should be 32769 ~ 65536.
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) + _16BIT_SECOND_HALF_START_IDX;
    }
    return key;
  }

  /**
   * Get a 16-bit radix key from a column values of the given tuple.
   * The sign of the column value should be considered.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int descNullLastSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16BIT_NULL_LAST_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // For positive values, the key should be 1 ~ 32768. For negative values, the key should be 32769 ~ 65536.
      key = _16BIT_FIRST_HALF_END_IDX - PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass));
    }
    return key;
  }

  /**
   * Get a 16-bit radix key from a column values of the given tuple.
   * The sign of the column value should be considered.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int descNullFirstSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16BIT_NULL_FIRST_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      // For positive values, the key should be 1 ~ 32768. For negative values, the key should be 32769 ~ 65536.
      key = _16BIT_FIRST_HALF_END_IDX - PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass));
    }
    return key;
  }

  /**
   * Get a 16-bit radix key from a column values of the given tuple.
   * The return key is an unsigned short value.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int ascNullLast16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16BIT_NULL_LAST_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = 1 + (PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & SHORT_UNSIGNED_MASK);
    }
    return key;
  }

  /**
   * Get a 16-bit radix key from a column values of the given tuple.
   * The return key is an unsigned short value.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int ascNullFirst16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16BIT_NULL_FIRST_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = 1 + (PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & SHORT_UNSIGNED_MASK);
    }
    return key;
  }

  /**
   * Get a 16-bit radix key from a column values of the given tuple.
   * The return key is an unsigned short value.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int descNullLast16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16BIT_NULL_LAST_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = _16BIT_MAX_BIN_IDX - (PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & SHORT_UNSIGNED_MASK);
    }
    return key;
  }

  /**
   * Get a 16-bit radix key from a column values of the given tuple.
   * The return key is an unsigned short value.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int descNullFirst16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16BIT_NULL_FIRST_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = _16BIT_MAX_BIN_IDX - (PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & SHORT_UNSIGNED_MASK);
    }
    return key;
  }

  /**
   * Calculate positions of the input tuples.
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
   * Calculate positions of the input tuples.
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
   * Calculate positions of the input tuples.
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
   * Calculate positions of the input tuples.
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
   * Calculate positions of the input tuples.
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
   * Calculate positions of the input tuples.
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
   * Calculate positions of the input tuples.
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
   * Calculate positions of the input tuples.
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

  private final static int _16BIT_BIN_NUM = 65538;
  private final static int _16BIT_NULL_FIRST_IDX = 0;
  private final static int _16BIT_NULL_LAST_IDX = 65537;
  private final static int _16BIT_MAX_BIN_IDX = 65536;
  private final static int _16BIT_FIRST_HALF_END_IDX = 32768;
  private final static int _16BIT_SECOND_HALF_START_IDX = 32769;
  private final static int SHORT_UNSIGNED_MASK = 0xFFFF;

  /**
   * Sort the specified part of the input tuples.
   * If the length of the part is sufficiently large, recursively call msdRadixSort(). Otherwise, call Arrays.sort().
   *
   * @param context radix sort context
   * @param start start position of the part will be sorted.
   * @param exclusiveEnd end position of the part will be sorted.
   * @param curSortKeyIdx current sort key index
   * @param asc ascending flag
   * @param pass current pass
   * @param considerSign a flag to represent that the sign must be considered
   */
  static void msdRadixSort(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, boolean asc,
                           int pass, boolean considerSign) {
    context.msdRadixSortCall++;

    // This array contains the end positions of bins. For example, suppose an input which consists of nulls and numbers
    // of 1 ~ 9. The number of each numbers and null is 10. If this input is organized into 12 bins which have the equal
    // length of 10, this array will contain the below values.
    //
    // Ex) asc, null first
    //
    //   [0]  [1]  [2]  [3]  [4]  [5]  [6]  [7]  [8]  [9]  [10]  [11] // 0th and 11th bins are reserved for null values
    //   10   20   30   40   50   60   70   80   90   100   100   100
    //
    // If the considerSign flag is set, this array is used for both negative and positive values.
    // The positions of both values are determined by the asc flag.
    // When the asc flag is set, the first half of the array is used for negative values.
    // Otherwise, the second half of the array is used for negative values.
    //
    // Note: too many recursive calls to msdRadixSort() can incur a lot of memory overhead because this array should be
    // always newly created when it is called.
    final int[] binEndIdx = new int[_16BIT_BIN_NUM];

    // An array to cache radix keys which are gotten while building the histogram.
    // Since getting keys is the most expensive part of this implementation, keys should be cached once they are gotten.
    final int[] keys = context.keys;

    // TODO: consider the current key type
    long before = System.currentTimeMillis();
    // Build a histogram.
    // Call different methods depending on the sort spec of the current key. This is to avoid frequent branch
    // mispredictions. This is effective because the below code block is the most expensive part of this implementation.
    // TODO: code generation can simplify the below codes.
    if (considerSign) {
      if (asc) {
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
      if (asc) {
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

    boolean needSwap = Arrays.stream(binEndIdx).filter(eachCount -> eachCount > 0).count() > 1;
    buildHistogram(context, start, binEndIdx);
    if (needSwap) {
      before = System.currentTimeMillis();
      final int[] binNextElemIdx = new int[_16BIT_BIN_NUM];
      System.arraycopy(binEndIdx, 0, binNextElemIdx, 0, _16BIT_BIN_NUM);
      for (int i = start; i < exclusiveEnd; i++) {
        context.out[--binNextElemIdx[keys[i]]] = context.in[i];
      }
      System.arraycopy(context.out, start, context.in, start, exclusiveEnd - start);
      context.swapTime += System.currentTimeMillis() - before;
    }

    // Recursive call radix sort if necessary.
    if (pass > 0 || curSortKeyIdx < context.maxSortKeyId) {
      boolean nextKey = pass == 0;
      int len = binEndIdx[0] - start;

      if (len > 1) {
        // Use the tim sort when the array length is sufficiently small.
        if (len < context.timSortThreshold) {
          Arrays.sort(context.in, start, binEndIdx[0], context.comparator);
        } else {
          if (nextKey) {
            recursiveCallForNextKey(context, start, binEndIdx[0], curSortKeyIdx + 1);
          } else {
            msdRadixSort(context, start, binEndIdx[0], curSortKeyIdx, asc, pass - 2, false);
          }
        }
      }

      for (int i = 0; i < _16BIT_MAX_BIN_IDX && binEndIdx[i] < exclusiveEnd; i++) {
        len = binEndIdx[i + 1] - binEndIdx[i];
        if (len > 1) {
          // Use the tim sort when the array length is sufficiently small.
          if (len < context.timSortThreshold) {
            Arrays.sort(context.in, binEndIdx[i], binEndIdx[i + 1], context.comparator);
          } else {
            if (nextKey) {
              recursiveCallForNextKey(context, binEndIdx[i], binEndIdx[i + 1], curSortKeyIdx + 1);
            } else {
              msdRadixSort(context, binEndIdx[i], binEndIdx[i + 1], curSortKeyIdx, asc, pass - 2, false);
            }
          }
        }
      }
    }
  }

  // Below methods are only used for floating point types.

  /**
   * Get a 1-bit radix key from a column values of the given tuple.
   * The keys of 0 and 3 are reserved for null values.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int ascNullFirst1bRadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 0; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = 2 - ((PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF) >> 15);
    }
    return key;
  }

  /**
   * Get a 1-bit radix key from a column values of the given tuple.
   * The keys of 0 and 3 are reserved for null values.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int ascNullLast1bRadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _1BIT_BIN_MAX_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = 2 - ((PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF) >> 15);
    }
    return key;
  }

  /**
   * Get a 1-bit radix key from a column values of the given tuple.
   * The keys of 0 and 3 are reserved for null values.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int descNullFirst1bRadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 0; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = 1 + ((PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF) >> 15);
    }
    return key;
  }

  /**
   * Get a 1-bit radix key from a column values of the given tuple.
   * The keys of 0 and 3 are reserved for null values.
   *
   * @param tuple
   * @param sortKeyId
   * @param pass
   * @return
   */
  static int descNullLast1bRadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _1BIT_BIN_MAX_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = 1 + ((PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF) >> 15);
    }
    return key;
  }

  /**
   * Calculate positions of the input tuples.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare1bAscNullFirstHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx,
                                             int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = ascNullFirst1bRadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  /**
   * Calculate positions of the input tuples.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare1bAscNullLastHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx,
                                            int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = ascNullLast1bRadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  /**
   * Calculate positions of the input tuples.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare1bDescNullFirstHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx,
                                              int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = descNullFirst1bRadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  /**
   * Calculate positions of the input tuples.
   *
   * @param context
   * @param start
   * @param exclusiveEnd
   * @param curSortKeyIdx
   * @param pass
   * @param positions
   * @param keys
   */
  static void prepare1bDescNullLastHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx,
                                             int pass, int[] positions, int[] keys) {
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = descNullLast1bRadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
      positions[keys[i]] += 1;
    }
  }

  private final static int _1BIT_BIN_NUM = 4;
  private final static int _1BIT_BIN_MAX_IDX = 3;

  /**
   * Sort the specified part of the input tuples when the current sort key has a floating point type.
   * This method is called only once at the first pass.
   *
   * @param context radix sort context
   * @param start start position of the part will be sorted
   * @param exclusiveEnd end position of the part will be sorted
   * @param curSortKeyIdx current sort key index
   * @param asc ascending flag
   * @param pass current pass
   */
  static void msdTernaryRadixSort(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, boolean asc,
                                  int pass) {
    context.msdRadixSortCall++;

    // The values of floating point types are organized into three groups, i.e., positives, negatives, and nulls.
    // The positions for these groups are stored in an integer array of length 4.
    // The first and last slots are reserved for null values.
    // The second and third slots are used for positives and negatives depending on the ascending order specification.
    // If the ascending order is specified, negatives come first. Otherwise, positives come first.
    // Ex) asc, null first
    //
    // [ nulls ] [ negatives ] [ positives ] [ empty ]
    //
    final int[] binEndIdx = new int[_1BIT_BIN_NUM];

    // An array to cache radix keys which are gotten while building the histogram.
    // Since getting keys is the most expensive part of this implementation, keys should be cached once they are gotten.
    final int[] keys = context.keys;

    // TODO: consider the current key type
    long before = System.currentTimeMillis();
    // Build a histogram.
    // Call different methods depending on the sort spec of the current key. This is to avoid frequent branch
    // mispredictions. This is effective because the below code block is the most expensive part of this implementation.
    // TODO: code generation can simplify the below codes.
    if (asc) {
      if (context.nullFirst[curSortKeyIdx]) {
        prepare1bAscNullFirstHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
      } else {
        prepare1bAscNullLastHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
      }
    } else {
      if (context.nullFirst[curSortKeyIdx]) {
        prepare1bDescNullFirstHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
      } else {
        prepare1bDescNullLastHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
      }
    }

    context.histogramPrepareTime += System.currentTimeMillis() - before;

    // Swap tuples if necessary.
    // If every tuple has the same radix key, tuples don't have to be swapped.
    boolean needSwap = Arrays.stream(binEndIdx).filter(eachCount -> eachCount > 0).count() > 1;
    buildHistogram(context, start, binEndIdx);
    if (needSwap) {
      before = System.currentTimeMillis();
      final int[] binNextElemIdx = new int[_1BIT_BIN_NUM];
      System.arraycopy(binEndIdx, 0, binNextElemIdx, 0, _1BIT_BIN_NUM);
      for (int i = start; i < exclusiveEnd; i++) {
        context.out[--binNextElemIdx[keys[i]]] = context.in[i];
      }
      System.arraycopy(context.out, start, context.in, start, exclusiveEnd - start);
      context.swapTime += System.currentTimeMillis() - before;
    }

    // Recursively call radix sort
    if (context.nullFirst[curSortKeyIdx]) {
      // The bin with null values doesn't have to be sorted anymore. As a result, call sort for the next key directly.
      if (curSortKeyIdx < context.maxSortKeyId) {
        recursiveCallForNextKey(context, start, binEndIdx[0], curSortKeyIdx + 1);
      }
    } else {
      // The bin with null values doesn't have to be sorted anymore. As a result, call sort for the next key directly.
      if (curSortKeyIdx < context.maxSortKeyId) {
        recursiveCallForNextKey(context, binEndIdx[2], binEndIdx[3], curSortKeyIdx + 1);
      }
    }

    int len = binEndIdx[1] - binEndIdx[0];

    if (len > 1) {
      // Use the tim sort when the array length is sufficiently small.
      if (len < context.timSortThreshold) {
        Arrays.sort(context.in, binEndIdx[0], binEndIdx[1], context.comparator);
      } else {
        msdRadixSort(context, binEndIdx[0], binEndIdx[1], curSortKeyIdx, false, pass, false);
      }
    }

    len = binEndIdx[2] - binEndIdx[1];

    if (len > 1) {
      // Use the tim sort when the array length is sufficiently small.
      if (len < context.timSortThreshold) {
        Arrays.sort(context.in, binEndIdx[1], binEndIdx[2], context.comparator);
      } else {
        msdRadixSort(context, binEndIdx[1], binEndIdx[2], curSortKeyIdx, true, pass, false);
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
      case FLOAT4:
        return 4;
      case INT8:
      case FLOAT8:
        return 8;
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
      default:
        return true;
    }
  }
}
