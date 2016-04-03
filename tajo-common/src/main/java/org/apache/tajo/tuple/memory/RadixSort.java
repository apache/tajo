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

import io.netty.util.internal.PlatformDependent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.util.SizeOf;
import sun.misc.Contended;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

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
  }

  private final static int _16B_BIN_NUM = 65536;
  private final static int _16B_MAX_BIN_IDX = 65535;
  private final static int TIM_SORT_THRESHOLD = 65535;

  public static List<UnSafeTuple> msdRadixSort(UnSafeTupleList list, int[] sortKeyIds, Type[] sortKeyTypes,
                                               boolean[] asc, boolean[] nullFirst, Comparator<UnSafeTuple> comp) {
    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);
    RadixSortContext context = new RadixSortContext(in, sortKeyIds, sortKeyTypes, asc, nullFirst, comp);

    long before = System.currentTimeMillis();
    msdRadixSort(context, 0, in.length, 0, calculateInitialPass(sortKeyTypes[0]), true);
    context.msdRadixSortTime += System.currentTimeMillis() - before;
    context.printMsdStat();
    ListIterator<UnSafeTuple> it = list.listIterator();
    for (UnSafeTuple t : context.in) {
      it.next();
      it.set(t);
    }
    return list;
  }

//  public static List<UnSafeTuple> lsdRadixSort(UnSafeTupleList list, int[] sortKeyIds, Type[] sortKeyTypes,
//                                               boolean[] asc, boolean[] nullFirst, Comparator<UnSafeTuple> comp) {
//    UnSafeTuple[] in = list.toArray(new UnSafeTuple[list.size()]);
//
//    RadixSortContext context = new RadixSortContext(in, sortKeyIds, sortKeyTypes, asc, nullFirst, comp);
//    long before = System.currentTimeMillis();
//    lsdRadixSort(context);
//    context.lsdRadixSortTime += System.currentTimeMillis() - before;
//    context.printLsdStat();
//    ListIterator<UnSafeTuple> it = list.listIterator();
//    for (UnSafeTuple t : context.positiveIn) {
//      it.next();
//      it.set(t);
//    }
//    return list;
//  }

  private static int getFieldOffset(long address, int fieldId) {
    return PlatformDependent.getInt(address + (long)(SizeOf.SIZE_OF_INT + (fieldId * SizeOf.SIZE_OF_INT)));
  }

  private static long getFieldAddr(long address, int fieldId) {
    return address + getFieldOffset(address, fieldId);
  }

  static int ascNullLastSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) + 32768;
    }
    return key;
  }
  static int ascNullFirstSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 0; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) + 32768;
    }
    return key;
  }

  static int descNullLastSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = 32767 - PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass));
    }
    return key;
  }
  static int descNullFirstSignConsidered16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 0; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = 32767 - PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass));
    }
    return key;
  }

  static int ascNullLast16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
    }
    return key;
  }

  static int ascNullFirst16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 0; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
    }
    return key;
  }

  static int descNullLast16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = _16B_MAX_BIN_IDX; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = _16B_MAX_BIN_IDX - PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
    }
    return key;
  }

  static int descNullFirst16RadixKey(UnSafeTuple tuple, int sortKeyId, int pass) {
    int key = 0; // for null
    if (!tuple.isBlankOrNull(sortKeyId)) {
      key = _16B_MAX_BIN_IDX - PlatformDependent.getShort(getFieldAddr(tuple.address(), sortKeyId) + (pass)) & 0xFFFF;
    }
    return key;
  }

  static void build16AscNullLastSignConsideredHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
                                                        int[] positions, int[] keys) {
    long before = System.currentTimeMillis();
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = ascNullLastSignConsidered16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
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

  static void build16AscNullFirstSignConsideredHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
                                                        int[] positions, int[] keys) {
    long before = System.currentTimeMillis();
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = ascNullFirstSignConsidered16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
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

  static void build16DescNullLastSignConsideredHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
                                                        int[] positions, int[] keys) {
    long before = System.currentTimeMillis();
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = descNullLastSignConsidered16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
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

  static void build16DescNullFirstSignConsideredHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
                                                         int[] positions, int[] keys) {
    long before = System.currentTimeMillis();
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = descNullFirstSignConsidered16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
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

  static void build16AscNullLastHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
                                          int[] positions, int[] keys) {
    long before = System.currentTimeMillis();
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = ascNullLast16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
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

  static void build16AscNullFirstHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
                                          int[] positions, int[] keys) {
    long before = System.currentTimeMillis();
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = ascNullFirst16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
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

  static void build16DescNullLastHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
                                          int[] positions, int[] keys) {
    long before = System.currentTimeMillis();
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = descNullLast16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
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

  static void build16DescNullFirstHistogram(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass,
                                           int[] positions, int[] keys) {
    long before = System.currentTimeMillis();
    for (int i = start; i < exclusiveEnd; i++) {
      keys[i] = descNullFirst16RadixKey(context.in[i], context.sortKeyIds[curSortKeyIdx], pass);
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

  static void msdRadixSort(RadixSortContext context, int start, int exclusiveEnd, int curSortKeyIdx, int pass, boolean considerSign) {
    context.msdRadixSortCall++;
    final int[] binEndIdx = new int[_16B_BIN_NUM];
    final int[] keys = context.keys;

    // Make histogram
    long before = System.currentTimeMillis();
    if (considerSign) {
      if (context.asc[curSortKeyIdx]) {
        if (context.nullFirst[curSortKeyIdx]) {
          build16AscNullFirstSignConsideredHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        } else {
          build16AscNullLastSignConsideredHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        }
      } else {
        if (context.nullFirst[curSortKeyIdx]) {
          build16DescNullFirstSignConsideredHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        } else {
          build16DescNullLastSignConsideredHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        }
      }
    } else {
      if (context.asc[curSortKeyIdx]) {
        if (context.nullFirst[curSortKeyIdx]) {
          build16AscNullFirstHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        } else {
          build16AscNullLastHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        }
      } else {
        if (context.nullFirst[curSortKeyIdx]) {
          build16DescNullFirstHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        } else {
          build16DescNullLastHistogram(context, start, exclusiveEnd, curSortKeyIdx, pass, binEndIdx, keys);
        }
      }
    }
    context.histogramBuildTime += System.currentTimeMillis() - before;

    if (binEndIdx[0] != exclusiveEnd &&
        (binEndIdx[32767] != 0 || binEndIdx[32768] != exclusiveEnd)) { // TODO: consider positive and negative
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
      boolean nextConsiderSign = false;
      if (pass > 0) {
        nextPass = pass - 2;
      } else {
        nextPass = typeByteSize(context.sortKeyTypes[++curSortKeyIdx]) - 2;
        nextConsiderSign = true;
      }

      int len = binEndIdx[0] - start;

      if (len > 1) {
        if (len < TIM_SORT_THRESHOLD) {
          Arrays.sort(context.in, start, binEndIdx[0], context.comparator);
        } else {
          msdRadixSort(context, start, binEndIdx[0], curSortKeyIdx, nextPass, nextConsiderSign);
        }
      }

      for (int i = 0; i < _16B_MAX_BIN_IDX && binEndIdx[i] < exclusiveEnd; i++) {
        len = binEndIdx[i + 1] - binEndIdx[i];
        if (len > 1) {
          if (len < TIM_SORT_THRESHOLD) {
            Arrays.sort(context.in, binEndIdx[i], binEndIdx[i + 1], context.comparator);
          } else {
            msdRadixSort(context, binEndIdx[i], binEndIdx[i + 1], curSortKeyIdx, nextPass, nextConsiderSign);
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
      case BOOLEAN:
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
      case CHAR:
      default:
        return -1;
    }
  }

  public static boolean isApplicableType(Type type) {
    switch (type) {
      case CHAR:
      case TEXT:
      case BLOB:
        return false;
      default:
        return true;
    }
  }
}
