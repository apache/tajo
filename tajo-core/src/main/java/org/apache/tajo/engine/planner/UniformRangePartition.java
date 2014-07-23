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

package org.apache.tajo.engine.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.exception.RangeOverflowException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.BytesUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;


public class UniformRangePartition extends RangePartitionAlgorithm {
  private int variableId;
  private BigInteger[] cardForEachDigit;
  private BigInteger[] colCards;

  /**
   *
   * @param totalRange
   * @param sortSpecs The description of sort keys
   * @param inclusive true if the end of the range is inclusive
   */
  public UniformRangePartition(final TupleRange totalRange, final SortSpec[] sortSpecs, boolean inclusive) {
    super(sortSpecs, totalRange, inclusive);
    colCards = new BigInteger[sortSpecs.length];

    normalize(sortSpecs, this.mergedRange);

    for (int i = 0; i < sortSpecs.length; i++) {
      colCards[i] = computeCardinality(sortSpecs[i].getSortKey().getDataType(), totalRange.getStart().get(i),
          totalRange.getEnd().get(i), inclusive, sortSpecs[i].isAscending());
    }

    cardForEachDigit = new BigInteger[colCards.length];
    for (int i = 0; i < colCards.length ; i++) {
      if (i == 0) {
        cardForEachDigit[i] = colCards[i];
      } else {
        cardForEachDigit[i] = cardForEachDigit[i - 1].multiply(colCards[i]);
      }
    }
  }

  public UniformRangePartition(TupleRange range, SortSpec [] sortSpecs) {
    this(range, sortSpecs, true);
  }

  @Override
  public TupleRange[] partition(int partNum) {
    Preconditions.checkArgument(partNum > 0,
        "The number of partitions must be positive, but the given number: "
            + partNum);
    Preconditions.checkArgument(totalCard.compareTo(BigInteger.valueOf(partNum)) >= 0,
        "the number of partition cannot exceed total cardinality (" + totalCard + ")");

    int varId;
    for (varId = 0; varId < cardForEachDigit.length; varId++) {
      if (cardForEachDigit[varId].compareTo(BigInteger.valueOf(partNum)) >= 0)
        break;
    }
    this.variableId = varId;

    BigInteger [] reverseCardsForDigit = new BigInteger[variableId+1];
    for (int i = variableId; i >= 0; i--) {
      if (i == variableId) {
        reverseCardsForDigit[i] = colCards[i];
      } else {
        reverseCardsForDigit[i] = reverseCardsForDigit[i+1].multiply(colCards[i]);
      }
    }

    List<TupleRange> ranges = Lists.newArrayList();

    BigDecimal x = new BigDecimal(reverseCardsForDigit[0]);

    BigInteger term = x.divide(BigDecimal.valueOf(partNum), RoundingMode.CEILING).toBigInteger();
    BigInteger reminder = reverseCardsForDigit[0];
    Tuple last = mergedRange.getStart();
    TupleRange tupleRange;
    while(reminder.compareTo(BigInteger.ZERO) > 0) {
      if (reminder.compareTo(term) <= 0) { // final one is inclusive
        tupleRange = new TupleRange(sortSpecs, last, mergedRange.getEnd());
      } else {
        Tuple next = increment(last, term, variableId);
        tupleRange = new TupleRange(sortSpecs, last, next);
      }

      ranges.add(tupleRange);
      last = ranges.get(ranges.size() - 1).getEnd();
      reminder = reminder.subtract(term);
    }

    for (TupleRange r : ranges) {
      denormalize(sortSpecs, r);
    }

    return ranges.toArray(new TupleRange[ranges.size()]);
  }

  /**
   * It normalizes the start and end keys to have the same length bytes if they are texts or bytes.
   *
   * @param sortSpecs The sort specs
   * @param range Tuple range to be normalize
   */
  public static void normalize(final SortSpec [] sortSpecs, TupleRange range) {
    // normalize text fields to have same bytes length
    for (int i = 0; i < sortSpecs.length; i++) {
      if (sortSpecs[i].getSortKey().getDataType().getType() == TajoDataTypes.Type.TEXT) {
        byte [] startBytes;
        byte [] endBytes;
        if (range.getStart().isNull(i)) {
          startBytes = BigInteger.ZERO.toByteArray();
        } else {
          startBytes = range.getStart().getBytes(i);
        }

        if (range.getEnd().isNull(i)) {
          endBytes = BigInteger.ZERO.toByteArray();
        } else {
          endBytes = range.getEnd().getBytes(i);
        }

        byte [][] padded = BytesUtils.padBytes(startBytes, endBytes);
        range.getStart().put(i, DatumFactory.createText(padded[0]));
        range.getEnd().put(i, DatumFactory.createText(padded[1]));
      }
    }
  }

  /**
   * Normalized keys have padding values, but it will cause the key mismatch in pull server.
   * So, it denormalize the normalized keys again.
   *
   * @param sortSpecs The sort specs
   * @param range Tuple range to be denormalized
   */
  public static void denormalize(SortSpec [] sortSpecs, TupleRange range) {
    for (int i = 0; i < sortSpecs.length; i++) {
      if (sortSpecs[i].getSortKey().getDataType().getType() == TajoDataTypes.Type.TEXT) {
        range.getStart().put(i,DatumFactory.createText(BytesUtils.trimBytes(range.getStart().getBytes(i))));
        range.getEnd().put(i,DatumFactory.createText(BytesUtils.trimBytes(range.getEnd().getBytes(i))));
      }
    }
  }

  /**
  *  Check whether an overflow occurs or not.
   *
   * @param colId The column id to be checked
   * @param last
   * @param inc
   * @param sortSpecs
   * @return
   */
  public boolean isOverflow(int colId, Datum last, BigInteger inc, SortSpec [] sortSpecs) {
    Column column = sortSpecs[colId].getSortKey();
    BigDecimal incDecimal = new BigDecimal(inc);
    BigDecimal candidate;
    boolean overflow = false;

    switch (column.getDataType().getType()) {
      case BIT: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(last.asByte()));
          return new BigDecimal(mergedRange.getEnd().get(colId).asByte()).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(last.asByte()).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().get(colId).asByte())) < 0;
        }
      }
      case CHAR: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal((int)last.asChar()));
          return new BigDecimal((int) mergedRange.getEnd().get(colId).asChar()).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal((int)last.asChar()).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal((int) mergedRange.getEnd().get(colId).asChar())) < 0;
        }
      }
      case INT2: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(last.asInt2()));
          return new BigDecimal(mergedRange.getEnd().get(colId).asInt2()).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(last.asInt2()).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().get(colId).asInt2())) < 0;
        }
      }
      case DATE:
      case INT4: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(last.asInt4()));
          return new BigDecimal(mergedRange.getEnd().get(colId).asInt4()).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(last.asInt4()).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().get(colId).asInt4())) < 0;
        }
      }
      case TIME:
      case TIMESTAMP:
      case INT8: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(last.asInt8()));
          return new BigDecimal(mergedRange.getEnd().get(colId).asInt8()).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(last.asInt8()).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().get(colId).asInt8())) < 0;
        }
      }
      case FLOAT4: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(last.asFloat4()));
          return new BigDecimal(mergedRange.getEnd().get(colId).asFloat4()).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(last.asFloat4()).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().get(colId).asFloat4())) < 0;
        }
      }
      case FLOAT8: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(last.asFloat8()));
          return new BigDecimal(mergedRange.getEnd().get(colId).asFloat8()).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(last.asFloat8()).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().get(colId).asFloat8())) < 0;
        }

      }
      case TEXT: {
        byte [] lastBytes = last.asByteArray();
        byte [] endBytes = mergedRange.getEnd().getBytes(colId);

        Preconditions.checkState(lastBytes.length == endBytes.length);

        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(new BigInteger(lastBytes)));
          return new BigDecimal(new BigInteger(endBytes)).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(new BigInteger(lastBytes)).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(new BigInteger(endBytes))) < 0;
        }
      }
      case INET4: {
        int candidateIntVal;
        byte[] candidateBytesVal = new byte[4];
        if (sortSpecs[colId].isAscending()) {
          candidateIntVal = incDecimal.intValue() + last.asInt4();
          if (candidateIntVal - incDecimal.intValue() != last.asInt4()) {
            return true;
          }
          Bytes.putInt(candidateBytesVal, 0, candidateIntVal);
          return Bytes.compareTo(mergedRange.getEnd().get(colId).asByteArray(), candidateBytesVal) < 0;
        } else {
          candidateIntVal = last.asInt4() - incDecimal.intValue();
          if (candidateIntVal + incDecimal.intValue() != last.asInt4()) {
            return true;
          }
          Bytes.putInt(candidateBytesVal, 0, candidateIntVal);
          return Bytes.compareTo(candidateBytesVal, mergedRange.getEnd().get(colId).asByteArray()) < 0;
        }
      }
    }
    return overflow;
  }

  public long incrementAndGetReminder(int colId, Datum last, long inc) {
    Column column = sortSpecs[colId].getSortKey();
    long reminder = 0;
    switch (column.getDataType().getType()) {
      case BIT: {
        long candidate = last.asByte() + inc;
        byte end = mergedRange.getEnd().get(colId).asByte();
        reminder = candidate - end;
        break;
      }
      case CHAR: {
        long candidate = last.asChar() + inc;
        char end = mergedRange.getEnd().get(colId).asChar();
        reminder = candidate - end;
        break;
      }
      case DATE:
      case INT4: {
        int candidate = (int) (last.asInt4() + inc);
        int end = mergedRange.getEnd().get(colId).asInt4();
        reminder = candidate - end;
        break;
      }
      case TIME:
      case TIMESTAMP:
      case INT8:
      case INET4: {
        long candidate = last.asInt8() + inc;
        long end = mergedRange.getEnd().get(colId).asInt8();
        reminder = candidate - end;
        break;
      }
      case FLOAT4: {
        float candidate = last.asFloat4() + inc;
        float end = mergedRange.getEnd().get(colId).asFloat4();
        reminder = (long) (candidate - end);
        break;
      }
      case FLOAT8: {
        double candidate = last.asFloat8() + inc;
        double end = mergedRange.getEnd().get(colId).asFloat8();
        reminder = (long) Math.ceil(candidate - end);
        break;
      }
      case TEXT: {
        byte [] lastBytes = last.asByteArray();
        byte [] endBytes = mergedRange.getEnd().get(colId).asByteArray();

        Preconditions.checkState(lastBytes.length == endBytes.length);

        BigInteger lastBInt = new BigInteger(lastBytes);
        BigInteger endBInt = new BigInteger(endBytes);
        BigInteger incBInt = BigInteger.valueOf(inc);

        BigInteger candidate = lastBInt.add(incBInt);
        reminder = candidate.subtract(endBInt).longValue();
        break;
      }
    }

    // including zero
    return reminder - 1;
  }

  /**
   *
   * @param last
   * @param interval
   * @return
   */
  public Tuple increment(final Tuple last, BigInteger interval, final int baseDigit) {
    BigInteger [] incs = new BigInteger[last.size()];
    boolean [] overflowFlag = new boolean[last.size()];
    BigInteger [] result;
    BigInteger value = interval;

    BigInteger [] reverseCardsForDigit = new BigInteger[baseDigit + 1];
    for (int i = baseDigit; i >= 0; i--) {
      if (i == baseDigit) {
        reverseCardsForDigit[i] = colCards[i];
      } else {
        reverseCardsForDigit[i] = reverseCardsForDigit[i+1].multiply(colCards[i]);
      }
    }

    for (int i = 0; i < baseDigit; i++) {
      result = value.divideAndRemainder(reverseCardsForDigit[i + 1]);
      incs[i] = result[0];
      value = result[1];
    }
    int finalId = baseDigit;
    incs[finalId] = value;
    for (int i = finalId; i >= 0; i--) {
      if (isOverflow(i, last.get(i), incs[i], sortSpecs)) {
        if (i == 0) {
          throw new RangeOverflowException(mergedRange, last, incs[i].longValue(), sortSpecs[i].isAscending());
        }
        long rem = incrementAndGetReminder(i, last.get(i), value.longValue());
        incs[i] = BigInteger.valueOf(rem);
        incs[i - 1] = incs[i-1].add(BigInteger.ONE);
        overflowFlag[i] = true;
      } else {
        if (i > 0) {
          incs[i] = value;
          break;
        }
      }
    }

    for (int i = 0; i < incs.length; i++) {
      if (incs[i] == null) {
        incs[i] = BigInteger.ZERO;
      }
    }

    Tuple end = new VTuple(sortSpecs.length);
    Column column;
    for (int i = 0; i < last.size(); i++) {
      column = sortSpecs[i].getSortKey();
      switch (column.getDataType().getType()) {
        case CHAR:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createChar((char) (mergedRange.getStart().get(i).asChar() + incs[i].longValue())));
          } else {
            end.put(i, DatumFactory.createChar((char) (last.get(i).asChar() + incs[i].longValue())));
          }
          break;
        case BIT:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createBit(
                (byte) (mergedRange.getStart().get(i).asByte() + incs[i].longValue())));
          } else {
            end.put(i, DatumFactory.createBit((byte) (last.get(i).asByte() + incs[i].longValue())));
          }
          break;
        case INT2:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createInt2(
                (short) (mergedRange.getStart().get(i).asInt2() + incs[i].longValue())));
          } else {
            end.put(i, DatumFactory.createInt2((short) (last.get(i).asInt2() + incs[i].longValue())));
          }
          break;
        case INT4:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createInt4(
                (int) (mergedRange.getStart().get(i).asInt4() + incs[i].longValue())));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createInt4((int) (last.get(i).asInt4() + incs[i].longValue())));
            } else {
              end.put(i, DatumFactory.createInt4((int) (last.get(i).asInt4() - incs[i].longValue())));
            }
          }
          break;
        case INT8:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createInt8(
                mergedRange.getStart().get(i).asInt8() + incs[i].longValue()));
          } else {
            end.put(i, DatumFactory.createInt8(last.get(i).asInt8() + incs[i].longValue()));
          }
          break;
        case FLOAT4:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createFloat4(
                mergedRange.getStart().get(i).asFloat4() + incs[i].longValue()));
          } else {
            end.put(i, DatumFactory.createFloat4(last.get(i).asFloat4() + incs[i].longValue()));
          }
          break;
        case FLOAT8:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createFloat8(
                mergedRange.getStart().get(i).asFloat8() + incs[i].longValue()));
          } else {
            end.put(i, DatumFactory.createFloat8(last.get(i).asFloat8() + incs[i].longValue()));
          }
          break;
        case TEXT:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createText(((char) (mergedRange.getStart().get(i).asChars().charAt(0)
                + incs[i].longValue())) + ""));
          } else {
            BigInteger lastBigInt;
            if (last.isNull(i)) {
              lastBigInt = BigInteger.valueOf(0);
            } else {
              lastBigInt = UnsignedLong.valueOf(new BigInteger(last.get(i).asByteArray())).bigIntegerValue();
            }
            end.put(i, DatumFactory.createText(lastBigInt.add(incs[i]).toByteArray()));
          }
          break;
        case DATE:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createDate((int) (mergedRange.getStart().get(i).asInt4() + incs[i].longValue())));
          } else {
            end.put(i, DatumFactory.createDate((int) (last.get(i).asInt4() + incs[i].longValue())));
          }
          break;
        case TIME:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createTime(mergedRange.getStart().get(i).asInt8() + incs[i].longValue()));
          } else {
            end.put(i, DatumFactory.createTime(last.get(i).asInt8() + incs[i].longValue()));
          }
          break;
        case TIMESTAMP:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createTimestmpDatumWithJavaMillis(
                mergedRange.getStart().get(i).asInt8() + incs[i].longValue()));
          } else {
            end.put(i, DatumFactory.createTimestmpDatumWithJavaMillis(last.get(i).asInt8() + incs[i].longValue()));
          }
          break;
        case INET4:
          byte[] ipBytes;
          if (overflowFlag[i]) {
            ipBytes = mergedRange.getStart().get(i).asByteArray();
            assert ipBytes.length == 4;
            end.put(i, DatumFactory.createInet4(ipBytes));
          } else {
            int lastVal = last.get(i).asInt4() + incs[i].intValue();
            ipBytes = new byte[4];
            Bytes.putInt(ipBytes, 0, lastVal);
            end.put(i, DatumFactory.createInet4(ipBytes));
          }
          break;
        default:
          throw new UnsupportedOperationException(column.getDataType() + " is not supported yet");
      }
    }

    return end;
  }
}
