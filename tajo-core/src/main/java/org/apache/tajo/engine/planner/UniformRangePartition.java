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
import com.sun.tools.javac.util.Convert;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.exception.RangeOverflowException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.BytesUtils;
import org.apache.tajo.util.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;

/**
 * It serializes multiple sort key spaces into one dimension space by regarding key spaces as
 * arbitrary base number systems respectively.
 */
public class UniformRangePartition extends RangePartitionAlgorithm {
  private final TupleRange originalRange;
  private int variableId;
  private BigInteger[] cardForEachDigit;
  private BigInteger[] colCards;
  private boolean [] isPureAscii; // flags to indicate if i'th key contains pure ascii characters.
  private boolean [] beginNulls; // flags to indicate if i'th begin value is null.
  private boolean [] endNulls; // flags to indicate if i'th begin value is null.

  /**
   *
   * @param entireRange
   * @param sortSpecs The description of sort keys
   * @param inclusive true if the end of the range is inclusive
   */
  public UniformRangePartition(final TupleRange entireRange, final SortSpec[] sortSpecs, boolean inclusive) {
    super(sortSpecs, entireRange, inclusive);

    this.originalRange = entireRange;
    beginNulls = new boolean[sortSpecs.length];
    endNulls = new boolean[sortSpecs.length];

    // filling pure ascii flags
    isPureAscii = new boolean[sortSpecs.length];
    for (int i = 0; i < sortSpecs.length; i++) {
      String startValue = entireRange.getStart().getText(i);
      String endValue = entireRange.getEnd().getText(i);
      isPureAscii[i] = StringUtils.isPureAscii(startValue) && StringUtils.isPureAscii(endValue);
      beginNulls[i] = entireRange.getStart().isBlankOrNull(i);
      endNulls[i] = entireRange.getEnd().isBlankOrNull(i);
    }

    colCards = new BigInteger[sortSpecs.length];
    normalize(sortSpecs, this.mergedRange);

    for (int i = 0; i < sortSpecs.length; i++) {
      colCards[i] =  computeCardinality(sortSpecs[i].getSortKey().getDataType(), entireRange, i,
          inclusive, sortSpecs[i].isAscending());
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

    // Recovering the transformed same bytes tuples into the original start and end keys
    ranges.get(0).setStart(originalRange.getStart());
    ranges.get(ranges.size() - 1).setEnd(originalRange.getEnd());

    // Ensure all keys are totally ordered in a right order.
    for (int i = 0; i < ranges.size(); i++) {
      if (i > 1) {
        Preconditions.checkState(ranges.get(i - 2).compareTo(ranges.get(i - 1)) < 0,
            "Computed ranges are not totally ordered. Previous key=" + ranges.get(i - 2) + ", Current Key="
                + ranges.get(i - 1));
      }
    }

    return ranges.toArray(new TupleRange[ranges.size()]);
  }

  /**
   * It normalizes the start and end keys to have the same length bytes if they are texts or bytes.
   *
   * @param sortSpecs The sort specs
   * @param range Tuple range to be normalize
   */
  public void normalize(final SortSpec [] sortSpecs, TupleRange range) {
    // normalize text fields to have same bytes length
    for (int i = 0; i < sortSpecs.length; i++) {
      if (sortSpecs[i].getSortKey().getDataType().getType() == TajoDataTypes.Type.TEXT) {
        if (isPureAscii[i]) {
          byte[] startBytes;
          byte[] endBytes;
          if (range.getStart().isBlankOrNull(i)) {
            startBytes = BigInteger.ZERO.toByteArray();
          } else {
            startBytes = range.getStart().getBytes(i);
          }

          if (range.getEnd().isBlankOrNull(i)) {
            endBytes = BigInteger.ZERO.toByteArray();
          } else {
            endBytes = range.getEnd().getBytes(i);
          }

          byte[][] padded = BytesUtils.padBytes(startBytes, endBytes);
          range.getStart().put(i, DatumFactory.createText(padded[0]));
          range.getEnd().put(i, DatumFactory.createText(padded[1]));

        } else {
          char[] startChars;
          char[] endChars;
          if (range.getStart().isBlankOrNull(i)) {
            startChars = new char[] {0};
          } else {
            startChars = range.getStart().getUnicodeChars(i);
          }

          if (range.getEnd().isBlankOrNull(i)) {
            endChars = new char[] {0};
          } else {
            endChars = range.getEnd().getUnicodeChars(i);
          }

          char[][] padded = StringUtils.padChars(startChars, endChars);
          range.getStart().put(i, DatumFactory.createText(new String(padded[0])));
          range.getEnd().put(i, DatumFactory.createText(new String(padded[1])));
        }
      }
    }
  }

  /**
  *  Check whether an overflow occurs or not.
   *
   * @param colId The column id to be checked
   * @param tuple
   * @param inc
   * @param sortSpecs
   * @return
   */
  public boolean isOverflow(int colId, Tuple tuple, int i, BigInteger inc, SortSpec [] sortSpecs) {
    Column column = sortSpecs[colId].getSortKey();
    BigDecimal incDecimal = new BigDecimal(inc);
    BigDecimal candidate;
    boolean overflow = false;

    switch (column.getDataType().getType()) {
      case BIT: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(tuple.getByte(i)));
          return new BigDecimal(mergedRange.getEnd().getByte(colId)).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(tuple.getByte(i)).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().getByte(colId))) < 0;
        }
      }
      case CHAR: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal((int)tuple.getChar(i)));
          return new BigDecimal((int) mergedRange.getEnd().getChar(colId)).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal((int)tuple.getChar(i)).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal((int) mergedRange.getEnd().getChar(colId))) < 0;
        }
      }
      case INT2: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(tuple.getInt2(i)));
          return new BigDecimal(mergedRange.getEnd().getInt2(colId)).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(tuple.getInt2(i)).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().getInt2(colId))) < 0;
        }
      }
      case DATE:
      case INT4: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(tuple.getInt4(i)));
          return new BigDecimal(mergedRange.getEnd().getInt4(colId)).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(tuple.getInt4(i)).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().getInt4(colId))) < 0;
        }
      }
      case TIME:
      case TIMESTAMP:
      case INT8: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(tuple.getInt8(i)));
          return new BigDecimal(mergedRange.getEnd().getInt8(colId)).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(tuple.getInt8(i)).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().getInt8(colId))) < 0;
        }
      }
      case FLOAT4: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(tuple.getFloat4(i)));
          return new BigDecimal(mergedRange.getEnd().getFloat4(colId)).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(tuple.getFloat4(i)).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().getFloat4(colId))) < 0;
        }
      }
      case FLOAT8: {
        if (sortSpecs[colId].isAscending()) {
          candidate = incDecimal.add(new BigDecimal(tuple.getFloat8(i)));
          return new BigDecimal(mergedRange.getEnd().getFloat8(colId)).compareTo(candidate) < 0;
        } else {
          candidate = new BigDecimal(tuple.getFloat8(i)).subtract(incDecimal);
          return candidate.compareTo(new BigDecimal(mergedRange.getEnd().getFloat8(colId))) < 0;
        }

      }
      case TEXT: {
        if (isPureAscii[colId]) {
          byte[] lastBytes = tuple.getBytes(i);
          byte[] endBytes = mergedRange.getEnd().getBytes(colId);

          Preconditions.checkState(lastBytes.length == endBytes.length);

          if (sortSpecs[colId].isAscending()) {
            candidate = incDecimal.add(new BigDecimal(new BigInteger(lastBytes)));
            return new BigDecimal(new BigInteger(endBytes)).compareTo(candidate) < 0;
          } else {
            candidate = new BigDecimal(new BigInteger(lastBytes)).subtract(incDecimal);
            return candidate.compareTo(new BigDecimal(new BigInteger(endBytes))) < 0;
          }
        } else {
          char[] lastChars = tuple.getUnicodeChars(i);
          char[] endChars = mergedRange.getEnd().getUnicodeChars(colId);

          Preconditions.checkState(lastChars.length == endChars.length);

          BigInteger lastBi = charsToBigInteger(lastChars);
          BigInteger endBi = charsToBigInteger(endChars);

          if (sortSpecs[colId].isAscending()) {
            candidate = incDecimal.add(new BigDecimal(lastBi));
            return new BigDecimal(endBi).compareTo(candidate) < 0;
          } else {
            candidate = new BigDecimal(lastBi).subtract(incDecimal);
            return candidate.compareTo(new BigDecimal(endBi)) < 0;
          }
        }
      }
      case INET4: {
        int candidateIntVal;
        byte[] candidateBytesVal = new byte[4];
        if (sortSpecs[colId].isAscending()) {
          candidateIntVal = incDecimal.intValue() + tuple.getInt4(i);
          if (candidateIntVal - incDecimal.intValue() != tuple.getInt4(i)) {
            return true;
          }
          Bytes.putInt(candidateBytesVal, 0, candidateIntVal);
          return Bytes.compareTo(mergedRange.getEnd().getBytes(colId), candidateBytesVal) < 0;
        } else {
          candidateIntVal = tuple.getInt4(i) - incDecimal.intValue();
          if (candidateIntVal + incDecimal.intValue() != tuple.getInt4(i)) {
            return true;
          }
          Bytes.putInt(candidateBytesVal, 0, candidateIntVal);
          return Bytes.compareTo(candidateBytesVal, mergedRange.getEnd().getBytes(colId)) < 0;
        }
      }
    }
    return overflow;
  }

  private long incrementAndGetReminder(int colId, Tuple last, long inc) {
    Column column = sortSpecs[colId].getSortKey();
    long reminder = 0;
    switch (column.getDataType().getType()) {
      case BIT: {
        long candidate = last.getByte(colId) + inc;
        byte end = mergedRange.getEnd().getByte(colId);
        reminder = candidate - end;
        break;
      }
      case CHAR: {
        long candidate = last.getChar(colId) + inc;
        char end = mergedRange.getEnd().getChar(colId);
        reminder = candidate - end;
        break;
      }
      case DATE:
      case INT4: {
        int candidate = (int) (last.getInt4(colId) + inc);
        int end = mergedRange.getEnd().getInt4(colId);
        reminder = candidate - end;
        break;
      }
      case TIME:
      case TIMESTAMP:
      case INT8:
      case INET4: {
        long candidate = last.getInt8(colId) + inc;
        long end = mergedRange.getEnd().getInt8(colId);
        reminder = candidate - end;
        break;
      }
      case FLOAT4: {
        float candidate = last.getFloat4(colId) + inc;
        float end = mergedRange.getEnd().getFloat4(colId);
        reminder = (long) (candidate - end);
        break;
      }
      case FLOAT8: {
        double candidate = last.getFloat8(colId) + inc;
        double end = mergedRange.getEnd().getFloat8(colId);
        reminder = (long) Math.ceil(candidate - end);
        break;
      }
      case TEXT: {
        byte [] lastBytes = last.getBytes(colId);
        byte [] endBytes = mergedRange.getEnd().getBytes(colId);

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
      if (isOverflow(i, last, i, incs[i], sortSpecs)) {
        if (i == 0) {
          throw new RangeOverflowException(mergedRange, last, incs[i].longValue(), sortSpecs[i].isAscending());
        }
        // increment some volume of the serialized one-dimension key space
        long rem = incrementAndGetReminder(i, last, value.longValue());
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

    VTuple end = new VTuple(sortSpecs.length);
    Column column;
    for (int i = 0; i < last.size(); i++) {
      column = sortSpecs[i].getSortKey();
      switch (column.getDataType().getType()) {
        case CHAR:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createChar((char) (mergedRange.getStart().getChar(i) + incs[i].longValue())));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createChar((char) (last.getChar(i) + incs[i].longValue())));
            } else {
              end.put(i, DatumFactory.createChar((char) (last.getChar(i) - incs[i].longValue())));
            }
          }
          break;
        case BIT:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createBit(
                (byte) (mergedRange.getStart().getByte(i) + incs[i].longValue())));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createBit((byte) (last.getByte(i) + incs[i].longValue())));
            } else {
              end.put(i, DatumFactory.createBit((byte) (last.getByte(i) - incs[i].longValue())));
            }
          }
          break;
        case INT2:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createInt2(
                (short) (mergedRange.getStart().getInt2(i) + incs[i].longValue())));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createInt2((short) (last.getInt2(i) + incs[i].longValue())));
            } else {
              end.put(i, DatumFactory.createInt2((short) (last.getInt2(i) - incs[i].longValue())));
            }
          }
          break;
        case INT4:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createInt4(
                (int) (mergedRange.getStart().getInt4(i) + incs[i].longValue())));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createInt4((int) (last.getInt4(i) + incs[i].longValue())));
            } else {
              end.put(i, DatumFactory.createInt4((int) (last.getInt4(i) - incs[i].longValue())));
            }
          }
          break;
        case INT8:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createInt8(
                mergedRange.getStart().getInt8(i) + incs[i].longValue()));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createInt8(last.getInt8(i) + incs[i].longValue()));
            } else {
              end.put(i, DatumFactory.createInt8(last.getInt8(i) - incs[i].longValue()));
            }
          }
          break;
        case FLOAT4:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createFloat4(
                mergedRange.getStart().getFloat4(i) + incs[i].longValue()));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createFloat4(last.getFloat4(i) + incs[i].longValue()));
            } else {
              end.put(i, DatumFactory.createFloat4(last.getFloat4(i) - incs[i].longValue()));
            }
          }
          break;
        case FLOAT8:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createFloat8(
                mergedRange.getStart().getFloat8(i) + incs[i].longValue()));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createFloat8(last.getFloat8(i) + incs[i].longValue()));
            } else {
              end.put(i, DatumFactory.createFloat8(last.getFloat8(i) - incs[i].longValue()));
            }
          }
          break;
        case TEXT:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createText(((char) (mergedRange.getStart().getText(i).charAt(0)
                + incs[i].longValue())) + ""));
          } else {
            BigInteger lastBigInt;
            if (last.isBlankOrNull(i)) {
              lastBigInt = BigInteger.valueOf(0);
              end.put(i, DatumFactory.createText(lastBigInt.add(incs[i]).toByteArray()));
            } else {

              if (isPureAscii[i]) {
                lastBigInt = new BigInteger(last.getBytes(i));
                if (sortSpecs[i].isAscending()) {
                  end.put(i, DatumFactory.createText(lastBigInt.add(incs[i]).toByteArray()));
                } else {
                  end.put(i, DatumFactory.createText(lastBigInt.subtract(incs[i]).toByteArray()));
                }
              } else {

                // We consider an array of chars as a 2^16 base number system because each char is 2^16 bits.
                // See Character.MAX_NUMBER. Then, we increase some number to the last array of chars.

                char[] lastChars = last.getUnicodeChars(i);
                int [] charIncs = new int[lastChars.length];

                BigInteger remain = incs[i];
                for (int k = lastChars.length - 1; k > 0 && remain.compareTo(BigInteger.ZERO) > 0; k--) {
                  BigInteger digitBase = BigInteger.valueOf(TextDatum.UNICODE_CHAR_BITS_NUM).pow(k);

                  if (remain.compareTo(digitBase) > 0) {
                    charIncs[k] = remain.divide(digitBase).intValue();
                    BigInteger sub = digitBase.multiply(BigInteger.valueOf(charIncs[k]));
                    remain = remain.subtract(sub);
                  }
                }
                charIncs[charIncs.length - 1] = remain.intValue();

                for (int k = 0; k < lastChars.length; k++) {
                  if (charIncs[k] == 0) {
                    continue;
                  }

                  if (sortSpecs[i].isAscending()) {
                    int sum = (int) lastChars[k] + charIncs[k];
                    if (sum > TextDatum.UNICODE_CHAR_BITS_NUM) { // if carry occurs in the current digit
                      charIncs[k] = sum - TextDatum.UNICODE_CHAR_BITS_NUM;
                      charIncs[k - 1] += 1;

                      lastChars[k - 1] += 1;
                      lastChars[k] += charIncs[k];
                    } else {
                      lastChars[k] += charIncs[k];
                    }
                  } else {
                    int sum = (int) lastChars[k] - charIncs[k];
                    if (sum < 0) { // if carry occurs in the current digit
                      charIncs[k] = TextDatum.UNICODE_CHAR_BITS_NUM - sum;
                      charIncs[k - 1] -= 1;

                      lastChars[k - 1] -= 1;
                      lastChars[k] += charIncs[k];
                    } else {
                      lastChars[k] -= charIncs[k];
                    }
                  }
                }

                end.put(i, DatumFactory.createText(Convert.chars2utf(lastChars)));
              }
            }
          }
          break;
        case DATE:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createDate((int) (mergedRange.getStart().getInt4(i) + incs[i].longValue())));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createDate((int) (last.getInt4(i) + incs[i].longValue())));
            } else {
              end.put(i, DatumFactory.createDate((int) (last.getInt4(i) - incs[i].longValue())));
            }
          }
          break;
        case TIME:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createTime(mergedRange.getStart().getInt8(i) + incs[i].longValue()));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createTime(last.getInt8(i) + incs[i].longValue()));
            } else {
              end.put(i, DatumFactory.createTime(last.getInt8(i) - incs[i].longValue()));
            }
          }
          break;
        case TIMESTAMP:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createTimestmpDatumWithJavaMillis(
                mergedRange.getStart().getInt8(i) + incs[i].longValue()));
          } else {
            if (sortSpecs[i].isAscending()) {
              end.put(i, DatumFactory.createTimestmpDatumWithJavaMillis(last.getInt8(i) + incs[i].longValue()));
            } else {
              end.put(i, DatumFactory.createTimestmpDatumWithJavaMillis(last.getInt8(i) - incs[i].longValue()));
            }
          }
          break;
        case INET4:
          byte[] ipBytes;
          if (overflowFlag[i]) {
            ipBytes = mergedRange.getStart().getBytes(i);
            assert ipBytes.length == 4;
            end.put(i, DatumFactory.createInet4(ipBytes));
          } else {
            if (sortSpecs[i].isAscending()) {
              int lastVal = last.getInt4(i) + incs[i].intValue();
              ipBytes = new byte[4];
              Bytes.putInt(ipBytes, 0, lastVal);
              end.put(i, DatumFactory.createInet4(ipBytes));
            } else {
              int lastVal = last.getInt4(i) - incs[i].intValue();
              ipBytes = new byte[4];
              Bytes.putInt(ipBytes, 0, lastVal);
              end.put(i, DatumFactory.createInet4(ipBytes));
            }
          }
          break;
        default:
          throw new UnsupportedOperationException(column.getDataType() + " is not supported yet");
      }

      // replace i'th end value by NULL if begin and end are all NULL
      if (beginNulls[i] && endNulls[i]) {
        end.put(i, NullDatum.get());
        continue;
      }
    }

    return end;
  }

  public static BigInteger charsToBigInteger(char [] chars) {
    BigInteger digitBase;
    BigInteger sum = BigInteger.ZERO;
    for (int i = chars.length - 1; i >= 0; i--) {
      BigInteger charVal = BigInteger.valueOf(chars[(chars.length - 1) - i]);
      if (i > 0) {
        digitBase = charVal.multiply(BigInteger.valueOf(TextDatum.UNICODE_CHAR_BITS_NUM).pow(i));
        sum = sum.add(digitBase);
      } else {
        sum = sum.add(charVal);
      }
    }
    return sum;
  }
}
