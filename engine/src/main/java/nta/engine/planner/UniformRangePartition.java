package nta.engine.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.storage.Tuple;
import nta.storage.TupleRange;
import nta.storage.VTuple;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * @author Hyunsik Choi
 */
public class UniformRangePartition extends RangePartitionAlgorithm {
  private int variableId;
  private BigDecimal[] cardForEachDigit;
  private BigDecimal[] colCards;

  /**
   *
   * @param schema
   * @param range
   * @param inclusive true if the end of the range is inclusive
   */
  public UniformRangePartition(Schema schema, TupleRange range, boolean inclusive) {
    super(schema, range, inclusive);
    colCards = new BigDecimal[schema.getColumnNum()];
    for (int i = 0; i < schema.getColumnNum(); i++) {
      colCards[i] = computeCardinality(schema.getColumn(i).getDataType(), range.getStart().get(i),
          range.getEnd().get(i), inclusive);
    }

    cardForEachDigit = new BigDecimal[colCards.length];
    for (int i = 0; i < colCards.length ; i++) {
      if (i == 0) {
        cardForEachDigit[i] = colCards[i];
      } else {
        cardForEachDigit[i] = cardForEachDigit[i - 1].multiply(colCards[i]);
      }
    }
  }

  public UniformRangePartition(Schema schema, TupleRange range) {
    this(schema, range, true);
  }

  @Override
  public TupleRange[] partition(int partNum) {
    Preconditions.checkArgument(partNum > 0, "The number of partitions must be positive, but the given number: "
        + partNum);
    Preconditions.checkArgument(totalCard.compareTo(new BigDecimal(partNum)) >= 0,
        "the number of partition cannot exceed total cardinality (" + totalCard + ")");

    int varId;
    for (varId = 0; varId < cardForEachDigit.length; varId++) {
      if (cardForEachDigit[varId].compareTo(new BigDecimal(partNum)) >= 0)
        break;
    }
    this.variableId = varId;

    BigDecimal [] reverseCardsForDigit = new BigDecimal[variableId+1];
    for (int i = variableId; i >= 0; i--) {
      if (i == variableId) {
        reverseCardsForDigit[i] = colCards[i];
      } else {
        reverseCardsForDigit[i] = reverseCardsForDigit[i+1].multiply(colCards[i]);
      }
    }

    List<TupleRange> ranges = Lists.newArrayList();
    BigDecimal term = reverseCardsForDigit[0].divide(new BigDecimal(partNum), RoundingMode.CEILING);
    BigDecimal reminder = reverseCardsForDigit[0];
    Tuple last = range.getStart();
    while(reminder.compareTo(new BigDecimal(0)) > 0) {
      if (reminder.compareTo(term) <= 0) { // final one is inclusive
        ranges.add(new TupleRange(schema, last, range.getEnd()));
      } else {
        Tuple next = increment(last, term.longValue(), variableId);
        ranges.add(new TupleRange(schema, last, next));
      }
      last = ranges.get(ranges.size() - 1).getEnd();
      reminder = reminder.subtract(term);
    }

    return ranges.toArray(new TupleRange[ranges.size()]);
  }

  public boolean isOverflow(int colId, Datum last, BigDecimal inc) {
    Column column = schema.getColumn(colId);
    BigDecimal candidate;
    boolean overflow = false;
    switch (column.getDataType()) {
      case BYTE: {
        candidate = inc.add(new BigDecimal(last.asByte()));
        return new BigDecimal(range.getEnd().get(colId).asByte()).compareTo(candidate) < 0;
      }
      case CHAR: {
        candidate = inc.add(new BigDecimal((int)last.asChar()));
        return new BigDecimal((int)range.getEnd().get(colId).asChar()).compareTo(candidate) < 0;
      }
      case SHORT: {
        candidate = inc.add(new BigDecimal(last.asShort()));
        return new BigDecimal(range.getEnd().get(colId).asShort()).compareTo(candidate) < 0;
      }
      case INT: {
        candidate = inc.add(new BigDecimal(last.asInt()));
        return new BigDecimal(range.getEnd().get(colId).asInt()).compareTo(candidate) < 0;
      }
      case LONG: {
        candidate = inc.add(new BigDecimal(last.asLong()));
        return new BigDecimal(range.getEnd().get(colId).asLong()).compareTo(candidate) < 0;
      }
      case FLOAT: {
        candidate = inc.add(new BigDecimal(last.asFloat()));
        return new BigDecimal(range.getEnd().get(colId).asFloat()).compareTo(candidate) < 0;
      }
      case DOUBLE: {
        candidate = inc.add(new BigDecimal(last.asDouble()));
        return new BigDecimal(range.getEnd().get(colId).asDouble()).compareTo(candidate) < 0;
      }
      case STRING: {
        candidate = inc.add(new BigDecimal((int)(last.asChars().charAt(0))));
        return new BigDecimal(range.getEnd().get(colId).asChars().charAt(0)).compareTo(candidate) < 0;
      }
    }
    return overflow;
  }

  public long incrementAndGetReminder(int colId, Datum last, long inc) {
    Column column = schema.getColumn(colId);
    long reminder = 0;
    switch (column.getDataType()) {
      case BYTE: {
        long candidate = last.asByte() + inc;
        byte end = range.getEnd().get(colId).asByte();
        long longReminder = candidate - end;
        reminder = longReminder;
        break;
      }
      case CHAR: {
        long candidate = last.asChar() + inc;
        char end = range.getEnd().get(colId).asChar();
        long longReminder = candidate - end;
        reminder = longReminder;
        break;
      }
      case INT: {
        int candidate = (int) (last.asInt() + inc);
        int end = range.getEnd().get(colId).asInt();
        int longReminder = candidate - end;
        reminder = longReminder;
        break;
      }
      case LONG: {
        long candidate = last.asLong() + inc;
        long end = range.getEnd().get(colId).asLong();
        long longReminder = candidate - end;
        reminder = longReminder;
        break;
      }
      case FLOAT: {
        float candidate = last.asFloat() + inc;
        float end = range.getEnd().get(colId).asFloat();
        float longReminder = candidate - end;
        reminder = (long) longReminder;
        break;
      }
      case DOUBLE: {
        double candidate = last.asDouble() + inc;
        double end = range.getEnd().get(colId).asDouble();
        double longReminder = candidate - end;
        reminder = (long) Math.ceil(longReminder);
        break;
      }
      case STRING: {
        char candidate = ((char)(last.asChars().charAt(0) + inc));
        char end = range.getEnd().get(colId).asChars().charAt(0);
        char charReminder = (char) (candidate - end);
        reminder = charReminder;
        break;
      }
    }

    // including zero
    return reminder - 1;
  }

  /**
   *
   * @param last
   * @param inc
   * @return
   */
  public Tuple increment(final Tuple last, final long inc, final int baseDigit) {
    BigDecimal [] incs = new BigDecimal[last.size()];
    boolean [] overflowFlag = new boolean[last.size()];
    BigDecimal [] result;
    BigDecimal value = new BigDecimal(inc);

    BigDecimal [] reverseCardsForDigit = new BigDecimal[baseDigit + 1];
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
      if (isOverflow(i, last.get(i), incs[i])) {
        if (i == 0) {
          throw new RangeOverflowException(range, last, incs[i].longValue());
        }
        long rem = incrementAndGetReminder(i, last.get(i), value.longValue());
        incs[i] = new BigDecimal(rem);
        incs[i - 1] = incs[i-1].add(new BigDecimal(1));
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
        incs[i] = new BigDecimal(0);
      }
    }

    Tuple end = new VTuple(schema.getColumnNum());
    Column column;
    for (int i = 0; i < last.size(); i++) {
      column = schema.getColumn(i);
      switch (column.getDataType()) {
        case CHAR:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createChar((char) (range.getStart().get(i).asChar() + incs[i].longValue())));
          } else {
            end.put(i, DatumFactory.createChar((char) (last.get(i).asChar() + incs[i].longValue())));
          }
          break;
        case BYTE:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createByte((byte) (range.getStart().get(i).asByte() + incs[i].longValue())));
          } else {
            end.put(i, DatumFactory.createByte((byte) (last.get(i).asByte() + incs[i].longValue())));
          }
          break;
        case SHORT:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createShort((short) (range.getStart().get(i).asShort() + incs[i].longValue())));
          } else {
            end.put(i, DatumFactory.createShort((short) (last.get(i).asShort() + incs[i].longValue())));
          }
          break;
        case INT:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createInt((int) (range.getStart().get(i).asInt() + incs[i].longValue())));
          } else {
            end.put(i, DatumFactory.createInt((int) (last.get(i).asInt() + incs[i].longValue())));
          }
          break;
        case LONG:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createLong(range.getStart().get(i).asInt() + incs[i].longValue()));
          } else {
            end.put(i, DatumFactory.createLong(last.get(i).asInt() + incs[i].longValue()));
          }
          break;
        case FLOAT:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createFloat(range.getStart().get(i).asFloat() + incs[i].longValue()));
          } else {
            end.put(i, DatumFactory.createFloat(last.get(i).asFloat() + incs[i].longValue()));
          }
          break;
        case DOUBLE:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createDouble(range.getStart().get(i).asDouble() + incs[i].longValue()));
          } else {
            end.put(i, DatumFactory.createDouble(last.get(i).asDouble() + incs[i].longValue()));
          }
          break;
        case STRING:
          if (overflowFlag[i]) {
            end.put(i, DatumFactory.createString(((char)(range.getStart().get(i).asChars().charAt(0)
                + incs[i].longValue())) + ""));
          } else {
            end.put(i, DatumFactory.createString(((char)(last.get(i).asChars().charAt(0) + incs[i].longValue())) + ""));
          }
          break;
        default:
          throw new UnsupportedOperationException(column.getDataType() + " is not supported yet");
      }
    }

    return end;
  }
}
