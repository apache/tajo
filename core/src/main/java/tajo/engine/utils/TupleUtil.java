package tajo.engine.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.statistics.ColumnStat;
import tajo.datum.*;
import tajo.storage.RowStoreUtil;
import tajo.storage.Tuple;
import tajo.storage.TupleRange;
import tajo.storage.VTuple;
import tajo.util.Bytes;
import tajo.worker.dataserver.HttpUtil;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TupleUtil {
  /** class logger **/
  private static final Log LOG = LogFactory.getLog(TupleUtil.class);

  /**
   * It computes the value cardinality of a tuple range.
   *
   * @param schema
   * @param range
   * @return
   */
  public static long computeCardinality(Schema schema, TupleRange range) {
    Tuple start = range.getStart();
    Tuple end = range.getEnd();
    Column col;

    long cardinality = 1;
    long columnCard;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      col = schema.getColumn(i);
      switch (col.getDataType()) {
        case CHAR:
          columnCard = end.get(i).asChar() - start.get(i).asChar();
          break;
        case BYTE:
          columnCard = end.get(i).asByte() - start.get(i).asByte();
          break;
        case SHORT:
          columnCard = end.get(i).asShort() - start.get(i).asShort();
          break;
        case INT:
          columnCard = end.get(i).asInt() - start.get(i).asInt();
          break;
        case LONG:
          columnCard = end.get(i).asLong() - start.get(i).asLong();
          break;
        case FLOAT:
          columnCard = end.get(i).asInt() - start.get(i).asInt();
          break;
        case DOUBLE:
          columnCard = end.get(i).asLong() - start.get(i).asLong();
          break;
        case STRING:
          columnCard = end.get(i).asChars().charAt(0) - start.get(i).asChars().charAt(0);
          break;
        default:
          throw new UnsupportedOperationException(col.getDataType() + " is not supported yet");
      }

      if (columnCard > 0) {
        cardinality *= columnCard + 1;
      }
    }

    return cardinality;
  }

  public static TupleRange [] getPartitions(Schema schema, int partNum, TupleRange range) {
    Tuple start = range.getStart();
    Tuple end = range.getEnd();
    Column col;
    TupleRange [] partitioned = new TupleRange[partNum];

    Datum[] term = new Datum[schema.getColumnNum()];
    Datum[] prevValues = new Datum[schema.getColumnNum()];

    // initialize term and previous values
    for (int i = 0; i < schema.getColumnNum(); i++) {
      col = schema.getColumn(i);
      prevValues[i] = start.get(i);
      switch (col.getDataType()) {
        case CHAR:
          int sChar = start.get(i).asChar();
          int eChar = end.get(i).asChar();
          int rangeChar;
          if ((eChar - sChar) > partNum) {
            rangeChar = (eChar - sChar) / partNum;
          } else {
            rangeChar = 1;
          }
          term[i] = DatumFactory.createInt(rangeChar);
        case BYTE:
          byte sByte = start.get(i).asByte();
          byte eByte = end.get(i).asByte();
          int rangeByte;
          if ((eByte - sByte) > partNum) {
            rangeByte = (eByte - sByte) / partNum;
          } else {
            rangeByte = 1;
          }
          term[i] = DatumFactory.createByte((byte)rangeByte);
          break;

        case SHORT:
          short sShort = start.get(i).asShort();
          short eShort = end.get(i).asShort();
          int rangeShort;
          if ((eShort - sShort) > partNum) {
            rangeShort = (eShort - sShort) / partNum;
          } else {
            rangeShort = 1;
          }
          term[i] = DatumFactory.createShort((short) rangeShort);
          break;

        case INT:
          int sInt = start.get(i).asInt();
          int eInt = end.get(i).asInt();
          int rangeInt;
          if ((eInt - sInt) > partNum) {
            rangeInt = (eInt - sInt) / partNum;
          } else {
            rangeInt = 1;
          }
          term[i] = DatumFactory.createInt(rangeInt);
          break;

        case LONG:
          long sLong = start.get(i).asLong();
          long eLong = end.get(i).asLong();
          long rangeLong;
          if ((eLong - sLong) > partNum) {
            rangeLong = ((eLong - sLong) / partNum);
          } else {
            rangeLong = 1;
          }
          term[i] = DatumFactory.createLong(rangeLong);
          break;

        case FLOAT:
          float sFloat = start.get(i).asFloat();
          float eFloat = end.get(i).asFloat();
          float rangeFloat;
          if ((eFloat - sFloat) > partNum) {
            rangeFloat = ((eFloat - sFloat) / partNum);
          } else {
            rangeFloat = 1;
          }
          term[i] = DatumFactory.createFloat(rangeFloat);
          break;
        case DOUBLE:
          double sDouble = start.get(i).asDouble();
          double eDouble = end.get(i).asDouble();
          double rangeDouble;
          if ((eDouble - sDouble) > partNum) {
            rangeDouble = ((eDouble - sDouble) / partNum);
          } else {
            rangeDouble = 1;
          }
          term[i] = DatumFactory.createDouble(rangeDouble);
          break;
        case STRING:
          char sChars = start.get(i).asChars().charAt(0);
          char eChars = end.get(i).asChars().charAt(0);
          int rangeString;
          if ((eChars - sChars) > partNum) {
            rangeString = ((eChars - sChars) / partNum);
          } else {
            rangeString = 1;
          }
          term[i] = DatumFactory.createString(((char)rangeString) + "");
          break;
        case IPv4:
          throw new UnsupportedOperationException();
        case BYTES:
          throw new UnsupportedOperationException();
        default:
          throw new UnsupportedOperationException();
      }
    }

    for (int p = 0; p < partNum; p++) {
      Tuple sTuple = new VTuple(schema.getColumnNum());
      Tuple eTuple = new VTuple(schema.getColumnNum());
      for (int i = 0; i < schema.getColumnNum(); i++) {
        col = schema.getColumn(i);
        sTuple.put(i, prevValues[i]);
        switch (col.getDataType()) {
          case CHAR:
            char endChar = (char) (prevValues[i].asChar() + term[i].asChar());
            if (endChar > end.get(i).asByte()) {
              eTuple.put(i, end.get(i));
            } else {
              eTuple.put(i, DatumFactory.createChar(endChar));
            }
            prevValues[i] = DatumFactory.createChar(endChar);
            break;
          case BYTE:
            byte endByte = (byte) (prevValues[i].asByte() + term[i].asByte());
            if (endByte > end.get(i).asByte()) {
              eTuple.put(i, end.get(i));
            } else {
              eTuple.put(i, DatumFactory.createByte(endByte));
            }
            prevValues[i] = DatumFactory.createByte(endByte);
            break;
          case SHORT:
            int endShort = (short) (prevValues[i].asShort() + term[i].asShort());
            if (endShort > end.get(i).asShort()) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createShort((short) endShort));
            }
            prevValues[i] = DatumFactory.createShort((short) endShort);
            break;
          case INT:
            int endInt = (prevValues[i].asInt() + term[i].asInt());
            if (endInt > end.get(i).asInt()) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createInt(endInt));
            }
            prevValues[i] = DatumFactory.createInt(endInt);
            break;

          case LONG:
            long endLong = (prevValues[i].asLong() + term[i].asLong());
            if (endLong > end.get(i).asLong()) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createLong(endLong));
            }
            prevValues[i] = DatumFactory.createLong(endLong);
            break;

          case FLOAT:
            float endFloat = (prevValues[i].asFloat() + term[i].asFloat());
            if (endFloat > end.get(i).asFloat()) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createFloat(endFloat));
            }
            prevValues[i] = DatumFactory.createFloat(endFloat);
            break;
          case DOUBLE:
            double endDouble = (prevValues[i].asDouble() + term[i].asDouble());
            if (endDouble > end.get(i).asDouble()) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createDouble(endDouble));
            }
            prevValues[i] = DatumFactory.createDouble(endDouble);
            break;
          case STRING:
            String endString = ((char)(prevValues[i].asChars().charAt(0) + term[i].asChars().charAt(0))) + "";
            if (endString.charAt(0) > end.get(i).asChars().charAt(0)) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createString(endString));
            }
            prevValues[i] = DatumFactory.createString(endString);
            break;
          case IPv4:
            throw new UnsupportedOperationException();
          case BYTES:
            throw new UnsupportedOperationException();
          default:
            throw new UnsupportedOperationException();
        }
      }
      partitioned[p] = new TupleRange(schema, sTuple, eTuple);
    }

    return partitioned;
  }

  public static String rangeToQuery(Schema schema, TupleRange range, boolean last) throws UnsupportedEncodingException {
    StringBuilder sb = new StringBuilder();
    byte [] startBytes = RowStoreUtil.RowStoreEncoder
        .toBytes(schema, range.getStart());
    byte [] endBytes = RowStoreUtil.RowStoreEncoder
        .toBytes(schema, range.getEnd());
    String startBase64 = new String(Base64.encodeBase64(startBytes));
    String endBase64 = new String(Base64.encodeBase64(endBytes));

      sb.append("start=")
        .append(startBase64)
        .append("&")
        .append("end=")
        .append(endBase64);

    if (last) {
      sb.append("&final=true");
    }

    return sb.toString();
  }

  public static String [] rangesToQueries(Schema schema, TupleRange[] ranges) throws UnsupportedEncodingException {
    String [] params = new String[ranges.length];
    for (int i = 0; i < ranges.length; i++) {
      params[i] =
        rangeToQuery(schema, ranges[i], i == (ranges.length - 1));
    }
    return params;
  }

  public static TupleRange queryToRange(Schema schema, String query) throws UnsupportedEncodingException {
    Map<String,String> params = HttpUtil.getParamsFromQuery(query);
    String startUrlDecoded = params.get("start");
    String endUrlDecoded = params.get("end");
    byte [] startBytes = Base64.decodeBase64(startUrlDecoded);
    byte [] endBytes = Base64.decodeBase64(endUrlDecoded);
    return new TupleRange(schema, RowStoreUtil.RowStoreDecoder
        .toTuple(schema, startBytes), RowStoreUtil.RowStoreDecoder
        .toTuple(schema, endBytes));
  }

  public static TupleRange columnStatToRange(Schema schema, Schema target, List<ColumnStat> colStats) {
    Map<Column, ColumnStat> statSet = Maps.newHashMap();
    for (ColumnStat stat : colStats) {
      statSet.put(stat.getColumn(), stat);
    }

    for (Column col : target.getColumns()) {
      Preconditions.checkState(statSet.containsKey(col),
          "ERROR: Invalid Column Stats (column stats: " + colStats + ", there exists not target " + col);
    }

    Tuple startTuple = new VTuple(target.getColumnNum());
    Tuple endTuple = new VTuple(target.getColumnNum());
    int i = 0;
    for (Column col : target.getColumns()) {
      startTuple.put(i, statSet.get(col).getMinValue());
      endTuple.put(i, statSet.get(col).getMaxValue());
      i++;
    }
    return new TupleRange(target, startTuple, endTuple);
  }

  public static Datum createFromBytes(CatalogProtos.DataType type, byte [] bytes) {
    switch (type) {
      case BOOLEAN:
        return new BoolDatum(bytes);
      case BYTE:
        return new ByteDatum(bytes);
      case CHAR:
        return new CharDatum(bytes);
      case SHORT:
        return new ShortDatum(bytes);
      case INT:
        return new IntDatum(bytes);
      case LONG:
        return new LongDatum(bytes);
      case FLOAT:
        return new FloatDatum(bytes);
      case DOUBLE:
        return new DoubleDatum(bytes);
      case STRING:
        return new StringDatum(bytes);
      case IPv4:
        return new IPv4Datum(bytes);
      default: throw new UnsupportedOperationException(type + " is not supported yet");
    }
  }

  private final static byte [] TRUE_BYTES = new byte[] {(byte)1};
  private final static byte [] FALSE_BYTES = new byte[] {(byte)0};

  public static byte [] toBytes(CatalogProtos.DataType type, Datum datum) {
    ByteBuffer bb = null;
    switch (type) {
      case BOOLEAN:
        if (datum.asBool()) {
          return TRUE_BYTES;
        } else {
          return FALSE_BYTES;
        }
      case BYTE:
      case CHAR:
        return new byte[] {datum.asByte()};

      case SHORT:
        return Bytes.toBytes(datum.asShort());
      case INT:
        return Bytes.toBytes(datum.asInt());
      case LONG:
        return Bytes.toBytes(datum.asLong());
      case FLOAT:
        return Bytes.toBytes(datum.asFloat());
      case DOUBLE:
        return Bytes.toBytes(datum.asDouble());
      case STRING:
        return Bytes.toBytes(datum.asChars());
      case IPv4:
        return datum.asByteArray();

      default: throw new UnsupportedOperationException(type + " is not supported yet");
    }
  }
}
