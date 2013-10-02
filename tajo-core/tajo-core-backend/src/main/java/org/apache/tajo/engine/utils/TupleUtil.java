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

package org.apache.tajo.engine.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.statistics.ColumnStat;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.dataserver.HttpUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
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
      switch (col.getDataType().getType()) {
        case CHAR:
          columnCard = end.get(i).asChar() - start.get(i).asChar();
          break;
        case BIT:
          columnCard = end.get(i).asByte() - start.get(i).asByte();
          break;
        case INT2:
          columnCard = end.get(i).asInt2() - start.get(i).asInt2();
          break;
        case INT4:
          columnCard = end.get(i).asInt4() - start.get(i).asInt4();
          break;
        case INT8:
          columnCard = end.get(i).asInt8() - start.get(i).asInt8();
          break;
        case FLOAT4:
          columnCard = end.get(i).asInt4() - start.get(i).asInt4();
          break;
        case FLOAT8:
          columnCard = end.get(i).asInt8() - start.get(i).asInt8();
          break;
        case TEXT:
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
      switch (col.getDataType().getType()) {
        case CHAR:
          int sChar = start.get(i).asChar();
          int eChar = end.get(i).asChar();
          int rangeChar;
          if ((eChar - sChar) > partNum) {
            rangeChar = (eChar - sChar) / partNum;
          } else {
            rangeChar = 1;
          }
          term[i] = DatumFactory.createInt4(rangeChar);
        case BIT:
          byte sByte = start.get(i).asByte();
          byte eByte = end.get(i).asByte();
          int rangeByte;
          if ((eByte - sByte) > partNum) {
            rangeByte = (eByte - sByte) / partNum;
          } else {
            rangeByte = 1;
          }
          term[i] = DatumFactory.createBit((byte) rangeByte);
          break;

        case INT2:
          short sShort = start.get(i).asInt2();
          short eShort = end.get(i).asInt2();
          int rangeShort;
          if ((eShort - sShort) > partNum) {
            rangeShort = (eShort - sShort) / partNum;
          } else {
            rangeShort = 1;
          }
          term[i] = DatumFactory.createInt2((short) rangeShort);
          break;

        case INT4:
          int sInt = start.get(i).asInt4();
          int eInt = end.get(i).asInt4();
          int rangeInt;
          if ((eInt - sInt) > partNum) {
            rangeInt = (eInt - sInt) / partNum;
          } else {
            rangeInt = 1;
          }
          term[i] = DatumFactory.createInt4(rangeInt);
          break;

        case INT8:
          long sLong = start.get(i).asInt8();
          long eLong = end.get(i).asInt8();
          long rangeLong;
          if ((eLong - sLong) > partNum) {
            rangeLong = ((eLong - sLong) / partNum);
          } else {
            rangeLong = 1;
          }
          term[i] = DatumFactory.createInt8(rangeLong);
          break;

        case FLOAT4:
          float sFloat = start.get(i).asFloat4();
          float eFloat = end.get(i).asFloat4();
          float rangeFloat;
          if ((eFloat - sFloat) > partNum) {
            rangeFloat = ((eFloat - sFloat) / partNum);
          } else {
            rangeFloat = 1;
          }
          term[i] = DatumFactory.createFloat4(rangeFloat);
          break;
        case FLOAT8:
          double sDouble = start.get(i).asFloat8();
          double eDouble = end.get(i).asFloat8();
          double rangeDouble;
          if ((eDouble - sDouble) > partNum) {
            rangeDouble = ((eDouble - sDouble) / partNum);
          } else {
            rangeDouble = 1;
          }
          term[i] = DatumFactory.createFloat8(rangeDouble);
          break;
        case TEXT:
          char sChars = start.get(i).asChars().charAt(0);
          char eChars = end.get(i).asChars().charAt(0);
          int rangeString;
          if ((eChars - sChars) > partNum) {
            rangeString = ((eChars - sChars) / partNum);
          } else {
            rangeString = 1;
          }
          term[i] = DatumFactory.createText(((char) rangeString) + "");
          break;
        case INET4:
          throw new UnsupportedOperationException();
        case BLOB:
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
        switch (col.getDataType().getType()) {
          case CHAR:
            char endChar = (char) (prevValues[i].asChar() + term[i].asChar());
            if (endChar > end.get(i).asByte()) {
              eTuple.put(i, end.get(i));
            } else {
              eTuple.put(i, DatumFactory.createChar(endChar));
            }
            prevValues[i] = DatumFactory.createChar(endChar);
            break;
          case BIT:
            byte endByte = (byte) (prevValues[i].asByte() + term[i].asByte());
            if (endByte > end.get(i).asByte()) {
              eTuple.put(i, end.get(i));
            } else {
              eTuple.put(i, DatumFactory.createBit(endByte));
            }
            prevValues[i] = DatumFactory.createBit(endByte);
            break;
          case INT2:
            int endShort = (short) (prevValues[i].asInt2() + term[i].asInt2());
            if (endShort > end.get(i).asInt2()) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createInt2((short) endShort));
            }
            prevValues[i] = DatumFactory.createInt2((short) endShort);
            break;
          case INT4:
            int endInt = (prevValues[i].asInt4() + term[i].asInt4());
            if (endInt > end.get(i).asInt4()) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createInt4(endInt));
            }
            prevValues[i] = DatumFactory.createInt4(endInt);
            break;

          case INT8:
            long endLong = (prevValues[i].asInt8() + term[i].asInt8());
            if (endLong > end.get(i).asInt8()) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createInt8(endLong));
            }
            prevValues[i] = DatumFactory.createInt8(endLong);
            break;

          case FLOAT4:
            float endFloat = (prevValues[i].asFloat4() + term[i].asFloat4());
            if (endFloat > end.get(i).asFloat4()) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createFloat4(endFloat));
            }
            prevValues[i] = DatumFactory.createFloat4(endFloat);
            break;
          case FLOAT8:
            double endDouble = (prevValues[i].asFloat8() + term[i].asFloat8());
            if (endDouble > end.get(i).asFloat8()) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createFloat8(endDouble));
            }
            prevValues[i] = DatumFactory.createFloat8(endDouble);
            break;
          case TEXT:
            String endString = ((char)(prevValues[i].asChars().charAt(0) + term[i].asChars().charAt(0))) + "";
            if (endString.charAt(0) > end.get(i).asChars().charAt(0)) {
              eTuple.put(i, end.get(i));
            } else {
              // TODO - to consider overflow
              eTuple.put(i, DatumFactory.createText(endString));
            }
            prevValues[i] = DatumFactory.createText(endString);
            break;
          case INET4:
            throw new UnsupportedOperationException();
          case BLOB:
            throw new UnsupportedOperationException();
          default:
            throw new UnsupportedOperationException();
        }
      }
      partitioned[p] = new TupleRange(schema, sTuple, eTuple);
    }

    return partitioned;
  }

  public static String rangeToQuery(Schema schema, TupleRange range,
                                    boolean ascendingFirstKey, boolean last)
      throws UnsupportedEncodingException {
    StringBuilder sb = new StringBuilder();
    byte [] firstKeyBytes = RowStoreUtil.RowStoreEncoder
        .toBytes(schema, range.getStart());
    byte [] endKeyBytes = RowStoreUtil.RowStoreEncoder
        .toBytes(schema, range.getEnd());

    String firstKeyBase64 = new String(Base64.encodeBase64(firstKeyBytes));
    String lastKeyBase64 = new String(Base64.encodeBase64(endKeyBytes));

    sb.append("start=")
        .append(URLEncoder.encode(firstKeyBase64, "utf-8"))
        .append("&")
        .append("end=")
        .append(URLEncoder.encode(lastKeyBase64, "utf-8"));

    if (last) {
      sb.append("&final=true");
    }

    return sb.toString();
  }

  public static String [] rangesToQueries(final SortSpec[] sortSpec,
                                          final TupleRange[] ranges)
      throws UnsupportedEncodingException {
    Schema schema = PlannerUtil.sortSpecsToSchema(sortSpec);
    boolean ascendingFirstKey = sortSpec[0].isAscending();
    String [] params = new String[ranges.length];
    for (int i = 0; i < ranges.length; i++) {
      params[i] =
        rangeToQuery(schema, ranges[i], ascendingFirstKey,
            ascendingFirstKey ? i == (ranges.length - 1) : i == 0);
    }
    return params;
  }

  public static TupleRange queryToRange(Schema schema, String query) throws UnsupportedEncodingException {
    Map<String,String> params = HttpUtil.getParamsFromQuery(query);
    String startUrlDecoded = URLDecoder.decode(params.get("start"), "utf-8");
    String endUrlDecoded = URLDecoder.decode(params.get("end"), "utf-8");
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

  /**
   * It creates a tuple of a given size filled with NULL values in all fields
   * It is usually used in outer join algorithms.
   *
   * @param size The number of columns of a creating tuple
   * @return The created tuple filled with NULL values
   */
  public static Tuple createNullPaddedTuple(int size){
    VTuple aTuple = new VTuple(size);
    int i;
    for(i = 0; i < size; i++){
      aTuple.put(i, DatumFactory.createNullDatum());
    }
    return aTuple;
  }
}
