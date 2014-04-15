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

package org.apache.tajo.datum;

import com.google.protobuf.Message;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.exception.InvalidCastException;
import org.apache.tajo.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import java.io.IOException;

public class DatumFactory {

  public static Class<? extends Datum> getDatumClass(Type type) {
    switch (type) {
      case BOOLEAN:
        return BooleanDatum.class;
      case INT2:
        return Int2Datum.class;
      case INT4:
        return Int4Datum.class;
      case INT8:
        return Int8Datum.class;
      case FLOAT4:
        return Float4Datum.class;
      case FLOAT8:
        return Float8Datum.class;
      case CHAR:
        return CharDatum.class;
      case TEXT:
        return TextDatum.class;
      case TIMESTAMP:
        return TimestampDatum.class;
      case DATE:
        return DateDatum.class;
      case TIME:
        return TimeDatum.class;
      case BIT:
        return BitDatum.class;
      case BLOB:
        return BlobDatum.class;
      case INET4:
        return Inet4Datum.class;
      case ANY:
        return NullDatum.class;
      case NULL_TYPE:
        return NullDatum.class;
      default:
        throw new UnsupportedOperationException(type.name());
    }
  }

  public static Datum createFromString(DataType dataType, String value) {
    switch (dataType.getType()) {

    case BOOLEAN:
      return createBool(value.equals(BooleanDatum.TRUE_STRING));
    case INT2:
      return createInt2(value);
    case INT4:
      return createInt4(value);
    case INT8:
      return createInt8(value);
    case FLOAT4:
      return createFloat4(value);
    case FLOAT8:
      return createFloat8(value);
    case CHAR:
      return createChar(value);
    case TEXT:
      return createText(value);
    case DATE:
      return createDate(value);
    case TIME:
      return createTime(value);
    case TIMESTAMP:
      return createTimeStamp(value);
    case BLOB:
      return createBlob(value);
    case INET4:
      return createInet4(value);
    default:
      throw new UnsupportedOperationException(dataType.toString());
    }
  }

  public static Datum createFromBytes(DataType dataType, byte[] bytes) {
    switch (dataType.getType()) {

      case BOOLEAN:
        return createBool(bytes[0]);
      case INT2:
        return createInt2(Bytes.toShort(bytes));
      case INT4:
        return createInt4(Bytes.toInt(bytes));
      case INT8:
        return createInt8(Bytes.toLong(bytes));
      case FLOAT4:
        return createFloat4(Bytes.toFloat(bytes));
      case FLOAT8:
        return createFloat8(Bytes.toDouble(bytes));
      case CHAR:
        return createChar(bytes);
      case TEXT:
        return createText(bytes);
      case DATE:
        return new DateDatum(bytes);
      case TIME:
        return new TimeDatum(bytes);
      case TIMESTAMP:
        return new TimestampDatum(bytes);
      case BIT:
        return createBit(bytes[0]);
      case BLOB:
        return createBlob(bytes);
      case INET4:
        return createInet4(bytes);
      case PROTOBUF:
        ProtobufDatumFactory factory = ProtobufDatumFactory.get(dataType);
        Message.Builder builder = factory.newBuilder();
        try {
          builder.mergeFrom(bytes);
          return factory.createDatum(builder.build());
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      default:
        throw new UnsupportedOperationException(dataType.toString());
    }
  }

  public static Datum createFromInt4(DataType type, int val) {
    switch (type.getType()) {
    case INT4:
      return new Int4Datum(val);
    case DATE:
      return new DateDatum(val);
    default:
      throw new UnsupportedOperationException("Cannot create " + type.getType().name() + " datum from INT4");
    }
  }

  public static Datum createFromInt8(DataType type, long val) {
    switch (type.getType()) {
    case INT8:
      return new Int8Datum(val);
    case TIMESTAMP:
      return createTimeStampFromMillis(val);
    case TIME:
      return createTime(val); 
    default:
      throw new UnsupportedOperationException("Cannot create " + type.getType().name() + " datum from INT8");
    }
  }

  public static NullDatum createNullDatum() {
    return NullDatum.get();
  }

  public static Datum createBool(byte val) {
    return BooleanDatum.THREE_VALUES[(int)val];
  }

  public static BooleanDatum createBool(boolean val) {
    return val ? BooleanDatum.TRUE : BooleanDatum.FALSE;
  }

  public static BitDatum createBit(byte val) {
    return new BitDatum(val);
  }

  public static CharDatum createChar(char val) {
    return new CharDatum(val);
  }

  public static CharDatum createChar(byte val) {
    return new CharDatum(val);
  }

  public static CharDatum createChar(byte[] bytes) {
    return new CharDatum(bytes);
  }

  public static CharDatum createChar(String val) {
    return new CharDatum(val);
  }

  public static Int2Datum createInt2(short val) {
    return new Int2Datum(val);
  }

  public static Int2Datum createInt2(String val) {
    return new Int2Datum(Short.valueOf(val));
  }

  public static Int4Datum createInt4(int val) {
    return new Int4Datum(val);
  }

  public static Int4Datum createInt4(String val) {
    return new Int4Datum(Integer.parseInt(val));
  }

  public static Int8Datum createInt8(long val) {
    return new Int8Datum(val);
  }

  public static Int8Datum createInt8(String val) {
    return new Int8Datum(Long.parseLong(val));
  }

  public static Float4Datum createFloat4(float val) {
    return new Float4Datum(val);
  }

  public static Float4Datum createFloat4(String val) {
    return new Float4Datum(Float.valueOf(val));
  }

  public static Float8Datum createFloat8(double val) {
    return new Float8Datum(val);
  }

  public static Float8Datum createFloat8(String val) {
    return new Float8Datum(Double.valueOf(val));
  }

  public static TextDatum createText(String val) {
    return new TextDatum(val);
  }

  public static TextDatum createText(byte[] val) {
    return new TextDatum(val);
  }

  public static DateDatum createDate(int instance) {
    return new DateDatum(instance);
  }

  public static DateDatum createDate(String dateStr) {
    return new DateDatum(LocalDate.parse(dateStr));
  }

  public static TimeDatum createTime(long instance) {
    return new TimeDatum(instance);
  }

  public static TimeDatum createTime(String dateStr) {
    return new TimeDatum(LocalTime.parse(dateStr));
  }

  public static TimestampDatum createTimeStamp(int unixTime) {
    return new TimestampDatum(unixTime);
  }

  public static TimestampDatum createTimeStampFromMillis(long millis) {
    return new TimestampDatum(new DateTime(millis));
  }

  public static TimestampDatum createTimeStamp(String timeStamp) {
    return new TimestampDatum(timeStamp);
  }

  public static DateDatum createDate(Datum datum) {
    switch (datum.type()) {
    case INT4:
      return new DateDatum(datum.asInt4());
    case INT8:
      return new DateDatum(datum.asInt4());
    case TEXT:
      return new DateDatum(datum.asChars());
    case DATE:
      return (DateDatum) datum;
    default:
      throw new InvalidCastException(datum.type() + " cannot be casted to DATE type");
    }
  }

  public static TimeDatum createTime(Datum datum) {
    switch (datum.type()) {
    case INT8:
      return new TimeDatum(datum.asInt8());
    case TEXT:
      return new TimeDatum(datum.asChars());
    case TIME:
      return (TimeDatum) datum;
    default:
      throw new InvalidCastException(datum.type() + " cannot be casted to TIME type");
    }
  }

  public static TimestampDatum createTimestamp(Datum datum) {
    switch (datum.type()) {
      case TEXT:
        return new TimestampDatum(datum.asChars());
      case TIMESTAMP:
        return (TimestampDatum) datum;
      default:
        throw new InvalidCastException(datum.type() + " cannot be casted to TIMESTAMP type");
    }
  }

  public static BlobDatum createBlob(byte[] val) {
    return new BlobDatum(val);
  }

  public static BlobDatum createBlob(byte[] val, int offset, int length) {
    return new BlobDatum(val, offset, length);
  }

  public static BlobDatum createBlob(String val) {
    return new BlobDatum(val.getBytes());
  }

  public static Inet4Datum createInet4(byte[] val) {
    return new Inet4Datum(val);
  }

  public static Inet4Datum createInet4(byte[] val, int offset, int length) {
    return new Inet4Datum(val, offset, length);
  }

  public static Inet4Datum createInet4(String val) {
    return new Inet4Datum(val);
  }

  public static Datum cast(Datum operandDatum, DataType target) {
    switch (target.getType()) {
    case BOOLEAN:
      return DatumFactory.createBool(operandDatum.asBool());
    case CHAR:
      return DatumFactory.createChar(operandDatum.asChar());
    case INT1:
    case INT2:
      return DatumFactory.createInt2(operandDatum.asInt2());
    case INT4:
      return DatumFactory.createInt4(operandDatum.asInt4());
    case INT8:
      return DatumFactory.createInt8(operandDatum.asInt8());
    case FLOAT4:
      return DatumFactory.createFloat4(operandDatum.asFloat4());
    case FLOAT8:
      return DatumFactory.createFloat8(operandDatum.asFloat8());
    case TEXT:
      return DatumFactory.createText(operandDatum.asTextBytes());
    case DATE:
      return DatumFactory.createDate(operandDatum);
    case TIME:
      return DatumFactory.createTime(operandDatum);
    case TIMESTAMP:
      return DatumFactory.createTimestamp(operandDatum);
    case BLOB:
      return DatumFactory.createBlob(operandDatum.asByteArray());
    default:
      throw new InvalidCastException("Cannot cast " + operandDatum.type() + " to " + target.getType());
    }
  }
}
