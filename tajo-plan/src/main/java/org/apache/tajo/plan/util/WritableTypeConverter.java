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

package org.apache.tajo.plan.util;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.*;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.*;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedDataTypeException;
import org.apache.tajo.type.TypeStringEncoder;
import org.apache.tajo.util.datetime.DateTimeConstants;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.reflections.ReflectionUtils;

import java.util.Set;

public class WritableTypeConverter {

  private static DataType.Builder builder = DataType.newBuilder();

  public static DataType convertWritableToTajoType(Class<? extends Writable> writableClass) throws UnsupportedDataTypeException {
    if (writableClass == null)
      return null;

    Set<Class<?>> parents = ReflectionUtils.getAllSuperTypes(writableClass);

    if (writableClass == ByteWritable.class || parents.contains(ByteWritable.class)) {
      return builder.setType(Type.INT1).build();
    }
    if (writableClass == ShortWritable.class || parents.contains(ShortWritable.class)) {
      return builder.setType(Type.INT2).build();
    }
    if (writableClass == IntWritable.class || parents.contains(IntWritable.class)) {
      return builder.setType(Type.INT4).build();
    }
    if (writableClass == LongWritable.class || parents.contains(LongWritable.class)) {
      return builder.setType(Type.INT8).build();
    }
    if (writableClass == HiveCharWritable.class || parents.contains(HiveCharWritable.class)) {
      return builder.setType(Type.CHAR).build();
    }
    if (writableClass == Text.class || parents.contains(Text.class)) {
      return builder.setType(Type.TEXT).build();
    }
    if (writableClass == FloatWritable.class || parents.contains(FloatWritable.class)) {
      return builder.setType(Type.FLOAT4).build();
    }
    if (writableClass == DoubleWritable.class || parents.contains(DoubleWritable.class)) {
      return builder.setType(Type.FLOAT8).build();
    }
    if (writableClass == DateWritable.class || parents.contains(DateWritable.class)) {
      return builder.setType(Type.DATE).build();
    }
    if (writableClass == TimestampWritable.class || parents.contains(TimestampWritable.class)) {
      return builder.setType(Type.TIMESTAMP).build();
    }
    if (writableClass == BytesWritable.class || parents.contains(BytesWritable.class)) {
      return builder.setType(Type.VARBINARY).build();
    }

    throw new UnsupportedDataTypeException(writableClass.getSimpleName());
  }

  public static Writable convertDatum2Writable(Datum value) {
    switch(value.kind()) {
      case INT1: return new ByteWritable(value.asByte());
      case INT2: return new ShortWritable(value.asInt2());
      case INT4: return new IntWritable(value.asInt4());
      case INT8: return new LongWritable(value.asInt8());

      case FLOAT4: return new FloatWritable(value.asFloat4());
      case FLOAT8: return new DoubleWritable(value.asFloat8());

      // NOTE: value should be DateDatum
      case DATE: return new DateWritable(value.asInt4() - DateTimeConstants.UNIX_EPOCH_JDATE);

      // NOTE: value should be TimestampDatum
      case TIMESTAMP:
        TimestampWritable result = new TimestampWritable();
        result.setTime(DateTimeUtil.julianTimeToJavaTime(value.asInt8()));
        return result;

      case CHAR: {
        String str = value.asChars();
        return new HiveCharWritable(new HiveChar(str, str.length()));
      }
      case TEXT: return new Text(value.asChars());
      case VARBINARY: return new BytesWritable(value.asByteArray());

      case NULL_TYPE: return null;
    }

    throw new TajoRuntimeException(new NotImplementedException(TypeStringEncoder.encode(value.type())));
  }

  public static Datum convertWritable2Datum(Writable value) throws UnsupportedDataTypeException {
    if (value == null) {
      return NullDatum.get();
    }

    DataType type = convertWritableToTajoType(value.getClass());

    switch(type.getType()) {
      case INT1: return new Int2Datum(((ByteWritable)value).get());
      case INT2: return new Int2Datum(((ShortWritable)value).get());
      case INT4: return new Int4Datum(((IntWritable)value).get());
      case INT8: return new Int8Datum(((LongWritable)value).get());

      case FLOAT4: return new Float4Datum(((FloatWritable)value).get());
      case FLOAT8: return new Float8Datum(((DoubleWritable)value).get());

      case DATE: return new DateDatum(((DateWritable)value).getDays() + DateTimeConstants.UNIX_EPOCH_JDATE);
      case TIMESTAMP: return new TimestampDatum(DateTimeUtil.javaTimeToJulianTime(
          ((TimestampWritable)value).getTimestamp().getTime()));

      case CHAR: return new CharDatum(value.toString());
      case TEXT: return new TextDatum(value.toString());
      case VARBINARY: return new BlobDatum(((BytesWritable)value).getBytes());
    }

    throw new TajoRuntimeException(new UnsupportedDataTypeException(value.getClass().getTypeName()));
  }
}
