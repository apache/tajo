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

package org.apache.tajo.util;

import org.apache.hadoop.io.*;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.*;
import org.reflections.ReflectionUtils;

import java.util.Set;

public class WritableTypeConverter {

  public static DataType convertWritableToTajoType(Class<? extends Writable> writableClass) {
    if (writableClass == null)
      return null;

    DataType.Builder builder = DataType.newBuilder();
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
    if (writableClass == Text.class || parents.contains(Text.class)) {
      return builder.setType(Type.TEXT).build();
    }
    if (writableClass == FloatWritable.class || parents.contains(FloatWritable.class)) {
      return builder.setType(Type.FLOAT4).build();
    }
    if (writableClass == DoubleWritable.class || parents.contains(DoubleWritable.class)) {
      return builder.setType(Type.FLOAT8).build();
    }

    return builder.setType(Type.NULL_TYPE).build();
  }

  public static Writable convertDatum2Writable(Datum value) {
    switch(value.type()) {
      case INT1: return new ByteWritable(value.asByte());
      case INT2: return new ShortWritable(value.asInt2());
      case INT4: return new IntWritable(value.asInt4());
      case INT8: return new LongWritable(value.asInt8());

      case FLOAT4: return new FloatWritable(value.asFloat4());
      case FLOAT8: return new DoubleWritable(value.asFloat8());

      case TEXT: return new Text(value.asChars());
    }

    return NullWritable.get();
  }

  public static Datum convertWritable2Datum(Writable value) {
    DataType type = convertWritableToTajoType(value.getClass());

    switch(type.getType()) {
      case INT1: return new Int2Datum(((ByteWritable)value).get());
      case INT2: return new Int2Datum(((ShortWritable)value).get());
      case INT4: return new Int4Datum(((IntWritable)value).get());
      case INT8: return new Int8Datum(((LongWritable)value).get());

      case FLOAT4: return new Float4Datum(((FloatWritable)value).get());
      case FLOAT8: return new Float8Datum(((DoubleWritable)value).get());

      case TEXT: return new TextDatum(value.toString());
    }

    return NullDatum.get();
  }
}
