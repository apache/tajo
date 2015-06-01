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

package org.apache.tajo.storage.hbase;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.Bytes;

import java.io.IOException;

public class HBaseBinarySerializerDeserializer {

  public static Datum deserialize(Column col, byte[] bytes) throws IOException {
    Datum datum;
    switch (col.getDataType().getType()) {
      case INT1:
      case INT2:
        datum = bytes == null || bytes.length == 0 ? NullDatum.get() : DatumFactory.createInt2(Bytes.toShort(bytes));
        break;
      case INT4:
        datum = bytes == null || bytes.length == 0 ? NullDatum.get() : DatumFactory.createInt4(Bytes.toInt(bytes));
        break;
      case INT8:
        if (bytes == null) {
          datum = NullDatum.get();
        } else {
          if (bytes.length == 4) {
            datum = DatumFactory.createInt8(Bytes.toInt(bytes));
          } else {
            datum = bytes.length == 0 ? NullDatum.get() : DatumFactory.createInt8(Bytes.toLong(bytes));
          }
        }
        break;
      case FLOAT4:
        datum = bytes == null || bytes.length == 0 ? NullDatum.get() : DatumFactory.createFloat4(Bytes.toFloat(bytes));
        break;
      case FLOAT8:
        datum = bytes == null || bytes.length == 0 ? NullDatum.get() : DatumFactory.createFloat8(Bytes.toDouble(bytes));
        break;
      case TEXT:
        datum = bytes == null ? NullDatum.get() : DatumFactory.createText(bytes);
        break;
      default:
        datum = NullDatum.get();
        break;
    }
    return datum;
  }

  public static byte[] serialize(Column col, Datum datum) throws IOException {
    if (datum == null || datum instanceof NullDatum) {
      return null;
    }

    byte[] bytes;
    switch (col.getDataType().getType()) {
      case INT1:
      case INT2:
        bytes = Bytes.toBytes(datum.asInt2());
        break;
      case INT4:
        bytes = Bytes.toBytes(datum.asInt4());
        break;
      case INT8:
        bytes = Bytes.toBytes(datum.asInt8());
        break;
      case FLOAT4:
        bytes = Bytes.toBytes(datum.asFloat4());
        break;
      case FLOAT8:
        bytes = Bytes.toBytes(datum.asFloat8());
        break;
      case TEXT:
        bytes = Bytes.toBytes(datum.asChars());
        break;
      default:
        bytes = null;
        break;
    }

    return bytes;
  }

  public static byte[] serialize(Column col, Tuple tuple, int index) throws IOException {
    if (tuple.isBlankOrNull(index)) {
      return null;
    }

    byte[] bytes;
    switch (col.getDataType().getType()) {
      case INT1:
      case INT2:
        bytes = Bytes.toBytes(tuple.getInt2(index));
        break;
      case INT4:
        bytes = Bytes.toBytes(tuple.getInt4(index));
        break;
      case INT8:
        bytes = Bytes.toBytes(tuple.getInt8(index));
        break;
      case FLOAT4:
        bytes = Bytes.toBytes(tuple.getFloat4(index));
        break;
      case FLOAT8:
        bytes = Bytes.toBytes(tuple.getFloat8(index));
        break;
      case TEXT:
        bytes = Bytes.toBytes(tuple.getText(index));
        break;
      default:
        bytes = null;
        break;
    }

    return bytes;
  }
}
