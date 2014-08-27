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

package org.apache.tajo.storage;

import com.google.protobuf.Message;
import org.apache.commons.codec.binary.Base64;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.*;
import org.apache.tajo.datum.protobuf.ProtobufJsonFormat;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.NumberUtil;

import java.io.IOException;
import java.io.OutputStream;

//Compatibility with Apache Hive
public class TextSerializerDeserializer implements SerializerDeserializer {
  public static final byte[] trueBytes = "true".getBytes();
  public static final byte[] falseBytes = "false".getBytes();
  private ProtobufJsonFormat protobufJsonFormat = ProtobufJsonFormat.getInstance();


  @Override
  public int serialize(Column col, Datum datum, OutputStream out, byte[] nullCharacters) throws IOException {

    byte[] bytes;
    int length = 0;
    TajoDataTypes.DataType dataType = col.getDataType();

    if (datum == null || datum instanceof NullDatum) {
      switch (dataType.getType()) {
        case CHAR:
        case TEXT:
          length = nullCharacters.length;
          out.write(nullCharacters);
          break;
        default:
          break;
      }
      return length;
    }

    switch (dataType.getType()) {
      case BOOLEAN:
        out.write(datum.asBool() ? trueBytes : falseBytes);
        length = trueBytes.length;
        break;
      case CHAR:
        byte[] pad = new byte[dataType.getLength() - datum.size()];
        bytes = datum.asTextBytes();
        out.write(bytes);
        out.write(pad);
        length = bytes.length + pad.length;
        break;
      case TEXT:
      case BIT:
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8:
      case INET4:
      case DATE:
      case INTERVAL:
        bytes = datum.asTextBytes();
        length = bytes.length;
        out.write(bytes);
        break;
      case TIME:
        bytes = ((TimeDatum)datum).asChars(TajoConf.getCurrentTimeZone(), true).getBytes();
        length = bytes.length;
        out.write(bytes);
        break;
      case TIMESTAMP:
        bytes = ((TimestampDatum)datum).asChars(TajoConf.getCurrentTimeZone(), true).getBytes();
        length = bytes.length;
        out.write(bytes);
        break;
      case INET6:
      case BLOB:
        bytes = Base64.encodeBase64(datum.asByteArray(), false);
        length = bytes.length;
        out.write(bytes, 0, length);
        break;
      case PROTOBUF:
        ProtobufDatum protobuf = (ProtobufDatum) datum;
        byte[] protoBytes = protobufJsonFormat.printToString(protobuf.get()).getBytes();
        length = protoBytes.length;
        out.write(protoBytes, 0, protoBytes.length);
        break;
      case NULL_TYPE:
      default:
        break;
    }
    return length;
  }

  @Override
  public Datum deserialize(Column col, byte[] bytes, int offset, int length, byte[] nullCharacters) throws IOException {

    Datum datum;
    switch (col.getDataType().getType()) {
      case BOOLEAN:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createBool(bytes[offset] == 't' || bytes[offset] == 'T');
        break;
      case BIT:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createBit(Byte.parseByte(new String(bytes, offset, length)));
        break;
      case CHAR:
        datum = isNullText(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createChar(new String(bytes, offset, length).trim());
        break;
      case INT1:
      case INT2:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createInt2((short) NumberUtil.parseInt(bytes, offset, length));
        break;
      case INT4:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createInt4(NumberUtil.parseInt(bytes, offset, length));
        break;
      case INT8:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createInt8(new String(bytes, offset, length));
        break;
      case FLOAT4:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createFloat4(new String(bytes, offset, length));
        break;
      case FLOAT8:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createFloat8(NumberUtil.parseDouble(bytes, offset, length));
        break;
      case TEXT: {
        byte[] chars = new byte[length];
        System.arraycopy(bytes, offset, chars, 0, length);
        datum = isNullText(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createText(chars);
        break;
      }
      case DATE:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createDate(new String(bytes, offset, length));
        break;
      case TIME:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createTime(new String(bytes, offset, length));
        break;
      case TIMESTAMP:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createTimestamp(new String(bytes, offset, length));
        break;
      case INTERVAL:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createInterval(new String(bytes, offset, length));
        break;
      case PROTOBUF: {
        if (isNull(bytes, offset, length, nullCharacters)) {
          datum = NullDatum.get();
        } else {
          ProtobufDatumFactory factory = ProtobufDatumFactory.get(col.getDataType());
          Message.Builder builder = factory.newBuilder();
          try {
            byte[] protoBytes = new byte[length];
            System.arraycopy(bytes, offset, protoBytes, 0, length);
            protobufJsonFormat.merge(protoBytes, builder);
            datum = factory.createDatum(builder.build());
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
        break;
      }
      case INET4:
        datum = isNull(bytes, offset, length, nullCharacters) ? NullDatum.get()
            : DatumFactory.createInet4(new String(bytes, offset, length));
        break;
      case BLOB: {
        if (isNull(bytes, offset, length, nullCharacters)) {
          datum = NullDatum.get();
        } else {
          byte[] blob = new byte[length];
          System.arraycopy(bytes, offset, blob, 0, length);
          datum = DatumFactory.createBlob(Base64.decodeBase64(blob));
        }
        break;
      }
      default:
        datum = NullDatum.get();
        break;
    }
    return datum;
  }

  private static boolean isNull(byte[] val, int offset, int length, byte[] nullBytes) {
    return length == 0 || ((length == nullBytes.length)
        && Bytes.equals(val, offset, length, nullBytes, 0, nullBytes.length));
  }

  private static boolean isNullText(byte[] val, int offset, int length, byte[] nullBytes) {
    return length > 0 && length == nullBytes.length
        && Bytes.equals(val, offset, length, nullBytes, 0, nullBytes.length);
  }
}
