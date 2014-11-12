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

package org.apache.tajo.storage.text;

import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.*;
import org.apache.tajo.datum.protobuf.ProtobufJsonFormat;
import org.apache.tajo.storage.FieldSerializerDeserializer;
import org.apache.tajo.util.NumberUtil;

import java.io.IOException;
import java.io.OutputStream;

//Compatibility with Apache Hive
public class TextFieldSerializerDeserializer implements FieldSerializerDeserializer {
  public static final byte[] trueBytes = "true".getBytes();
  public static final byte[] falseBytes = "false".getBytes();
  private ProtobufJsonFormat protobufJsonFormat = ProtobufJsonFormat.getInstance();

  private static boolean isNull(ByteBuf val, ByteBuf nullBytes) {
    return !val.isReadable() || nullBytes.equals(val);
  }

  private static boolean isNullText(ByteBuf val, ByteBuf nullBytes) {
    return val.readableBytes() > 0 && nullBytes.equals(val);
  }

  @Override
  public int serialize(OutputStream out, Datum datum, Column col, int columnIndex, byte[] nullChars) throws IOException {
    byte[] bytes;
    int length = 0;
    TajoDataTypes.DataType dataType = col.getDataType();

    if (datum == null || datum instanceof NullDatum) {
      switch (dataType.getType()) {
        case CHAR:
        case TEXT:
          length = nullChars.length;
          out.write(nullChars);
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
        bytes = ((TimeDatum) datum).asChars(TajoConf.getCurrentTimeZone(), true).getBytes();
        length = bytes.length;
        out.write(bytes);
        break;
      case TIMESTAMP:
        bytes = ((TimestampDatum) datum).asChars(TajoConf.getCurrentTimeZone(), true).getBytes();
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
  public Datum deserialize(ByteBuf buf, Column col, int columnIndex, ByteBuf nullChars) throws IOException{
    Datum datum;
    TajoDataTypes.Type type = col.getDataType().getType();
    boolean nullField;
    if (type == TajoDataTypes.Type.TEXT || type == TajoDataTypes.Type.CHAR) {
      nullField = isNullText(buf, nullChars);
    } else {
      nullField = isNull(buf, nullChars);
    }

    if (nullField) {
      datum = NullDatum.get();
    } else {
      switch (type) {
        case BOOLEAN:
          byte bool = buf.readByte();
          datum = DatumFactory.createBool(bool == 't' || bool == 'T');
          break;
        case BIT:
          datum = DatumFactory.createBit(Byte.parseByte(buf.toString(CharsetUtil.UTF_8)));
          break;
        case CHAR:
          datum = DatumFactory.createChar(buf.toString(CharsetUtil.UTF_8).trim());
          break;
        case INT1:
        case INT2: {
          //TODO zero-copy
          byte[] bytes = new byte[buf.readableBytes()];
          buf.readBytes(bytes);
          datum = DatumFactory.createInt2((short) NumberUtil.parseInt(bytes, 0, bytes.length));
          break;
        }
        case INT4: {
          //TODO zero-copy
          byte[] bytes = new byte[buf.readableBytes()];
          buf.readBytes(bytes);
          datum = DatumFactory.createInt4(NumberUtil.parseInt(bytes, 0, bytes.length));
          break;
        }
        case INT8:
          //TODO zero-copy
          datum = DatumFactory.createInt8(buf.toString(CharsetUtil.UTF_8));
          break;
        case FLOAT4:
          //TODO zero-copy
          datum = DatumFactory.createFloat4(buf.toString(CharsetUtil.UTF_8));
          break;
        case FLOAT8: {
          //TODO zero-copy
          byte[] bytes = new byte[buf.readableBytes()];
          buf.readBytes(bytes);
          datum = DatumFactory.createFloat8(NumberUtil.parseDouble(bytes, 0, bytes.length));
          break;
        }
        case TEXT: {
          byte[] bytes = new byte[buf.readableBytes()];
          buf.readBytes(bytes);
          datum = DatumFactory.createText(bytes);
          break;
        }
        case DATE:
          datum = DatumFactory.createDate(buf.toString(CharsetUtil.UTF_8));
          break;
        case TIME:
          datum = DatumFactory.createTime(buf.toString(CharsetUtil.UTF_8));
          break;
        case TIMESTAMP:
          datum = DatumFactory.createTimestamp(buf.toString(CharsetUtil.UTF_8));
          break;
        case INTERVAL:
          datum = DatumFactory.createInterval(buf.toString(CharsetUtil.UTF_8));
          break;
        case PROTOBUF: {
          ProtobufDatumFactory factory = ProtobufDatumFactory.get(col.getDataType());
          Message.Builder builder = factory.newBuilder();
          try {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            protobufJsonFormat.merge(bytes, builder);
            datum = factory.createDatum(builder.build());
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        }
        case INET4:
          datum = DatumFactory.createInet4(buf.toString(CharsetUtil.UTF_8));
          break;
        case BLOB: {
          byte[] bytes = new byte[buf.readableBytes()];
          buf.readBytes(bytes);
          datum = DatumFactory.createBlob(Base64.decodeBase64(bytes));
          break;
        }
        default:
          datum = NullDatum.get();
          break;
      }
    }
    return datum;
  }
}
