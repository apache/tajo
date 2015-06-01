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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.datum.protobuf.ProtobufJsonFormat;
import org.apache.tajo.exception.ValueTooLongForTypeCharactersException;
import org.apache.tajo.storage.FieldSerializerDeserializer;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.NumberUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.CharsetDecoder;
import java.util.TimeZone;

public class TextFieldSerializerDeserializer implements FieldSerializerDeserializer {
  private static final byte[] trueBytes = "true".getBytes(Bytes.UTF8_CHARSET);
  private static final byte[] falseBytes = "false".getBytes(Bytes.UTF8_CHARSET);
  private static ProtobufJsonFormat protobufJsonFormat = ProtobufJsonFormat.getInstance();
  private final CharsetDecoder decoder = CharsetUtil.getDecoder(CharsetUtil.UTF_8);

  private final boolean hasTimezone;
  private final TimeZone timezone;

  private Schema schema;

  public TextFieldSerializerDeserializer(TableMeta meta) {
    hasTimezone = meta.containsOption(StorageConstants.TIMEZONE);
    timezone = TimeZone.getTimeZone(meta.getOption(StorageConstants.TIMEZONE, TajoConstants.DEFAULT_SYSTEM_TIMEZONE));
  }

  private static boolean isNull(ByteBuf val, ByteBuf nullBytes) {
    return !val.isReadable() || nullBytes.equals(val);
  }

  private static boolean isNullText(ByteBuf val, ByteBuf nullBytes) {
    return val.readableBytes() > 0 && nullBytes.equals(val);
  }

  @Override
  public void init(Schema schema) {
    this.schema = schema;
  }

  @Override
  public int serialize(int columnIndex, Tuple tuple, OutputStream out, byte[] nullChars)
      throws IOException {
    byte[] bytes;
    int length = 0;
    Column col = schema.getColumn(columnIndex);
    TajoDataTypes.DataType dataType = col.getDataType();

    if (tuple.isBlankOrNull(columnIndex)) {
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
        out.write(tuple.getBool(columnIndex) ? trueBytes : falseBytes);
        length = trueBytes.length;
        break;
      case CHAR:
        int size = dataType.getLength() - tuple.size(columnIndex);
        if (size < 0){
          throw new ValueTooLongForTypeCharactersException(dataType.getLength());
        }

        byte[] pad = new byte[size];
        bytes = tuple.getBytes(columnIndex);
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
        bytes = tuple.getTextBytes(columnIndex);
        length = bytes.length;
        out.write(bytes);
        break;
      case TIME:
        if (hasTimezone) {
          bytes = TimeDatum.asChars(tuple.getTimeDate(columnIndex), timezone, true).getBytes(Bytes.UTF8_CHARSET);
        } else {
          bytes = tuple.getTextBytes(columnIndex);
        }
        length = bytes.length;
        out.write(bytes);
        break;
      case TIMESTAMP:
        if (hasTimezone) {
          bytes = TimestampDatum.asChars(tuple.getTimeDate(columnIndex), timezone, true).getBytes(Bytes.UTF8_CHARSET);
        } else {
          bytes = tuple.getTextBytes(columnIndex);
        }
        length = bytes.length;
        out.write(bytes);
        break;
      case INET6:
      case BLOB:
        bytes = Base64.encodeBase64(tuple.getBytes(columnIndex), false);
        length = bytes.length;
        out.write(bytes, 0, length);
        break;
      case PROTOBUF:
        ProtobufDatum protobuf = (ProtobufDatum) tuple.getProtobufDatum(columnIndex);
        byte[] protoBytes = protobufJsonFormat.printToString(protobuf.get()).getBytes(Bytes.UTF8_CHARSET);
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
  public Datum deserialize(int columnIndex, ByteBuf buf, ByteBuf nullChars) throws IOException {
    Datum datum;

    Column col = schema.getColumn(columnIndex);
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
          datum = DatumFactory.createBit(Byte.parseByte(
              decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString()));
          break;
        case CHAR:
          datum = DatumFactory.createChar(
              decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString().trim());
          break;
        case INT1:
        case INT2:
          datum = DatumFactory.createInt2((short) NumberUtil.parseInt(buf));
          break;
        case INT4:
          datum = DatumFactory.createInt4(NumberUtil.parseInt(buf));
          break;
        case INT8:
          datum = DatumFactory.createInt8(NumberUtil.parseLong(buf));
          break;
        case FLOAT4:
          datum = DatumFactory.createFloat4(
              decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString());
          break;
        case FLOAT8:
          datum = DatumFactory.createFloat8(NumberUtil.parseDouble(buf));
          break;
        case TEXT: {
          byte[] bytes = new byte[buf.readableBytes()];
          buf.readBytes(bytes);
          datum = DatumFactory.createText(bytes);
          break;
        }
        case DATE:
          datum = DatumFactory.createDate(
              decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString());
          break;
        case TIME:
          if (hasTimezone) {
            datum = DatumFactory.createTime(
                decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString(), timezone);
          } else {
            datum = DatumFactory.createTime(
                decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString());
          }
          break;
        case TIMESTAMP:
          if (hasTimezone) {
            datum = DatumFactory.createTimestamp(
                decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString(), timezone);
          } else {
            datum = DatumFactory.createTimestamp(
                decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString());
          }
          break;
        case INTERVAL:
          datum = DatumFactory.createInterval(
              decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString());
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
          datum = DatumFactory.createInet4(
              decoder.decode(buf.nioBuffer(buf.readerIndex(), buf.readableBytes())).toString());
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
