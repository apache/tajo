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

package org.apache.tajo.storage.kafka.serDe;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatumFactory;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.datum.protobuf.ProtobufJsonFormat;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.kafka.KafkaStorageConstants;
import org.apache.tajo.storage.text.TextFieldSerializerDeserializer;
import org.apache.tajo.storage.text.TextLineSerDe;
import org.apache.tajo.util.NumberUtil;
import org.apache.tajo.util.ReflectionUtil;

import com.google.protobuf.Message;

public class KafkaSerializerDeserializer extends TextFieldSerializerDeserializer {
  /** it caches serde classes. */
  private static final Map<String, Class<? extends TextLineSerDe>> serdeClassCache = new ConcurrentHashMap<String, Class<? extends TextLineSerDe>>();
  private static ProtobufJsonFormat protobufJsonFormat = ProtobufJsonFormat.getInstance();
  private final boolean hasTimezone;
  private final TimeZone timezone;

  public KafkaSerializerDeserializer(TableMeta meta) {
    super(meta);
    hasTimezone = meta.containsOption(StorageConstants.TIMEZONE);
    timezone = TimeZone.getTimeZone(meta.getOption(StorageConstants.TIMEZONE, TajoConstants.DEFAULT_SYSTEM_TIMEZONE));
  }

  @SuppressWarnings("unchecked")
  public static TextLineSerDe getTextSerde(TableMeta meta) {
    TextLineSerDe lineSerder;

    String serDeClassName;

    // if there is no given serde class, it will use Kafka message serder.
    serDeClassName = meta.getOption(KafkaStorageConstants.KAFKA_SERDE_CLASS,
        KafkaStorageConstants.DEFAULT_KAFKA_SERDE_CLASS);

    try {
      Class<? extends TextLineSerDe> serdeClass;

      if (serdeClassCache.containsKey(serDeClassName)) {
        serdeClass = serdeClassCache.get(serDeClassName);
      } else {
        serdeClass = (Class<? extends TextLineSerDe>) Class.forName(serDeClassName);
        serdeClassCache.put(serDeClassName, serdeClass);
      }
      lineSerder = (TextLineSerDe) ReflectionUtil.newInstance(serdeClass);
    } catch (Throwable e) {
      throw new RuntimeException("TextLineSerde class cannot be initialized.", e);
    }

    return lineSerder;
  }

  private static boolean isNull(ByteBuf val, ByteBuf nullBytes) {
    return !val.isReadable() || nullBytes.equals(val);
  }

  private static boolean isNullText(ByteBuf val, ByteBuf nullBytes) {
    return val.readableBytes() > 0 && nullBytes.equals(val);
  }

  @Override
  public Datum deserialize(ByteBuf buffer, Column col, int columnIndex, ByteBuf nullChars) throws IOException {
    Datum datum;
    TajoDataTypes.Type type = col.getDataType().getType();
    boolean nullField;
    if (type == TajoDataTypes.Type.TEXT || type == TajoDataTypes.Type.CHAR) {
      nullField = isNullText(buffer, nullChars);
    } else {
      nullField = isNull(buffer, nullChars);
    }

    if (nullField) {
      datum = NullDatum.get();
    } else {
      byte[] bytes = new byte[buffer.readableBytes()];
      buffer.readBytes(bytes);
      switch (type) {
      case BOOLEAN:
        byte bool = bytes[0];
        datum = DatumFactory.createBool(bool == 't' || bool == 'T');
        break;
      case BIT:
        datum = DatumFactory.createBit(Byte.parseByte(new String(bytes, TextDatum.DEFAULT_CHARSET)));
        break;
      case CHAR:
        datum = DatumFactory.createChar(Byte.parseByte(new String(bytes, TextDatum.DEFAULT_CHARSET)));
        break;
      case INT1:
      case INT2:
        datum = DatumFactory.createInt2((short) NumberUtil.parseInt(bytes, 0, bytes.length));
        break;
      case INT4:
        datum = DatumFactory.createInt4(NumberUtil.parseInt(bytes, 0, bytes.length));
        break;
      case INT8:
        datum = DatumFactory.createInt8(NumberUtil.parseLong(bytes, 0, bytes.length));
        break;
      case FLOAT4:
        datum = DatumFactory.createFloat4(new String(bytes, TextDatum.DEFAULT_CHARSET));
        break;
      case FLOAT8:
        datum = DatumFactory.createFloat8(NumberUtil.parseDouble(bytes, 0, bytes.length));
        break;
      case TEXT: {
        datum = DatumFactory.createText(bytes);
        break;
      }
      case DATE:
        datum = DatumFactory.createDate(new String(bytes, TextDatum.DEFAULT_CHARSET));
        break;
      case TIME:
        if (hasTimezone) {
          datum = DatumFactory.createTime(new String(bytes, TextDatum.DEFAULT_CHARSET), timezone);
        } else {
          datum = DatumFactory.createTime(new String(bytes, TextDatum.DEFAULT_CHARSET));
        }
        break;
      case TIMESTAMP:
        if (hasTimezone) {
          datum = DatumFactory.createTimestamp(new String(bytes, TextDatum.DEFAULT_CHARSET), timezone);
        } else {
          datum = DatumFactory.createTimestamp(new String(bytes, TextDatum.DEFAULT_CHARSET));
        }
        break;
      case INTERVAL:
        datum = DatumFactory.createInterval(new String(bytes, TextDatum.DEFAULT_CHARSET));
        break;
      case PROTOBUF: {
        ProtobufDatumFactory factory = ProtobufDatumFactory.get(col.getDataType());
        Message.Builder builder = factory.newBuilder();
        try {
          protobufJsonFormat.merge(bytes, builder);
          datum = factory.createDatum(builder.build());
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        break;
      }
      case INET4:
        datum = DatumFactory.createInet4(new String(bytes, TextDatum.DEFAULT_CHARSET));
        break;
      case BLOB: {
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
