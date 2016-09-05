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

package org.apache.tajo.storage.regex;


import io.netty.util.CharsetUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.datum.protobuf.ProtobufJsonFormat;
import org.apache.tajo.exception.InvalidTablePropertyException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.ValueTooLongForTypeCharactersException;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.text.TextLineSerDe;
import org.apache.tajo.storage.text.TextLineSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.TimeZone;

public class RegexLineSerializer extends TextLineSerializer {
  private static ProtobufJsonFormat protobufJsonFormat = ProtobufJsonFormat.getInstance();
  private String outputFormatString;
  private TimeZone tableTimezone;
  private int columnNum;
  private String nullChars;

  public RegexLineSerializer(Schema schema, TableMeta meta) {
    super(schema, meta);
  }

  @Override
  public void init() {
    // Read the configuration parameters
    outputFormatString = meta.getProperty(StorageConstants.TEXT_REGEX_OUTPUT_FORMAT_STRING);

    if (outputFormatString == null) {
      throw new TajoRuntimeException(new InvalidTablePropertyException(StorageConstants.TEXT_REGEX_OUTPUT_FORMAT_STRING,
          "Cannot write data into table because \"" + StorageConstants.TEXT_REGEX_OUTPUT_FORMAT_STRING + "\""
              + " is not specified in serde properties of the table."));
    }

    tableTimezone = TimeZone.getTimeZone(meta.getProperty(StorageConstants.TIMEZONE,
        StorageUtil.TAJO_CONF.getSystemTimezone().getID()));
    nullChars = new String(TextLineSerDe.getNullCharsAsBytes(meta), CharsetUtil.UTF_8);
    columnNum = schema.size();
  }

  @Override
  public int serialize(OutputStream out, Tuple input) throws IOException {

    String[] values = new String[columnNum];

    for (int i = 0; i < columnNum; i++) {
      values[i] = convertToString(i, input, nullChars);
    }

    byte[] bytes = String.format(outputFormatString, values).getBytes(CharsetUtil.UTF_8);
    out.write(bytes);
    return bytes.length;
  }


  private String convertToString(int columnIndex, Tuple tuple, String nullChars)
      throws IOException {

    Column col = schema.getColumn(columnIndex);
    TajoDataTypes.DataType dataType = col.getDataType();

    if (tuple.isBlankOrNull(columnIndex)) {
      switch (dataType.getType()) {
      case CHAR:
      case TEXT:
        return nullChars;
      default:
        return StringUtils.EMPTY;
      }
    }

    switch (dataType.getType()) {
    case BOOLEAN:
      return tuple.getBool(columnIndex) ? "true" : "false";
    case CHAR:
      int size = dataType.getLength() - tuple.size(columnIndex);
      if (size < 0) {
        throw new ValueTooLongForTypeCharactersException(dataType.getLength());
      }

      return StringUtils.rightPad(tuple.getText(columnIndex), size, "");
    case TEXT:
    case BIT:
    case INT2:
    case INT4:
    case INT8:
    case FLOAT4:
    case FLOAT8:
    case DATE:
    case INTERVAL:
    case TIME:
      return tuple.getText(columnIndex);
    case TIMESTAMP:
      // UTC to table timezone
      return TimestampDatum.asChars(tuple.getTimeDate(columnIndex), tableTimezone, false);
    case BLOB:
      return Base64.encodeBase64String(tuple.getBytes(columnIndex));
    case PROTOBUF:
      ProtobufDatum protobuf = (ProtobufDatum) tuple.getProtobufDatum(columnIndex);
      return protobufJsonFormat.printToString(protobuf.get());
    case NULL_TYPE:
    default:
      return StringUtils.EMPTY;
    }
  }

  @Override
  public void release() {
  }
}
