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

package org.apache.tajo.plan.function.stream;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.AnyDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.io.OutputStream;

public class CSVLineSerializer extends TextLineSerializer {
  private FieldSerializerDeserializer serde;

  private byte[] nullChars;
  private byte[] delimiter;
  private int columnNum;

  private final static String PARAM_DELIM = "|\t_";

  public CSVLineSerializer(Schema schema, TableMeta meta) {
    super(schema, meta);
  }

  @Override
  public void init() {
    nullChars = TextLineSerDe.getNullCharsAsBytes(meta);
    delimiter = "|,_".getBytes();
    columnNum = schema.size();

    serde = new TextFieldSerializerDeserializer(meta);
  }

  @Override
  public int serialize(OutputStream out, Tuple input) throws IOException {
    int writtenBytes = 0;

    for (int i = 0; i < columnNum; i++) {
      Datum datum = input.get(i);
      String typeStr;
      if (datum.type() == TajoDataTypes.Type.ANY) {
        typeStr = getTypeString(((AnyDatum)datum).getActual());
      } else {
        typeStr = getTypeString(datum);
      }
      out.write(typeStr.getBytes());
      out.write(PARAM_DELIM.getBytes());

      writtenBytes += serde.serialize(out, datum, schema.getColumn(i), i, nullChars);

      if (columnNum - 1 > i) {
        out.write(delimiter);
        writtenBytes += delimiter.length;
      }
    }

    return writtenBytes;
  }

  @Override
  public void release() {

  }

  private static String getTypeString(Datum val) {
    switch (val.type()) {
      case NULL_TYPE:
        return "-";
      case BOOLEAN:
        return "B";
      case INT1:
      case INT2:
      case INT4:
        return "I";
      case INT8:
        return "L";
      case FLOAT4:
        return "F";
      case FLOAT8:
        return "D";
      case NUMERIC:
        return "E";
      case CHAR:
      case TEXT:
        return "C";
      case DATE:
      case TIME:
      case TIMESTAMP:
        return "T";
      case BLOB:
      case INET4:
      case INET6:
        return "A";
      default:
        throw new UnsupportedException(val.type().name());
    }
  }
}
