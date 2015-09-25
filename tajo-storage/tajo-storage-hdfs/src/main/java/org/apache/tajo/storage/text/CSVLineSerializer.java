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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.FieldSerializerDeserializer;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.io.OutputStream;

public class CSVLineSerializer extends TextLineSerializer {
  private FieldSerializerDeserializer serde;

  private byte[] nullChars;
  private byte[] delimiter;
  private int columnNum;

  public CSVLineSerializer(Schema schema, TableMeta meta) {
    super(schema, meta);
  }

  @Override
  public void init() {
    nullChars = TextLineSerDe.getNullCharsAsBytes(meta);
    delimiter = CSVLineSerDe.getFieldDelimiter(meta);
    columnNum = schema.size();

    serde = new TextFieldSerializerDeserializer(meta);
    serde.init(schema);
  }

  @Override
  public int serialize(OutputStream out, Tuple input) throws IOException {
    int writtenBytes = 0;

    for (int i = 0; i < columnNum; i++) {
      writtenBytes += serde.serialize(i, input, out, nullChars);

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
}
