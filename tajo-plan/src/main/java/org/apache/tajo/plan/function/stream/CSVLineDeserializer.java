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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public class CSVLineDeserializer extends TextLineDeserializer {
  private ByteBufProcessor processor;
  private FieldSerializerDeserializer fieldSerDer;
  private ByteBuf nullChars;
  private int delimiterCompensation;

  public CSVLineDeserializer(Schema schema, TableMeta meta, int[] targetColumnIndexes) {
    super(schema, meta, targetColumnIndexes);
  }

  @Override
  public void init() {
    byte[] delimiter = CSVLineSerDe.getFieldDelimiter(meta);
    this.processor = new FieldSplitProcessor(delimiter[0]);
    this.delimiterCompensation = delimiter.length - 1;

    if (nullChars != null) {
      nullChars.release();
    }
    nullChars = TextLineSerDe.getNullChars(meta);

    fieldSerDer = new TextFieldSerializerDeserializer(meta);
  }

  public void deserialize(final ByteBuf lineBuf, Tuple output) throws IOException, TextLineParsingError {
    int[] projection = targetColumnIndexes;
    if (lineBuf == null || targetColumnIndexes == null || targetColumnIndexes.length == 0) {
      return;
    }

    final int rowLength = lineBuf.readableBytes();
    int start = 0, fieldLength = 0, end = 0;

    //Projection
    int currentTarget = 0;
    int currentIndex = 0;

    while (end != -1) {
      end = lineBuf.forEachByte(start, rowLength - start, processor);

      if (end < 0) {
        fieldLength = rowLength - start;
      } else {
        fieldLength = end - start - delimiterCompensation;
      }

      if (projection.length > currentTarget && currentIndex == projection[currentTarget]) {
        lineBuf.setIndex(start, start + fieldLength);
        Datum datum = fieldSerDer.deserialize(lineBuf, schema.getColumn(currentIndex), currentIndex, nullChars);
        output.put(currentIndex, datum);
        currentTarget++;
      }

      if (projection.length == currentTarget) {
        break;
      }

      start = end + 1;
      currentIndex++;
    }
  }

  @Override
  public void release() {
    if (nullChars != null) {
      nullChars.release();
      nullChars = null;
    }
  }
}
