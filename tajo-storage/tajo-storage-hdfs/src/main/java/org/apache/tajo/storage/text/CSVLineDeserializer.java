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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.FieldSerializerDeserializer;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.util.Arrays;

public class CSVLineDeserializer extends TextLineDeserializer {
  private ByteBufProcessor processor;
  private FieldSerializerDeserializer fieldSerDer;
  private ByteBuf nullChars;
  private int delimiterCompensation;

  private int [] targetColumnIndexes;

  public CSVLineDeserializer(Schema schema, TableMeta meta, Column [] projected) {
    super(schema, meta);
    targetColumnIndexes = PlannerUtil.getTargetIds(schema, projected);
  }

  @Override
  public void init() {
    byte[] delimiter = CSVLineSerDe.getFieldDelimiter(meta);
    if (delimiter.length == 1) {
      this.processor = new FieldSplitProcessor(delimiter[0]);
    } else {
      this.processor = new MultiBytesFieldSplitProcessor(delimiter);
    }
    this.delimiterCompensation = delimiter.length - 1;

    if (nullChars != null) {
      nullChars.release();
    }
    nullChars = TextLineSerDe.getNullChars(meta);

    fieldSerDer = new TextFieldSerializerDeserializer(meta);
    fieldSerDer.init(schema);
  }

  public void deserialize(final ByteBuf lineBuf, Tuple output) throws IOException, TextLineParsingError {
    if (lineBuf == null || targetColumnIndexes == null || targetColumnIndexes.length == 0) {
      return;
    }
    int[] projection = targetColumnIndexes;

    final int rowLength = lineBuf.readableBytes();
    int start = 0, fieldLength = 0, end = 0;

    // Projection
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
        try {
          Datum datum = fieldSerDer.deserialize(currentIndex, lineBuf, nullChars);
          output.put(currentTarget, datum);
        } catch (Exception e) {
          output.put(currentTarget, NullDatum.get());
        }
        currentTarget++;
      }

      if (projection.length == currentTarget) {
        break;
      }

      start = end + 1;
      currentIndex++;
    }

    /* If a text row is less than table schema size, tuple should set to NullDatum */
    if (projection.length > currentTarget) {
      for (; currentTarget < projection.length; currentTarget++) {
        output.put(currentTarget, NullDatum.get());
      }
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
