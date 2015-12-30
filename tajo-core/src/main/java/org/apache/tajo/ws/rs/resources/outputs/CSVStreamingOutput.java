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

package org.apache.tajo.ws.rs.resources.outputs;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.ws.rs.annotation.RestReturnType;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

@RestReturnType(
  mimeType = "text/csv"
)
public class CSVStreamingOutput extends AbstractStreamingOutput {
  private String output;
  private boolean alreadyCalculated = false;
  private int size = 0;

  public CSVStreamingOutput(NonForwardQueryResultScanner cachedQueryResultScanner, Integer count, Integer startOffset) throws IOException {
    super(cachedQueryResultScanner, count, startOffset);
  }

  @Override
  public boolean hasLength() {
    return true;
  }

  @Override
  public int length() {
    try {
      fetch();
      return output.length();
    } catch (Exception e) {
      return 0;
    }
  }

  @Override
  public int count() {
    try {
      fetch();
      return size;
    } catch (Exception e) {
      return 0;
    }
  }

  private void fetch() throws IOException {
    if (output != null) {
      return;
    }

    List<Tuple> outputTupletList = scanner.getNextTupleRows(count);
    size = outputTupletList.size();

    StringBuilder sb = new StringBuilder();
    if (startOffset == 0) {
      Schema schema = this.scanner.getLogicalSchema();
      List<Column> columns = schema.getAllColumns();
      boolean first = true;
      for (Column column : columns) {
        if (!first) {
          sb.append(",");
        }

        sb.append(StringEscapeUtils.escapeCsv(column.getSimpleName()));
        first = false;
      }

      sb.append("\r\n");
    }

    for (Tuple tuple : outputTupletList) {
      Datum[] datums = tuple.getValues();
      int size = datums.length;

      for (int i = 0; i < size; i++) {
        if (i != 0) {
          sb.append(",");
        }
        Datum datum = datums[i];
        sb.append(StringEscapeUtils.escapeCsv(datum.toString()));
      }

      sb.append("\r\n");
    }

    output = sb.toString();
  }

  @Override
  public void write(OutputStream outputStream) throws IOException, WebApplicationException {
    fetch();
    DataOutputStream streamingOutputStream = new DataOutputStream(new BufferedOutputStream(outputStream));
    streamingOutputStream.write(output.getBytes("utf-8"));
    streamingOutputStream.flush();
  }

  @Override
  public String contentType() {
    return MediaType.APPLICATION_JSON;
  }
}
