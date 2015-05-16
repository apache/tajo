/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.FileUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@link OutputHandler} is responsible for handling the output of the
 * Tajo-Streaming external command.
 */
public class OutputHandler implements Closeable {

  private static final Log LOG = LogFactory.getLog(OutputHandler.class);
  private static int DEFAULT_BUFFER = 64 * 1024;
  private final static byte[] END_OF_RECORD_DELIM = "|_\n".getBytes();

  private final TextLineDeserializer deserializer;

  private ByteBufLineReader in = null;

  private String currValue = null;

  private InputStream istream;

  private ByteBuf buf;

  // Both of these ignore the trailing "\n".  So if the default delimiter is "\n", recordDelimStr is "".
  private String recordDelimStr = null;
  private int recordDelimLength = 0;
  private final Tuple tuple;

  // flag to mark if close() has already been called
  private boolean alreadyClosed = false;
  private final String FIELD_DELIM;

  public OutputHandler(TextLineDeserializer deserializer) {
    this.deserializer = deserializer;
    FIELD_DELIM = new String(CSVLineSerDe.getFieldDelimiter(deserializer.meta));
    tuple = new VTuple(deserializer.schema.size());
  }

  /**
   * Bind the <code>OutputHandler</code> to the <code>InputStream</code>
   * from which to read the output data of the managed process.
   *
   * @param is <code>InputStream</code> from which to read the output data
   *           of the managed process
   * @throws IOException
   */
  public void bindTo(InputStream is) throws IOException {
    this.istream  = is;
    this.in = new ByteBufLineReader(new ByteBufInputChannel(istream));
    this.buf = BufferPool.directBuffer(DEFAULT_BUFFER);
  }

  /**
   * Get the next output <code>Tuple</code> of the managed process.
   *
   * @return the next output <code>Tuple</code> of the managed process
   * @throws IOException
   */
  public Tuple getNext() throws IOException {
    if (in == null) {
      return null;
    }

    currValue = null;
    if (!readValue()) {
      return null;
    }
    buf.clear();
    buf.writeBytes(currValue.getBytes());
    try {
      deserializer.deserialize(buf, tuple);
    } catch (TextLineParsingError textLineParsingError) {
      throw new IOException(textLineParsingError);
    }
    return tuple;
  }

  public String getPartialResultString() throws IOException {
    if (in == null) {
      return null;
    }

    currValue = null;
    if (!readValue()) {
      return null;
    }
    return currValue;
  }

  public FunctionContext getNext(FunctionContext context) throws IOException {
    if (in == null) {
      return null;
    }

    currValue = null;
    if (!readValue()) {
      return null;
    }
    buf.clear();
    buf.writeBytes(currValue.getBytes());
    try {
      deserializer.deserialize(buf, context);
    } catch (TextLineParsingError textLineParsingError) {
      throw new IOException(textLineParsingError);
    }

    return context;
  }

  private boolean readValue() throws IOException {
    currValue = in.readLine();
    if (currValue == null) {
      return false;
    }

    while(!isEndOfRow()) {
      // Need to add back the newline character we ate.
      currValue += '\n';

      byte[] lineBytes = readNextLine();
      if (lineBytes == null) {
        // We have no more input, so just break;
        break;
      }
      currValue += new String(lineBytes);
    }

    String line = currValue;

    int candidate = -1;
    StringBuffer sb = new StringBuffer();
    while ((candidate=line.indexOf("|")) >= 0) {
      if (line.substring(candidate, candidate+2).equals("|_")) {
        // record end
        sb.append(line.substring(0, candidate));
        break;
      } else if (line.substring(candidate, candidate+3).equals("|,_")) {
        sb.append(line.substring(0, candidate)).append(FIELD_DELIM);
        line = line.substring(candidate+3, line.length());
      } else if (line.substring(candidate, candidate+3).equals("|-_")) {
        // null value
        sb.append(FIELD_DELIM);
        line = line.substring(candidate+3, line.length());
      }
    }
    currValue = sb.toString();

    return true;
  }

  private byte[] readNextLine() throws IOException {
    String line = in.readLine();
    if (line == null) {
      return null;
    }

    return line.getBytes();
  }

  private boolean isEndOfRow() {
    if (recordDelimStr == null) {
      byte[] recordDelimBa = END_OF_RECORD_DELIM;
      recordDelimLength = recordDelimBa.length - 1; //Ignore trailing \n
      recordDelimStr = new String(recordDelimBa, 0, recordDelimLength,  Charsets.UTF_8);
    }
    if (recordDelimLength == 0 || currValue.length() < recordDelimLength) {
      return true;
    }
    return currValue.contains(recordDelimStr);
  }

  /**
   * Close the <code>OutputHandler</code>.
   * @throws IOException
   */
  public void close() throws IOException {
    if(!alreadyClosed) {
      FileUtil.cleanup(LOG, istream, in);
      in = null;
      istream = null;
      alreadyClosed = true;
      if (this.buf.refCnt() > 0) {
        this.buf.release();
      }
    }
  }
}
