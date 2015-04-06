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
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link OutputHandler} is responsible for handling the output of the
 * Tajo-Streaming external command.
 *
 * The output of the managed executable could be fetched in a
 * {@link OutputType#SYNCHRONOUS} manner via its <code>stdout</code> or in an
 * {@link OutputType#ASYNCHRONOUS} manner via an external file to which the
 * process wrote its output.
 */
public abstract class OutputHandler {
  private static int DEFAULT_BUFFER = 64 * 1024;
  public static final Object END_OF_OUTPUT = new Object();
  private static final byte[] DEFAULT_RECORD_DELIM = new byte[] {'\n'};

  public enum OutputType {SYNCHRONOUS, ASYNCHRONOUS}

  protected TextLineDeserializer deserializer;

  protected ByteBufLineReader in = null;

  private String currValue = null;

  private InputStream istream;

  //Both of these ignore the trailing \n.  So if the
  //default delimiter is "\n" recordDelimStr is "".
  private String recordDelimStr = null;
  private int recordDelimLength = 0;
  private Tuple tuple = new VTuple(1);

  /**
   * Get the handled <code>OutputType</code>.
   * @return the handled <code>OutputType</code>
   */
  public abstract OutputType getOutputType();

  // flag to mark if close() has already been called
  protected boolean alreadyClosed = false;

  /**
   * Bind the <code>OutputHandler</code> to the <code>InputStream</code>
   * from which to read the output data of the managed process.
   *
   * @param is <code>InputStream</code> from which to read the output data
   *           of the managed process
   * @throws IOException
   */
  public void bindTo(String fileName, InputStream is,
                     long offset, long end) throws IOException {
    this.istream  = is;
    this.in = new ByteBufLineReader(new ByteBufInputChannel(istream));

    // TODO
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
    ByteBuf buf = BufferPool.directBuffer(DEFAULT_BUFFER);
    buf.writeBytes(currValue.getBytes());
    try {
      deserializer.deserialize(buf, tuple);
    } catch (TextLineParsingError textLineParsingError) {
      throw new IOException(textLineParsingError);
    }
    return tuple;
  }

  private boolean readValue() throws IOException {
    currValue = in.readLine();
    if (currValue == null) {
      return false;
    }

    while(!isEndOfRow()) {
      //Need to add back the newline character we ate.
//      currValue.append(new byte[] {'\n'}, 0, 1);
      currValue += '\n';

      byte[] lineBytes = readNextLine();
      if (lineBytes == null) {
        //We have no more input, so just break;
        break;
      }
//      currValue.append(lineBytes, 0, lineBytes.length);
      currValue += new String(lineBytes);
    }

    if (currValue.contains("|_")) {
      int pos = currValue.lastIndexOf("|_");
      currValue = currValue.substring(0, pos);
    }

    return true;
  }

  private byte[] readNextLine() throws IOException {
//    Text line = new Text();
    String line = in.readLine();
    if (line == null) {
      return null;
    }

    return line.getBytes();
  }

  private boolean isEndOfRow() {
    if (recordDelimStr == null) {
      byte[] recordDelimBa = getRecordDelimiter();
      recordDelimLength = recordDelimBa.length - 1; //Ignore trailing \n
      recordDelimStr = new String(recordDelimBa, 0, recordDelimLength,  Charsets.UTF_8);
    }
    if (recordDelimLength == 0 || currValue.length() < recordDelimLength) {
      return true;
    }
    return currValue.contains(recordDelimStr);
  }

  protected byte[] getRecordDelimiter() {
    return DEFAULT_RECORD_DELIM;
  }

  /**
   * Close the <code>OutputHandler</code>.
   * @throws IOException
   */
  public synchronized void close() throws IOException {
    if(!alreadyClosed) {
      istream.close();
      istream = null;
      alreadyClosed = true;
    }
  }
}
