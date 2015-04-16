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

import org.apache.tajo.storage.Tuple;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link InputHandler} is responsible for handling the input to the Tajo-Streaming external command.
 *
 */
public class InputHandler implements Closeable {

  private final static byte[] END_OF_RECORD_DELIM = "|_\n".getBytes();
  private final static byte[] END_OF_STREAM = ("C" + "\\x04" + "|_\n").getBytes();

  private final TextLineSerializer serializer;

  private OutputStream out;

  // flag to mark if close() has already been called
  private boolean alreadyClosed = false;

  public InputHandler(TextLineSerializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Send the given input <code>Tuple</code> to the managed executable.
   *
   * @param t input <code>Tuple</code>
   * @throws IOException
   */
  public void putNext(Tuple t) throws IOException {
    serializer.serialize(out, t);
    out.write(END_OF_RECORD_DELIM);
  }

  public void close() throws IOException {
    if (!alreadyClosed) {
      out.flush();
      out.close();
      out = null;
      alreadyClosed = true;
    }
  }

  /**
   * Bind the <code>InputHandler</code> to the <code>OutputStream</code>
   * from which it reads input and sends it to the managed process.
   *
   * @param os <code>OutputStream</code> from which to read input data for the
   *           managed process
   * @throws IOException
   */
  public void bindTo(OutputStream os) throws IOException {
    out = os;
  }
}
