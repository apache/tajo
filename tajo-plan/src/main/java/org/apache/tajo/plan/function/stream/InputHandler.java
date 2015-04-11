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

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link InputHandler} is responsible for handling the input to the Tajo-Streaming external command.
 *
 */
public class InputHandler {

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

  /**
   * Close the <code>InputHandler</code> since there is no more input
   * to be sent to the managed process.
   * @param process the managed process - this could be null in some cases
   * like when input is through files. In that case, the process would not
   * have been exec'ed yet - if this method if overridden it is the responsibility
   * of the implementer to check that the process is usable. The managed process
   * object is supplied by the ExecutableManager to this call so that this method
   * can check if the process is alive if it needs to know.
   *
   * @throws IOException
   */
  public synchronized void close(Process process) throws IOException {
    try {
      if (!alreadyClosed) {
        alreadyClosed = true;
        out.flush();
        out.close();
        out = null;
      }
    } catch(IOException e) {
      // check if we got an exception because
      // the process actually completed and we were
      // trying to flush and close it's stdin
      if (process == null || process.exitValue() != 0) {
        // the process had not terminated normally
        // throw the exception we got
        throw e;
      }
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
