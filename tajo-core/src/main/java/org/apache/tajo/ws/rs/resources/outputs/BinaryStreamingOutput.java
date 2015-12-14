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

import com.google.protobuf.ByteString;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.ws.rs.annotation.RestReturnType;

import javax.ws.rs.WebApplicationException;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@RestReturnType(
  mimeType = "application/octet-stream"
)
public class BinaryStreamingOutput extends AbstractStreamingOutput {
  private List<byte[]> byteOutputLists = null;
  private int length = -1;

  public BinaryStreamingOutput(NonForwardQueryResultScanner scanner, Integer count, Integer startOffset) throws IOException {
    super(scanner, count, startOffset);
  }

  @Override
  public boolean hasLength() {
    return true;
  }

  private void fetch() {
    if (length == -1) {
      length = fill();
    }
  }

  @Override
  public int length() {
    fetch();
    return length;
  }

  @Override
  public int count() {
    try {
      fetch();
      return byteOutputLists.size();
    } catch (Exception e) {
      return 0;
    }
  }

  @Override
  public void write(OutputStream outputStream) throws IOException, WebApplicationException {
    fetch();

    DataOutputStream streamingOutputStream = new DataOutputStream(new BufferedOutputStream(outputStream));

    for (byte[] bytes: byteOutputLists) {
      streamingOutputStream.writeInt(bytes.length);
      streamingOutputStream.write(bytes);
    }

    streamingOutputStream.flush();
  }

  private int fill() {
    int tmpLen = 0;
    try {
      byteOutputLists = new ArrayList<byte[]>();

      List<ByteString> outputList = scanner.getNextRows(count);
      for (ByteString byteString : outputList) {
        byte[] byteStringArray = byteString.toByteArray();
        byteOutputLists.add(byteStringArray);
        tmpLen += 4;
        tmpLen += byteStringArray.length;
      }
    } catch (IOException e) {
    }

    return tmpLen;
  }
}
