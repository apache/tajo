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

package tajo.storage.hcfile.reader;

import tajo.datum.ArrayDatum;
import tajo.datum.Datum;
import tajo.datum.json.GsonCreator;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ArrayReader extends TypeReader {

  @Override
  public Datum read(ByteBuffer buffer) throws IOException {
    if (buffer.hasRemaining()) {
      int arrayByteSize = buffer.getInt();
      byte [] arrayBytes = new byte[arrayByteSize];
      buffer.get(arrayBytes);
      String json = new String(arrayBytes);
      ArrayDatum array = (ArrayDatum) GsonCreator
          .getInstance().fromJson(json, Datum.class);
      return array;
    } else {
      return null;
    }
  }
}
