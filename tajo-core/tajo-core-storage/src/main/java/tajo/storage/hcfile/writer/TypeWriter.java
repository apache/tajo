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

package tajo.storage.hcfile.writer;

import org.apache.hadoop.fs.FSDataOutputStream;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.storage.exception.UnknownDataTypeException;

import java.io.IOException;

public abstract class TypeWriter {

  protected final FSDataOutputStream out;

  public static TypeWriter get(FSDataOutputStream out,
                               DataType type)
      throws IOException, UnknownDataTypeException {

    switch (type) {
      case BOOLEAN :
      case BYTE :
        return new ByteWriter(out);

      case CHAR :
        return new CharWriter(out);

      case SHORT :
        return new ShortWriter(out);

      case INT :
        return new IntWriter(out);

      case LONG :
        return new LongWriter(out);

      case FLOAT :
        return new FloatWriter(out);

      case DOUBLE:
        return new DoubleWriter(out);

      case STRING2:
      case STRING:
      case BYTES:
      case IPv4:
        return new BytesWriter(out);

      case ARRAY:
        return new ArrayWriter(out);

      default:
        throw new UnknownDataTypeException(type.toString());
    }
  }

  protected TypeWriter(FSDataOutputStream out) {
    this.out = out;
  }

  public FSDataOutputStream getOutputStream() {
    return this.out;
  }

  public abstract void write(Datum data) throws IOException;

  public long getPos() throws IOException {
    return out.getPos();
  }

  public void close() throws IOException {

  }
}
