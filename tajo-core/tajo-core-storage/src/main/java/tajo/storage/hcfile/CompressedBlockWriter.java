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

package tajo.storage.hcfile;

import org.apache.hadoop.fs.FSDataOutputStream;
import tajo.catalog.proto.CatalogProtos.CompressType;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.storage.exception.UnknownCodecException;
import tajo.storage.hcfile.compress.Codec;
import tajo.storage.hcfile.writer.Writer;

import java.io.IOException;

public class CompressedBlockWriter implements Writer {
  private Codec codec;
  private DataType dataType;
  private FSDataOutputStream out;

  public CompressedBlockWriter(FSDataOutputStream out,
                               DataType dataType, CompressType compType)
      throws UnknownCodecException, IOException {
    codec = Codec.get(compType);
    this.dataType = dataType;
    this.out = out;
  }

  @Override
  public void write(Block block) throws IOException {
    byte[] in = null;
    byte[] buffer;

    // TODO
    buffer = codec.compress(in);
    out.write(buffer);
  }

  @Override
  public long getPos() throws IOException {
    return out.getPos();
  }

  @Override
  public void close() throws IOException {

  }
}
