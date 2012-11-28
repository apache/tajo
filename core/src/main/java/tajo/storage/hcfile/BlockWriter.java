/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.storage.hcfile.writer.TypeWriter;
import tajo.storage.hcfile.writer.Writer;
import tajo.storage.exception.UnknownDataTypeException;

import java.io.IOException;

/**
 * BlockWriter writes a block to the disk.
 */
public class BlockWriter implements Writer {
  private TypeWriter writer;

  public BlockWriter(FSDataOutputStream out, DataType type)
      throws IOException, UnknownDataTypeException {
    writer = TypeWriter.get(out, type);
  }

  public void write(Block block) throws IOException {
    writer.getOutputStream().writeInt(block.getSize());
    for (Datum datum : block.asArray()) {
      writer.write(datum);
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public long getPos() throws IOException {
    return writer.getPos();
  }
}
