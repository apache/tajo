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

import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.storage.hcfile.reader.Reader;
import tajo.storage.hcfile.reader.TypeReader;
import tajo.storage.exception.UnknownDataTypeException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * BlockReader reads data from the disk
 * and fills a block.
 */
public class BlockReader implements Reader {
  private TypeReader reader;

  public BlockReader(DataType type)
      throws IOException, UnknownDataTypeException {
    reader = TypeReader.get(type);
  }

  @Override
  public UpdatableBlock read(BlockMeta meta, ByteBuffer buffer) throws IOException {
    UpdatableBlock block = new BasicBlock(meta);
    Datum datum;
    while ((datum=reader.read(buffer)) != null) {
      block.appendValue(datum);
    }
    block.getMeta().setRecordNum(block.getDataNum());
    return block;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
