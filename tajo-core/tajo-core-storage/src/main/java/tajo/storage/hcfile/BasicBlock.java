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

import com.google.common.collect.Lists;
import tajo.datum.Datum;

import java.io.IOException;
import java.util.List;

public class BasicBlock extends UpdatableSeekableBlock {
  private final int MAX_BLOCK_SIZE = 65535; // 64KB
  private BlockMeta meta;
  private List<Datum> datums = Lists.newArrayList();
  private int pos;
  private int next;
  int size;

  public BasicBlock() {
    size = 0;
  }

  public BasicBlock(BlockMeta meta) {
    this.setMeta(meta);
  }

  public void setMeta(BlockMeta meta) {
    this.meta = meta;
  }

  @Override
  public BlockMeta getMeta() throws IOException {
    return meta;
  }

  @Override
  public Datum[] asArray() throws IOException {
    return datums.toArray(new Datum[datums.size()]);
  }

  @Override
  public Datum next() throws IOException {
    if (next >= datums.size()) {
      return null;
    }
    pos = next;
    Datum datum = datums.get(next++);
    return datum;
  }

  @Override
  public int getPos() throws IOException {
    return pos;
  }

  @Override
  public int getSize() throws IOException {
    return this.size;
  }

  @Override
  public int getDataNum() throws IOException {
    return datums.size();
  }

  @Override
  public void first() throws IOException {
    pos = 0;
    next = 0;
  }

  @Override
  public void last() throws IOException {
    pos = datums.size() - 1;
    next = datums.size() - 1;
  }

  @Override
  public void pos(long pos) throws IOException {
    this.pos = (int)pos;
    this.next = this.pos;
  }

  @Override
  public void appendValue(Datum data) throws IOException {
    this.size += ColumnStoreUtil.getWrittenSize(data);
    datums.add(data);
  }

  public boolean isAppendable(Datum datum) throws IOException {
    return Integer.SIZE/8 // block size
        + this.size
        + ColumnStoreUtil.getWrittenSize(datum)
        < MAX_BLOCK_SIZE;
  }

  @Override
  public void setValues(Datum[] data) throws IOException {
    if (datums == null) {
      datums = Lists.newArrayList();
    } else {
      this.clear();
    }
    for (Datum d : data) {
      this.appendValue(d);
    }
  }

  @Override
  public Datum removeValue() throws IOException {
    this.size -= ColumnStoreUtil.getWrittenSize(datums.get(pos));
    return datums.remove(pos);
  }

  @Override
  public void clear() throws IOException {
    datums.clear();
    pos = 0;
    next = 0;
    meta.setRecordNum(0);
    meta.setStartRid(0);
    size = 0;
  }
}
