/***
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

package org.apache.tajo.tuple.memory;

import io.netty.util.internal.PlatformDependent;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.tuple.RowBlockReader;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.Deallocatable;
import org.apache.tajo.util.SizeOf;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class MemoryRowBlock implements RowBlock, Deallocatable {
  public static final int NULL_FIELD_OFFSET = -1;

  private DataType[] dataTypes;

  // Basic States
  private int maxRowNum = Integer.MAX_VALUE; // optional
  private int rowNum;

  private RowWriter builder;
  private MemoryBlock memory;

  public MemoryRowBlock(DataType[] dataTypes, ResizableLimitSpec limitSpec, boolean isDirect) {
    this.memory = new ResizableMemoryBlock(limitSpec, isDirect);
    this.dataTypes = dataTypes;
  }

  public MemoryRowBlock(MemoryRowBlock rowBlock) {
    this.memory = TUtil.checkTypeAndGet(rowBlock.getMemory().duplicate(), ResizableMemoryBlock.class);
    this.rowNum = rowBlock.rowNum;
    this.dataTypes = rowBlock.dataTypes;
  }

  public MemoryRowBlock(MemoryBlock memory, DataType[] dataTypes, int rowNum) {
    this.memory = memory;
    this.rowNum = rowNum;
    this.dataTypes = dataTypes;
  }

  public MemoryRowBlock(DataType[] dataTypes) {
    this(dataTypes, new ResizableLimitSpec(64 * StorageUnit.KB), true);
  }

  public MemoryRowBlock(DataType[] dataTypes, int bytes) {
    this(dataTypes, new ResizableLimitSpec(bytes), true);
  }

  public MemoryRowBlock(DataType[] dataTypes, int bytes, boolean isDirect) {
    this(dataTypes, new ResizableLimitSpec(bytes), isDirect);
  }

  @Override
  public void clear() {
    reset();
    memory.clear();
  }

  private void reset() {
    rowNum = 0;
    if (builder != null) {
      builder.clear();
    }
  }

  @Override
  public int capacity() {
    return memory.capacity();
  }

  public int maxRowNum() {
    return maxRowNum;
  }

  @Override
  public int rows() {
    return rowNum;
  }

  @Override
  public void setRows(int rowNum) {
    this.rowNum = rowNum;
  }

  @Override
  public DataType[] getDataTypes() {
    return dataTypes;
  }

  @Override
  public boolean copyFromChannel(ScatteringByteChannel channel) throws IOException {
    reset();

    int readBytes = memory.writeBytes(channel);

    if (readBytes > 0) {
      // get row capacity in buffer
      while (memory.isReadable()) {
        if (memory.readableBytes() < SizeOf.SIZE_OF_INT) {
          return true;
        }

        int recordSize = PlatformDependent.getInt(memory.address() + memory.readerPosition());
        assert recordSize > 0;
        if (memory.readableBytes() < recordSize) {
          return true;
        } else {
          memory.readerPosition(memory.readerPosition() + recordSize);
        }

        rowNum++;
      }

      return true;
    } else {
      return false;
    }
  }

  @Override
  public RowWriter getWriter() {

    if (builder == null) {
      if (!getMemory().hasAddress()) {
        throw new TajoInternalError(new NotImplementedException("Heap memory writer not implemented yet"));
      } else {
        this.builder = new OffHeapRowBlockWriter(this);
      }
    }
    return builder;
  }

  @Override
  public MemoryBlock getMemory() {
    return memory;
  }

  @Override
  public void release() {
    memory.release();
  }

  @Override
  public RowBlockReader getReader() {
    if (!getMemory().hasAddress()) {
      return new HeapRowBlockReader(this);
    } else {
      return new OffHeapRowBlockReader(this);
    }
  }
}
