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
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.annotation.NotThreadSafe;
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

@NotThreadSafe
public class MemoryRowBlock implements RowBlock, Deallocatable {
  public static final int NULL_FIELD_OFFSET = -1;

  private final DataType[] dataTypes;
  private final String dataFormat;

  // Basic States
  private int maxRowNum = Integer.MAX_VALUE; // optional
  private int rowNum;

  private RowWriter builder;
  private MemoryBlock memory;

  public MemoryRowBlock(DataType[] dataTypes, ResizableLimitSpec limitSpec, boolean isDirect) {
    this(dataTypes, limitSpec, isDirect, BuiltinStorages.DRAW);
  }

  public MemoryRowBlock(DataType[] dataTypes, ResizableLimitSpec limitSpec, boolean isDirect, String dataFormat) {
    this.memory = new ResizableMemoryBlock(limitSpec, isDirect);
    this.dataTypes = dataTypes;
    this.dataFormat = dataFormat;
  }

  public MemoryRowBlock(MemoryRowBlock rowBlock) {
    this.memory = TUtil.checkTypeAndGet(rowBlock.getMemory().duplicate(), ResizableMemoryBlock.class);
    this.rowNum = rowBlock.rowNum;
    this.dataTypes = rowBlock.dataTypes;
    this.dataFormat = rowBlock.dataFormat;
  }

  public MemoryRowBlock(DataType[] dataTypes) {
    this(dataTypes, new ResizableLimitSpec(64 * StorageUnit.KB), true);
  }

  public MemoryRowBlock(DataType[] dataTypes, int bytes) {
    this(dataTypes, new ResizableLimitSpec(bytes), true);
  }

  public MemoryRowBlock(DataType[] dataTypes, int bytes, boolean isDirect, String dataFormat) {
    this(dataTypes, new ResizableLimitSpec(bytes), isDirect, dataFormat);
  }

  @Override
  public String getDataFormat() {
    return dataFormat;
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

  @Override
  public int usedMem() {
    return memory.writerPosition();
  }

  @Override
  public float usage() {
    if (usedMem() > 0) {
      return (usedMem() / (float) capacity());
    } else {
      return 0.0f;
    }
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
    switch (dataFormat) {
    case BuiltinStorages.DRAW:
      return fillDrawBuffer(channel);
    default:
      throw new TajoInternalError(new NotImplementedException("Heap memory writer not implemented yet"));
    }
  }

  protected boolean fillDrawBuffer(ScatteringByteChannel channel) throws IOException {
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

    if (!getMemory().hasAddress()) {
      throw new TajoInternalError(new NotImplementedException("Heap memory writer not implemented yet"));
    }

    if (builder == null) {
      switch (dataFormat) {
      case BuiltinStorages.DRAW:
        this.builder = new OffHeapRowBlockWriter(this);
        break;
      case BuiltinStorages.RAW:
        this.builder = new CompactRowBlockWriter(this);
        break;
      default:
        throw new TajoInternalError(new NotImplementedException(dataFormat + " memory writer not implemented yet"));
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

    switch (dataFormat) {
    case BuiltinStorages.DRAW: {
      if (!getMemory().hasAddress()) {
        return new HeapRowBlockReader(this);
      } else {
        return new OffHeapRowBlockReader(this);
      }
    }
    default:
      throw new TajoInternalError(new NotImplementedException(dataFormat + " memory writer not implemented yet"));
    }
  }
}
