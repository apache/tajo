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

package org.apache.tajo.tuple.offheap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.util.Deallocatable;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.SizeOf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class OffHeapRowBlock extends OffHeapMemory implements Deallocatable {
  private static final Log LOG = LogFactory.getLog(OffHeapRowBlock.class);

  public static final int NULL_FIELD_OFFSET = -1;

  DataType [] dataTypes;

  // Basic States
  private int maxRowNum = Integer.MAX_VALUE; // optional
  private int rowNum;
  protected int position = 0;

  private OffHeapRowBlockWriter builder;

  private OffHeapRowBlock(ByteBuffer buffer, Schema schema, ResizableLimitSpec limitSpec) {
    super(buffer, limitSpec);
    initialize(schema);
  }

  public OffHeapRowBlock(Schema schema, ResizableLimitSpec limitSpec) {
    super(limitSpec);
    initialize(schema);
  }

  private void initialize(Schema schema) {
    dataTypes = SchemaUtil.toDataTypes(schema);

    this.builder = new OffHeapRowBlockWriter(this);
  }

  @VisibleForTesting
  public OffHeapRowBlock(Schema schema, int bytes) {
    this(schema, new ResizableLimitSpec(bytes));
  }

  @VisibleForTesting
  public OffHeapRowBlock(Schema schema, ByteBuffer buffer) {
    this(buffer, schema, ResizableLimitSpec.DEFAULT_LIMIT);
  }

  public void position(int pos) {
    this.position = pos;
  }

  public void clear() {
    this.position = 0;
    this.rowNum = 0;

    builder.clear();
  }

  @Override
  public ByteBuffer nioBuffer() {
    return (ByteBuffer) buffer.position(0).limit(position);
  }

  public int position() {
    return position;
  }

  public long usedMem() {
    return position;
  }

  /**
   * Ensure that this buffer has enough remaining space to add the size.
   * Creates and copies to a new buffer if necessary
   *
   * @param size Size to add
   */
  public void ensureSize(int size) {
    if (remain() - size < 0) {
      if (!limitSpec.canIncrease(memorySize)) {
        throw new RuntimeException("Cannot increase RowBlock anymore.");
      }

      int newBlockSize = limitSpec.increasedSize(memorySize);
      resize(newBlockSize);
      LOG.info("Increase DirectRowBlock to " + FileUtil.humanReadableByteCount(newBlockSize, false));
    }
  }

  public long remain() {
    return memorySize - position - builder.offset();
  }

  public int maxRowNum() {
    return maxRowNum;
  }
  public int rows() {
    return rowNum;
  }

  public void setRows(int rowNum) {
    this.rowNum = rowNum;
  }

  public boolean copyFromChannel(FileChannel channel, TableStats stats) throws IOException {
    if (channel.position() < channel.size()) {
      clear();

      buffer.clear();
      channel.read(buffer);
      memorySize = buffer.position();

      while (position < memorySize) {
        long recordPtr = address + position;

        if (remain() < SizeOf.SIZE_OF_INT) {
          channel.position(channel.position() - remain());
          memorySize = (int) (memorySize - remain());
          return true;
        }

        int recordSize = UNSAFE.getInt(recordPtr);

        if (remain() < recordSize) {
          channel.position(channel.position() - remain());
          memorySize = (int) (memorySize - remain());
          return true;
        }

        position += recordSize;
        rowNum++;
      }

      return true;
    } else {
      return false;
    }
  }

  public RowWriter getWriter() {
    return builder;
  }

  public OffHeapRowBlockReader getReader() {
    return new OffHeapRowBlockReader(this);
  }
}
