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

package org.apache.tajo.storage.offheap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.Deallocatable;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.SizeOf;
import sun.misc.Unsafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class OffHeapRowBlock extends OffHeapMemory implements Deallocatable {
  private static final Log LOG = LogFactory.getLog(OffHeapRowBlock.class);

  public static final int NULL_FIELD_OFFSET = -1;

  DataType [] dataTypes;
  int fieldIndexBytesLen;

  // Basic States
  private int maxRowNum = Integer.MAX_VALUE; // optional
  private int rowNum;
  protected int position = 0;

  // Write States --------------------
  long rowStartAddrForWrite;

  private TupleBuilder builder;

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
    fieldIndexBytesLen = SizeOf.SIZE_OF_INT * schema.size();

    this.builder = new TupleBuilder();
  }

  @VisibleForTesting
  public OffHeapRowBlock(Schema schema, int bytes) {
    this(schema, new ResizableLimitSpec(bytes));
  }

  @VisibleForTesting
  public OffHeapRowBlock(Schema schema, ByteBuffer buffer) {
    this(buffer, schema, ResizableLimitSpec.DEFAULT_LIMIT);
  }

  public void clear() {
    this.position = 0;
    this.rowNum = 0;

    builder.clear();
  }

  public ByteBuffer nioBuffer() {
    buffer.flip();
    buffer.limit(position);
    return buffer;
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

      // compute the relative position of current row start offset
      long relativeRowStartPos = this.rowStartAddrForWrite - this.address;

      int newBlockSize = limitSpec.increasedSize(memorySize);
      resize(newBlockSize);
      LOG.info("Increase DirectRowBlock to " + FileUtil.humanReadableByteCount(newBlockSize, false));

      // Update current write states
      rowStartAddrForWrite = address() + relativeRowStartPos;
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

  public boolean copyFromChannel(FileChannel channel, TableStats stats) throws IOException {
    if (channel.position() < channel.size()) {
      clear();

      buffer.clear();
      channel.read(buffer);
      memorySize = buffer.position();

      while (position < memorySize) {
        rowStartAddrForWrite = address + position;

        if (remain() < SizeOf.SIZE_OF_INT) {
          channel.position(channel.position() - remain());
          memorySize = (int) (memorySize - remain());
          return true;
        }

        int recordSize = UNSAFE.getInt(rowStartAddrForWrite);

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

  public TupleBuilder getWriter() {
    return builder;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // RowBlockWriter Implementation
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // <Row Record Structure>
  //
  // | row length (4 bytes) | field 1 offset | field 2 offset | ... | field N offset| field 1 | field 2| ... | field N |
  //                               4 bytes          4 bytes               4 bytes

  public class TupleBuilder {
    private int [] fieldIndexesForWrite;
    private int curFieldIdxForWrite;
    private int rowOffsetForWrite;
    TupleBuilder() {
      fieldIndexesForWrite = new int[dataTypes.length];
    }

    void clear() {
      rowOffsetForWrite = 0;
      curFieldIdxForWrite = 0;
    }

    public int offset() {
      return rowOffsetForWrite;
    }

    public void addTuple(Tuple tuple) {
      startRow();

      for (int i = 0; i < dataTypes.length; i++) {
        if (tuple.isNull(i)) {
          skipField();
          continue;
        }
        switch (dataTypes[i].getType()) {
        case BOOLEAN:
          putBool(tuple.getBool(i));
          break;
        case INT1:
        case INT2:
          putInt2(tuple.getInt2(i));
          break;
        case INT4:
        case DATE:
        case INET4:
          putInt4(tuple.getInt4(i));
          break;
        case INT8:
        case TIMESTAMP:
        case TIME:
          putInt8(tuple.getInt8(i));
          break;
        case FLOAT4:
          putFloat4(tuple.getFloat4(i));
          break;
        case FLOAT8:
          putFloat8(tuple.getFloat8(i));
          break;
        case TEXT:
          putText(tuple.getBytes(i));
          break;
        case INTERVAL:
          putInterval((IntervalDatum) tuple.getInterval(i));
          break;
        case PROTOBUF:
          putProtoDatum((ProtobufDatum) tuple.getProtobufDatum(i));
          break;
        default:
          throw new UnsupportedException("Unknown data type: " + dataTypes[i]);
        }
      }

      endRow();
    }

    public boolean startRow() {
      rowStartAddrForWrite = address + position;
      rowOffsetForWrite = 0;
      rowOffsetForWrite += 4; // skip row header
      rowOffsetForWrite += fieldIndexBytesLen; // skip an array of field indices
      curFieldIdxForWrite = 0;
      return true;
    }

    public void endRow() {
      rowNum++;

      long rowHeaderPos = rowStartAddrForWrite;
      UNSAFE.putInt(rowHeaderPos, rowOffsetForWrite);
      rowHeaderPos += SizeOf.SIZE_OF_INT;

      for (int i = 0; i < curFieldIdxForWrite; i++) {
        UNSAFE.putInt(rowHeaderPos, fieldIndexesForWrite[i]);
        rowHeaderPos += SizeOf.SIZE_OF_INT;
      }
      for (int i = curFieldIdxForWrite; i < dataTypes.length; i++) {
        UNSAFE.putInt(rowHeaderPos, NULL_FIELD_OFFSET);
        rowHeaderPos += SizeOf.SIZE_OF_INT;
      }

      // rowOffset is equivalent to a byte length of this row.
      position += rowOffsetForWrite;
    }

    public void skipField() {
      fieldIndexesForWrite[curFieldIdxForWrite++] = NULL_FIELD_OFFSET;
    }

    public void putBool(boolean val) {
      ensureSize(SizeOf.SIZE_OF_BOOL);

      fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;

      UNSAFE.putByte(rowStartAddrForWrite + rowOffsetForWrite, (byte) (val ? 0x01 : 0x00));

      rowOffsetForWrite += SizeOf.SIZE_OF_BOOL;

      curFieldIdxForWrite++;
    }

    public void putInt2(short val) {
      ensureSize(SizeOf.SIZE_OF_SHORT);

      fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;

      UNSAFE.putShort(rowStartAddrForWrite + rowOffsetForWrite, val);
      rowOffsetForWrite += SizeOf.SIZE_OF_SHORT;

      curFieldIdxForWrite++;
    }

    public void putInt4(int val) {
      ensureSize(SizeOf.SIZE_OF_INT);

      fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;

      UNSAFE.putInt(rowStartAddrForWrite + rowOffsetForWrite, val);
      rowOffsetForWrite += SizeOf.SIZE_OF_INT;

      curFieldIdxForWrite++;
    }

    public void putInt8(long val) {
      ensureSize(SizeOf.SIZE_OF_LONG);

      fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;
      UNSAFE.putLong(rowStartAddrForWrite + rowOffsetForWrite, val);
      rowOffsetForWrite += SizeOf.SIZE_OF_LONG;

      curFieldIdxForWrite++;
    }

    public void putFloat4(float val) {
      ensureSize(SizeOf.SIZE_OF_FLOAT);

      fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;
      UNSAFE.putFloat(rowStartAddrForWrite + rowOffsetForWrite, val);
      rowOffsetForWrite += SizeOf.SIZE_OF_FLOAT;

      curFieldIdxForWrite++;
    }

    public void putFloat8(double val) {
      ensureSize(SizeOf.SIZE_OF_DOUBLE);

      fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;
      UNSAFE.putDouble(rowStartAddrForWrite + rowOffsetForWrite, val);
      rowOffsetForWrite += SizeOf.SIZE_OF_DOUBLE;

      curFieldIdxForWrite++;
    }

    public void putText(String val) {
      byte[] bytes = val.getBytes(TextDatum.DEFAULT_CHARSET);
      putText(bytes);
    }

    public void putText(byte[] val) {
      int bytesLen = val.length;

      ensureSize(SizeOf.SIZE_OF_INT + bytesLen);

      fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;
      UNSAFE.putInt(rowStartAddrForWrite + rowOffsetForWrite, bytesLen);
      rowOffsetForWrite += SizeOf.SIZE_OF_INT;

      UNSAFE.copyMemory(val, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, rowStartAddrForWrite + rowOffsetForWrite, bytesLen);
      rowOffsetForWrite += bytesLen;

      curFieldIdxForWrite++;
    }

    public void putBlob(byte[] val) {
      int bytesLen = val.length;

      ensureSize(SizeOf.SIZE_OF_INT + bytesLen);

      fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;
      UNSAFE.putInt(rowStartAddrForWrite + rowOffsetForWrite, bytesLen);
      rowOffsetForWrite += SizeOf.SIZE_OF_INT;

      UNSAFE.copyMemory(val, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, rowStartAddrForWrite + rowOffsetForWrite, bytesLen);
      rowOffsetForWrite += bytesLen;

      curFieldIdxForWrite++;
    }

    public void putTimestamp(long val) {
      putInt8(val);
    }

    public void putDate(int val) {
      putInt4(val);
    }

    public void putTime(long val) {
      putInt8(val);
    }

    public void putInterval(IntervalDatum val) {
      ensureSize(SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG);
      fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;

      long offset = rowStartAddrForWrite + rowOffsetForWrite;
      UNSAFE.putInt(offset, val.getMonths());
      offset += SizeOf.SIZE_OF_INT;
      UNSAFE.putLong(offset, val.getMilliSeconds());
      rowOffsetForWrite += SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG;

      curFieldIdxForWrite++;
    }

    public void putInet4(int val) {
      putInt4(val);
    }

    public void putProtoDatum(ProtobufDatum val) {
      putBlob(val.asByteArray());
    }
  }
}
