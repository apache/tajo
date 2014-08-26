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

package org.apache.tajo.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.directmem.SizeOf;
import org.apache.tajo.storage.directmem.UnsafeUtil;
import org.apache.tajo.storage.exception.UnknownDataTypeException;
import org.apache.tajo.storage.rawfile.DirectRawFileScanner;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.BitArray;
import org.apache.tajo.util.FileUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.tajo.common.TajoDataTypes.Type;

public class RowStoreUtil {
  public static int[] getTargetIds(Schema inSchema, Schema outSchema) {
    int[] targetIds = new int[outSchema.size()];
    int i = 0;
    for (Column target : outSchema.getColumns()) {
      targetIds[i] = inSchema.getColumnId(target.getQualifiedName());
      i++;
    }

    return targetIds;
  }

  public static Tuple project(Tuple in, Tuple out, int[] targetIds) {
    out.clear();
    for (int idx = 0; idx < targetIds.length; idx++) {
      out.put(idx, in.get(targetIds[idx]));
    }
    return out;
  }

  public static RowStoreEncoder createEncoder(Schema schema) {
    return new RowStoreEncoder(schema);
  }

  public static RowStoreDecoder createDecoder(Schema schema) {
    return new RowStoreDecoder(schema);
  }

  public static DirectRowStoreEncoder createDirectRawEncoder(Schema schema) {
    return new DirectRowStoreEncoder(schema);
  }

  public static class RowStoreDecoder {

    private Schema schema;
    private BitArray nullFlags;
    private int headerSize;

    private RowStoreDecoder(Schema schema) {
      this.schema = schema;
      nullFlags = new BitArray(schema.size());
      headerSize = nullFlags.bytesLength();
    }


    public Tuple toTuple(byte [] bytes) {
      nullFlags.clear();
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      Tuple tuple = new VTuple(schema.size());
      Column col;
      TajoDataTypes.DataType type;

      bb.limit(headerSize);
      nullFlags.fromByteBuffer(bb);
      bb.limit(bytes.length);

      for (int i =0; i < schema.size(); i++) {
        if (nullFlags.get(i)) {
          tuple.put(i, DatumFactory.createNullDatum());
          continue;
        }

        col = schema.getColumn(i);
        type = col.getDataType();
        switch (type.getType()) {
          case BOOLEAN: tuple.put(i, DatumFactory.createBool(bb.get())); break;
          case BIT:
            byte b = bb.get();
            tuple.put(i, DatumFactory.createBit(b));
            break;

          case CHAR:
            byte c = bb.get();
            tuple.put(i, DatumFactory.createChar(c));
            break;

          case INT2:
            short s = bb.getShort();
            tuple.put(i, DatumFactory.createInt2(s));
            break;

          case INT4:
          case DATE:
            int i_ = bb.getInt();
            tuple.put(i, DatumFactory.createFromInt4(type, i_));
            break;

          case INT8:
          case TIME:
          case TIMESTAMP:
            long l = bb.getLong();
            tuple.put(i, DatumFactory.createFromInt8(type, l));
            break;

        case INTERVAL:
            int month  = bb.getInt();
            long milliseconds  = bb.getLong();
            tuple.put(i, new IntervalDatum(month, milliseconds));
            break;

          case FLOAT4:
            float f = bb.getFloat();
            tuple.put(i, DatumFactory.createFloat4(f));
            break;

          case FLOAT8:
            double d = bb.getDouble();
            tuple.put(i, DatumFactory.createFloat8(d));
            break;

          case TEXT:
            byte [] _string = new byte[bb.getInt()];
            bb.get(_string);
            tuple.put(i, DatumFactory.createText(_string));
            break;

          case BLOB:
            byte [] _bytes = new byte[bb.getInt()];
            bb.get(_bytes);
            tuple.put(i, DatumFactory.createBlob(_bytes));
            break;

          case INET4:
            byte [] _ipv4 = new byte[4];
            bb.get(_ipv4);
            tuple.put(i, DatumFactory.createInet4(_ipv4));
            break;
          case INET6:
            // TODO - to be implemented
            throw new UnsupportedException(type.getType().name());
          default:
            throw new RuntimeException(new UnknownDataTypeException(type.getType().name()));
        }
      }
      return tuple;
    }

    public Schema getSchema() {
      return schema;
    }
  }

  public static class RowStoreEncoder {
    private Schema schema;
    private BitArray nullFlags;
    private int headerSize;

    private RowStoreEncoder(Schema schema) {
      this.schema = schema;
      nullFlags = new BitArray(schema.size());
      headerSize = nullFlags.bytesLength();
    }
    public byte [] toBytes(Tuple tuple) {
      nullFlags.clear();
      int size = estimateTupleDataSize(tuple);
      ByteBuffer bb = ByteBuffer.allocate(size + headerSize);
      bb.position(headerSize);
      Column col;
      for (int i = 0; i < schema.size(); i++) {
        if (tuple.isNull(i)) {
          nullFlags.set(i);
          continue;
        }

        col = schema.getColumn(i);
        switch (col.getDataType().getType()) {
          case NULL_TYPE: nullFlags.set(i); break;
          case BOOLEAN: bb.put(tuple.get(i).asByte()); break;
          case BIT: bb.put(tuple.get(i).asByte()); break;
          case CHAR: bb.put(tuple.get(i).asByte()); break;
          case INT2: bb.putShort(tuple.get(i).asInt2()); break;
          case INT4: bb.putInt(tuple.get(i).asInt4()); break;
          case INT8: bb.putLong(tuple.get(i).asInt8()); break;
          case FLOAT4: bb.putFloat(tuple.get(i).asFloat4()); break;
          case FLOAT8: bb.putDouble(tuple.get(i).asFloat8()); break;
          case TEXT:
            byte [] _string = tuple.get(i).asByteArray();
            bb.putInt(_string.length);
            bb.put(_string);
            break;
          case DATE: bb.putInt(tuple.get(i).asInt4()); break;
          case TIME:
          case TIMESTAMP:
            bb.putLong(tuple.get(i).asInt8());
            break;
          case INTERVAL:
            IntervalDatum interval = (IntervalDatum) tuple.get(i);
            bb.putInt(interval.getMonths());
            bb.putLong(interval.getMilliSeconds());
            break;
          case BLOB:
            byte [] bytes = tuple.get(i).asByteArray();
            bb.putInt(bytes.length);
            bb.put(bytes);
            break;
          case INET4:
            byte [] ipBytes = tuple.get(i).asByteArray();
            bb.put(ipBytes);
            break;
          case INET6: bb.put(tuple.get(i).asByteArray()); break;
          default:
            throw new RuntimeException(new UnknownDataTypeException(col.getDataType().getType().name()));
        }
      }

      byte[] flags = nullFlags.toArray();
      int finalPosition = bb.position();
      bb.position(0);
      bb.put(flags);

      bb.position(finalPosition);
      bb.flip();
      byte [] buf = new byte [bb.limit()];
      bb.get(buf);
      return buf;
    }

    // Note that, NULL values are treated separately
    private int estimateTupleDataSize(Tuple tuple) {
      int size = 0;
      Column col;

      for (int i = 0; i < schema.size(); i++) {
        if (tuple.isNull(i)) {
          continue;
        }

        col = schema.getColumn(i);
        switch (col.getDataType().getType()) {
          case BOOLEAN:
          case BIT:
          case CHAR: size += 1; break;
          case INT2: size += 2; break;
          case DATE:
          case INT4:
          case FLOAT4: size += 4; break;
          case TIME:
          case TIMESTAMP:
          case INT8:
          case FLOAT8: size += 8; break;
          case INTERVAL: size += 12; break;
          case TEXT:
          case BLOB: size += (4 + tuple.get(i).asByteArray().length); break;
          case INET4:
          case INET6: size += tuple.get(i).asByteArray().length; break;
          default:
            throw new RuntimeException(new UnknownDataTypeException(col.getDataType().getType().name()));
        }
      }

      size += 100; // optimistic reservation

      return size;
    }

    public Schema getSchema() {
      return schema;
    }
  }

  public static class DirectRowStoreEncoder {
    private static final Log LOG = LogFactory.getLog(DirectRowStoreEncoder.class);

    private static final Unsafe UNSAFE = UnsafeUtil.unsafe;
    private Type [] types;
    private ByteBuffer buffer;
    private long address;
    private int rowOffset;

    public DirectRowStoreEncoder(Schema schema) {
      this.types = SchemaUtil.toTypes(schema);
      buffer = ByteBuffer.allocateDirect(64 * StorageUnit.KB).order(ByteOrder.nativeOrder());
      address = UnsafeUtil.getAddress(buffer);
    }

    private void ensureSize(int size) {

      if (buffer.remaining() - size < 0) { // check the remain size
        // enlarge new buffer and copy writing data
        int newBlockSize = UnsafeUtil.alignedSize(buffer.capacity() << 1);
        ByteBuffer newByteBuf = ByteBuffer.allocateDirect(newBlockSize);
        long newAddress = ((DirectBuffer)newByteBuf).address();
        UNSAFE.copyMemory(this.address, newAddress, buffer.limit());
        LOG.debug("Increase DirectRowBlock to " + FileUtil.humanReadableByteCount(newBlockSize, false));

        // release existing buffer and replace variables
        UnsafeUtil.free(buffer);
        buffer = newByteBuf;
        address = newAddress;
      }
    }

    public ByteBuffer encode(Tuple tuple) {
      for (int i = 0; i < types.length; i++) {
        switch (types[i]) {
        case BOOLEAN:
          ensureSize(SizeOf.SIZE_OF_BYTE);
          UNSAFE.putByte(address + rowOffset, (byte) (tuple.getBool(i) ? 0x01 : 0x00));
          rowOffset += SizeOf.SIZE_OF_BYTE;
          break;
        case INT1:
        case INT2:
          ensureSize(SizeOf.SIZE_OF_SHORT);
          UNSAFE.putShort(address + rowOffset, tuple.getInt2(i));
          rowOffset += SizeOf.SIZE_OF_SHORT;
          break;

        case INT4:
        case DATE:
        case INET4:
          ensureSize(SizeOf.SIZE_OF_INT);
          UNSAFE.putInt(address + rowOffset, tuple.getInt4(i));
          rowOffset += SizeOf.SIZE_OF_INT;
          break;

        case INT8:
        case TIMESTAMP:
        case TIME:
          ensureSize(SizeOf.SIZE_OF_LONG);
          UNSAFE.putLong(address + rowOffset, tuple.getInt8(i));
          rowOffset += SizeOf.SIZE_OF_LONG;
          break;

        case FLOAT4:
          ensureSize(SizeOf.SIZE_OF_FLOAT);
          UNSAFE.putFloat(address + rowOffset, tuple.getFloat4(i));
          rowOffset += SizeOf.SIZE_OF_FLOAT;
          break;
        case FLOAT8:
          ensureSize(SizeOf.SIZE_OF_DOUBLE);
          UNSAFE.putDouble(address + rowOffset, tuple.getFloat8(i));
          rowOffset += SizeOf.SIZE_OF_DOUBLE;
          break;

        case TEXT:
        case PROTOBUF:
        case BLOB:
          byte [] bytes = tuple.getBytes(i);

          ensureSize(SizeOf.SIZE_OF_INT + bytes.length);

          UNSAFE.putInt(address + rowOffset, bytes.length);
          rowOffset += SizeOf.SIZE_OF_INT;

          UNSAFE.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, address + rowOffset, bytes.length);
          rowOffset += bytes.length;
          break;

        case INTERVAL:
          ensureSize(SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG);

          IntervalDatum interval = tuple.getInterval(i);

          UNSAFE.putInt(address + rowOffset, interval.getMonths());
          rowOffset += SizeOf.SIZE_OF_INT;

          UNSAFE.putLong(address + rowOffset, interval.getMilliSeconds());
          rowOffset += SizeOf.SIZE_OF_LONG;
          break;

        default:
          throw new UnsupportedException("Unknown data type: " + types[i]);
        }
      }

      buffer.position(0).limit(rowOffset);
      return buffer;
    }
  }
}
