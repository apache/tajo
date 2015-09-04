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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.exception.ValueTooLongForTypeCharactersException;
import org.apache.tajo.util.BitArray;

import java.nio.ByteBuffer;

public class RowStoreUtil {
  public static int[] getTargetIds(Schema inSchema, Schema outSchema) {
    int[] targetIds = new int[outSchema.size()];
    int i = 0;
    for (Column target : outSchema.getRootColumns()) {
      targetIds[i] = inSchema.getColumnId(target.getQualifiedName());
      i++;
    }

    return targetIds;
  }

  public static Tuple project(Tuple in, Tuple out, int[] targetIds) {
    for (int idx = 0; idx < targetIds.length; idx++) {
      out.put(idx, in.asDatum(targetIds[idx]));
    }
    return out;
  }

  public static RowStoreEncoder createEncoder(Schema schema) {
    return new RowStoreEncoder(schema);
  }

  public static RowStoreDecoder createDecoder(Schema schema) {
    return new RowStoreDecoder(schema);
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


    public Tuple toTuple(byte[] bytes) {
      nullFlags.clear();
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      Tuple tuple = new VTuple(schema.size());
      Column col;
      TajoDataTypes.DataType type;

      bb.limit(headerSize);
      nullFlags.fromByteBuffer(bb);
      bb.limit(bytes.length);

      for (int i = 0; i < schema.size(); i++) {
        if (nullFlags.get(i)) {
          tuple.put(i, DatumFactory.createNullDatum());
          continue;
        }

        col = schema.getColumn(i);
        type = col.getDataType();
        switch (type.getType()) {
        case BOOLEAN:
          tuple.put(i, DatumFactory.createBool(bb.get()));
          break;
        case BIT:
          byte b = bb.get();
          tuple.put(i, DatumFactory.createBit(b));
          break;

        case CHAR:
          byte[] _str = new byte[type.getLength()];
          bb.get(_str);
          tuple.put(i, DatumFactory.createChar(_str));
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
          int month = bb.getInt();
          long milliseconds = bb.getLong();
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
          byte[] _string = new byte[bb.getInt()];
          bb.get(_string);
          tuple.put(i, DatumFactory.createText(_string));
          break;

        case BLOB:
          byte[] _bytes = new byte[bb.getInt()];
          bb.get(_bytes);
          tuple.put(i, DatumFactory.createBlob(_bytes));
          break;

        case INET4:
          byte[] _ipv4 = new byte[4];
          bb.get(_ipv4);
          tuple.put(i, DatumFactory.createInet4(_ipv4));
          break;

        default:
          throw new TajoRuntimeException(
              new UnsupportedException("data type '" + col.getDataType().getType().name() + "'"));
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

    public byte[] toBytes(Tuple tuple) {
      nullFlags.clear();
      int size = estimateTupleDataSize(tuple);
      ByteBuffer bb = ByteBuffer.allocate(size + headerSize);
      bb.position(headerSize);
      Column col;
      for (int i = 0; i < schema.size(); i++) {
        if (tuple.isBlankOrNull(i)) {
          nullFlags.set(i);
          continue;
        }

        col = schema.getColumn(i);
        switch (col.getDataType().getType()) {
          case NULL_TYPE:
            nullFlags.set(i);
            break;
          case BOOLEAN:
            bb.put(tuple.getByte(i));
            break;
          case BIT:
            bb.put(tuple.getByte(i));
            break;
          case CHAR:
            int charSize = col.getDataType().getLength();
            byte [] _char = new byte[charSize];
            byte [] src = tuple.getBytes(i);
            if (charSize < src.length) {
              throw new ValueTooLongForTypeCharactersException(charSize);
            }

            System.arraycopy(src, 0, _char, 0, src.length);
            bb.put(_char);
            break;
          case INT2:
            bb.putShort(tuple.getInt2(i));
            break;
          case INT4:
            bb.putInt(tuple.getInt4(i));
            break;
          case INT8:
            bb.putLong(tuple.getInt8(i));
            break;
          case FLOAT4:
            bb.putFloat(tuple.getFloat4(i));
            break;
          case FLOAT8:
            bb.putDouble(tuple.getFloat8(i));
            break;
          case TEXT:
            byte[] _string = tuple.getBytes(i);
            bb.putInt(_string.length);
            bb.put(_string);
            break;
          case DATE:
            bb.putInt(tuple.getInt4(i));
            break;
          case TIME:
          case TIMESTAMP:
            bb.putLong(tuple.getInt8(i));
            break;
          case INTERVAL:
            IntervalDatum interval = (IntervalDatum) tuple.getInterval(i);
            bb.putInt(interval.getMonths());
            bb.putLong(interval.getMilliSeconds());
            break;
          case BLOB:
            byte[] bytes = tuple.getBytes(i);
            bb.putInt(bytes.length);
            bb.put(bytes);
            break;
          case INET4:
            byte[] ipBytes = tuple.getBytes(i);
            bb.put(ipBytes);
            break;
          case INET6:
            bb.put(tuple.getBytes(i));
            break;
          default:
            throw new TajoRuntimeException(
                new UnsupportedException("data type '" + col.getDataType().getType().name() + "'"));
        }
      }

      byte[] flags = nullFlags.toArray();
      int finalPosition = bb.position();
      bb.position(0);
      bb.put(flags);

      bb.position(finalPosition);
      bb.flip();
      byte[] buf = new byte[bb.limit()];
      bb.get(buf);
      return buf;
    }

    // Note that, NULL values are treated separately
    private int estimateTupleDataSize(Tuple tuple) {
      int size = 0;
      Column col;

      for (int i = 0; i < schema.size(); i++) {
        if (tuple.isBlankOrNull(i)) {
          continue;
        }

        col = schema.getColumn(i);
        switch (col.getDataType().getType()) {
          case BOOLEAN:
          case BIT:
            size += 1;
            break;
          case CHAR:
            size += col.getDataType().getLength();
            break;
          case INT2:
            size += 2;
            break;
          case DATE:
          case INT4:
          case FLOAT4:
            size += 4;
            break;
          case TIME:
          case TIMESTAMP:
          case INT8:
          case FLOAT8:
            size += 8;
            break;
          case INTERVAL:
            size += 12;
            break;
          case TEXT:
          case BLOB:
            size += (4 + tuple.getBytes(i).length);
            break;
          case INET4:
          case INET6:
            size += tuple.getBytes(i).length;
            break;
          default:
            throw new TajoRuntimeException(
                new UnsupportedException("data type '" + col.getDataType().getType().name() + "'"));
        }
      }

      size += 100; // optimistic reservation

      return size;
    }

    public Schema getSchema() {
      return schema;
    }
  }
}
