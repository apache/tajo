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
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.util.Bytes;

import java.nio.ByteBuffer;

public class RowStoreUtil {
  public static int[] getTargetIds(Schema inSchema, Schema outSchema) {
    int[] targetIds = new int[outSchema.getColumnNum()];
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

  public static class RowStoreDecoder {

    public static Tuple toTuple(Schema schema, byte [] bytes) {
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      Tuple tuple = new VTuple(schema.getColumnNum());
      Column col;
      TajoDataTypes.DataType type;
      for (int i =0; i < schema.getColumnNum(); i++) {
        col = schema.getColumn(i);
        type = col.getDataType();
        switch (type.getType()) {
          case BOOLEAN: tuple.put(i, DatumFactory.createBool(bb.get())); break;
          case BIT:
            byte b = bb.get();
            if(b == 0) {
              tuple.put(i, DatumFactory.createNullDatum());
            } else {
              tuple.put(i, DatumFactory.createBit(b));
            }
            break;

          case CHAR:
            byte c = bb.get();
            if(c == 0) {
              tuple.put(i, DatumFactory.createNullDatum());
            } else {
              tuple.put(i, DatumFactory.createChar(c));
            }
            break;

          case INT2:
            short s = bb.getShort();
            if(s < Short.MIN_VALUE + 1) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
              tuple.put(i, DatumFactory.createInt2(s));
            }
            break;

          case INT4:
          case DATE:
            int i_ = bb.getInt();
            if ( i_ < Integer.MIN_VALUE + 1) {
              tuple.put(i, DatumFactory.createNullDatum());
            } else {
              tuple.put(i, DatumFactory.createFromInt4(type, i_));
            }
            break;

          case INT8:
          case TIMESTAMP:
            long l = bb.getLong();
            if ( l < Long.MIN_VALUE + 1) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
              tuple.put(i, DatumFactory.createFromInt8(type, l));
            }
            break;

          case FLOAT4:
            float f = bb.getFloat();
            if (Float.isNaN(f)) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
              tuple.put(i, DatumFactory.createFloat4(f));
            }
            break;

          case FLOAT8:
            double d = bb.getDouble();
            if(Double.isNaN(d)) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
              tuple.put(i, DatumFactory.createFloat8(d));
            }
            break;

          case TEXT:
            byte [] _string = new byte[bb.getInt()];
            bb.get(_string);
            String str = new String(_string);
            if(str.compareTo("NULL") == 0) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
            tuple.put(i, DatumFactory.createText(str));
            }
            break;

          case BLOB:
            byte [] _bytes = new byte[bb.getInt()];
            bb.get(_bytes);
            if(Bytes.compareTo(bytes, Bytes.toBytes("NULL")) == 0) {
              tuple.put(i, DatumFactory.createNullDatum());
            } else {
              tuple.put(i, DatumFactory.createBlob(_bytes));
            }
            break;

          case INET4:
            byte [] _ipv4 = new byte[4];
            bb.get(_ipv4);
            tuple.put(i, DatumFactory.createInet4(_ipv4));
            break;
          case INET6:
            // TODO - to be implemented
        }
      }
      return tuple;
    }
  }

  public static class RowStoreEncoder {

    public static byte [] toBytes(Schema schema, Tuple tuple) {
      int size = StorageUtil.getRowByteSize(schema);
      ByteBuffer bb = ByteBuffer.allocate(size);
      Column col;
      for (int i = 0; i < schema.getColumnNum(); i++) {
        col = schema.getColumn(i);
        switch (col.getDataType().getType()) {
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
          case TIMESTAMP: bb.putLong(((TimestampDatum)tuple.get(i)).getMillis()); break;
          case BLOB:
            byte [] bytes = tuple.get(i).asByteArray();
            bb.putInt(bytes.length);
            bb.put(bytes);
            break;
          case INET4:
            byte [] ipBytes = tuple.getIPv4Bytes(i);
            bb.put(ipBytes);
            break;
          case INET6: bb.put(tuple.getIPv6Bytes(i)); break;
          default:
        }
      }

      bb.flip();
      byte [] buf = new byte [bb.limit()];
      bb.get(buf);
      return buf;
    }
  }
}
