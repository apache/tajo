/*
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

package tajo.storage;

import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.datum.DatumFactory;
import tajo.util.Bytes;

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
      for (int i =0; i < schema.getColumnNum(); i++) {
        col = schema.getColumn(i);

        switch (col.getDataType()) {
          case BYTE:
            byte b = bb.get();
            if(b == 0) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
              tuple.put(i, DatumFactory.createByte(b));
            }break;
          case CHAR:
            byte c = bb.get();
            if(c == 0) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
              tuple.put(i, DatumFactory.createChar(c));
            }break;
          case BOOLEAN: tuple.put(i, DatumFactory.createBool(bb.get())); break;
          case SHORT:
            short s = bb.getShort();
            if(s < Short.MIN_VALUE + 1) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
              tuple.put(i, DatumFactory.createShort(s));
            }break;
          case INT:
            int i_ = bb.getInt();
            if ( i_ < Integer.MIN_VALUE + 1) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
            tuple.put(i, DatumFactory.createInt(i_));
            }break;
          case LONG:
            long l = bb.getLong();
            if ( l < Long.MIN_VALUE + 1) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
              tuple.put(i, DatumFactory.createLong(l));
            }break;
          case FLOAT:
            float f = bb.getFloat();
            if (Float.isNaN(f)) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
              tuple.put(i, DatumFactory.createFloat(f));
            }break;
          case DOUBLE:
            double d = bb.getDouble();
            if(Double.isNaN(d)) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
              tuple.put(i, DatumFactory.createDouble(d));
            }break;
          case STRING:
            byte [] _string = new byte[bb.getInt()];
            bb.get(_string);
            String str = new String(_string);
            if(str.compareTo("NULL") == 0) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
            tuple.put(i, DatumFactory.createString(str));
            }break;
          case BYTES:
            byte [] _bytes = new byte[bb.getInt()];
            bb.get(_bytes);
            if(Bytes.compareTo(bytes, Bytes.toBytes("NULL")) == 0) {
              tuple.put(i, DatumFactory.createNullDatum());
            }else {
            tuple.put(i, DatumFactory.createBytes(_bytes));
            }break;
          case IPv4:
            byte [] _ipv4 = new byte[4];
            bb.get(_ipv4);
            tuple.put(i, DatumFactory.createIPv4(_ipv4));
            break;
          case IPv6:
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
        switch (col.getDataType()) {
          case BYTE: bb.put(tuple.get(i).asByte()); break;
          case CHAR: bb.put(tuple.get(i).asByte()); break;
          case BOOLEAN: bb.put(tuple.get(i).asByte()); break;
          case SHORT: bb.putShort(tuple.get(i).asShort()); break;
          case INT: bb.putInt(tuple.get(i).asInt()); break;
          case LONG: bb.putLong(tuple.get(i).asLong()); break;
          case FLOAT: bb.putFloat(tuple.get(i).asFloat()); break;
          case DOUBLE: bb.putDouble(tuple.get(i).asDouble()); break;
          case STRING:
            byte [] _string = tuple.get(i).asByteArray();
            bb.putInt(_string.length);
            bb.put(_string);
            break;
          case BYTES:
            byte [] bytes = tuple.get(i).asByteArray();
            bb.putInt(bytes.length);
            bb.put(bytes);
            break;
          case IPv4:
            byte [] ipBytes = tuple.getIPv4Bytes(i);
            bb.put(ipBytes);
            break;
          case IPv6: bb.put(tuple.getIPv6Bytes(i)); break;
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
