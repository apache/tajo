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

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.*;
import org.apache.tajo.util.Bytes;

import java.io.IOException;
import java.io.OutputStream;

public class BinarySerializerDeserializer implements SerializerDeserializer {

  static final byte[] INVALID_UTF__SINGLE_BYTE = {(byte) Integer.parseInt("10111111", 2)};

  @Override
  public int serialize(Column col, Datum datum, OutputStream out, byte[] nullCharacters)
      throws IOException {
    byte[] bytes;
    int length = 0;
    if (datum == null || datum instanceof NullDatum) {
      return 0;
    }

    switch (col.getDataType().getType()) {
      case BOOLEAN:
      case BIT:
      case CHAR:
        bytes = datum.asByteArray();
        length = bytes.length;
        out.write(bytes, 0, length);
        break;
      case INT2:
        length = writeShort(out, datum.asInt2());
        break;
      case INT4:
        length = writeVLong(out, datum.asInt4());
        break;
      case INT8:
        length = writeVLong(out, datum.asInt8());
        break;
      case FLOAT4:
        length = writeFloat(out, datum.asFloat4());
        break;
      case FLOAT8:
        length = writeDouble(out, datum.asFloat8());
        break;
      case TEXT: {
        bytes = datum.asTextBytes();
        length = datum.size();
        if (length == 0) {
          bytes = INVALID_UTF__SINGLE_BYTE;
          length = INVALID_UTF__SINGLE_BYTE.length;
        }
        out.write(bytes, 0, bytes.length);
        break;
      }
      case BLOB:
      case INET4:
      case INET6:
        bytes = datum.asByteArray();
        length = bytes.length;
        out.write(bytes, 0, length);
        break;
      case PROTOBUF:
        ProtobufDatum protobufDatum = (ProtobufDatum) datum;
        bytes = protobufDatum.asByteArray();
        length = bytes.length;
        out.write(bytes, 0, length);
        break;
      case NULL_TYPE:
        break;
      default:
        throw new IOException("Does not support type");
    }
    return length;
  }

  @Override
  public Datum deserialize(Column col, byte[] bytes, int offset, int length, byte[] nullCharacters) throws IOException {
    if (length == 0) return NullDatum.get();

    Datum datum;
    switch (col.getDataType().getType()) {
      case BOOLEAN:
        datum = DatumFactory.createBool(bytes[offset]);
        break;
      case BIT:
        datum = DatumFactory.createBit(bytes[offset]);
        break;
      case CHAR: {
        byte[] chars = new byte[length];
        System.arraycopy(bytes, offset, chars, 0, length);
        datum = DatumFactory.createChar(chars);
        break;
      }
      case INT2:
        datum = DatumFactory.createInt2(Bytes.toShort(bytes, offset, length));
        break;
      case INT4:
        datum = DatumFactory.createInt4((int) Bytes.readVLong(bytes, offset));
        break;
      case INT8:
        datum = DatumFactory.createInt8(Bytes.readVLong(bytes, offset));
        break;
      case FLOAT4:
        datum = DatumFactory.createFloat4(toFloat(bytes, offset, length));
        break;
      case FLOAT8:
        datum = DatumFactory.createFloat8(toDouble(bytes, offset, length));
        break;
      case TEXT: {
        byte[] chars = new byte[length];
        System.arraycopy(bytes, offset, chars, 0, length);

        if (Bytes.equals(INVALID_UTF__SINGLE_BYTE, chars)) {
          datum = DatumFactory.createText(new byte[0]);
        } else {
          datum = DatumFactory.createText(chars);
        }
        break;
      }
      case PROTOBUF: {
        ProtobufDatumFactory factory = ProtobufDatumFactory.get(col.getDataType().getCode());
        Message.Builder builder = factory.newBuilder();
        builder.mergeFrom(bytes, offset, length);
        datum = factory.createDatum(builder);
        break;
      }
      case INET4:
        datum = DatumFactory.createInet4(bytes, offset, length);
        break;
      case BLOB:
        datum = DatumFactory.createBlob(bytes, offset, length);
        break;
      default:
        datum = NullDatum.get();
    }
    return datum;
  }

  private byte[] shortBytes = new byte[2];

  public int writeShort(OutputStream out, short val) throws IOException {
    shortBytes[0] = (byte) (val >> 8);
    shortBytes[1] = (byte) val;
    out.write(shortBytes, 0, 2);
    return 2;
  }

  public float toFloat(byte[] bytes, int offset, int length) {
    Preconditions.checkArgument(length == 4);

    int val = ((bytes[offset] & 0x000000FF) << 24) +
        ((bytes[offset + 1] & 0x000000FF) << 16) +
        ((bytes[offset + 2] & 0x000000FF) << 8) +
        (bytes[offset + 3] & 0x000000FF);
    return Float.intBitsToFloat(val);
  }

  private byte[] floatBytes = new byte[4];

  public int writeFloat(OutputStream out, float f) throws IOException {
    int val = Float.floatToIntBits(f);

    floatBytes[0] = (byte) (val >> 24);
    floatBytes[1] = (byte) (val >> 16);
    floatBytes[2] = (byte) (val >> 8);
    floatBytes[3] = (byte) val;
    out.write(floatBytes, 0, 4);
    return floatBytes.length;
  }

  public double toDouble(byte[] bytes, int offset, int length) {
    Preconditions.checkArgument(length == 8);
    long val = ((long) (bytes[offset] & 0x00000000000000FF) << 56) +
        ((long) (bytes[offset + 1] & 0x00000000000000FF) << 48) +
        ((long) (bytes[offset + 2] & 0x00000000000000FF) << 40) +
        ((long) (bytes[offset + 3] & 0x00000000000000FF) << 32) +
        ((long) (bytes[offset + 4] & 0x00000000000000FF) << 24) +
        ((long) (bytes[offset + 5] & 0x00000000000000FF) << 16) +
        ((long) (bytes[offset + 6] & 0x00000000000000FF) << 8) +
        (long) (bytes[offset + 7] & 0x00000000000000FF);
    return Double.longBitsToDouble(val);
  }

  private byte[] doubleBytes = new byte[8];

  public int writeDouble(OutputStream out, double d) throws IOException {
    long val = Double.doubleToLongBits(d);

    doubleBytes[0] = (byte) (val >> 56);
    doubleBytes[1] = (byte) (val >> 48);
    doubleBytes[2] = (byte) (val >> 40);
    doubleBytes[3] = (byte) (val >> 32);
    doubleBytes[4] = (byte) (val >> 24);
    doubleBytes[5] = (byte) (val >> 16);
    doubleBytes[6] = (byte) (val >> 8);
    doubleBytes[7] = (byte) val;
    out.write(doubleBytes, 0, 8);
    return doubleBytes.length;
  }

  private byte[] vLongBytes = new byte[9];

  public static int writeVLongToByteArray(byte[] bytes, int offset, long l) {
    if (l >= -112 && l <= 127) {
      bytes[offset] = (byte) l;
      return 1;
    }

    int len = -112;
    if (l < 0) {
      l ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = l;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    bytes[offset++] = (byte) len;
    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      bytes[offset++] = (byte) ((l & (0xFFL << shiftbits)) >> shiftbits);
    }
    return 1 + len;
  }

  public int writeVLong(OutputStream out, long l) throws IOException {
    int len = writeVLongToByteArray(vLongBytes, 0, l);
    out.write(vLongBytes, 0, len);
    return len;
  }
}
