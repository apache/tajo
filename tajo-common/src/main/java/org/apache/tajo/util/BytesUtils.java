/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.util;

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.netty.util.internal.StringUtil.isSurrogate;

/**
 * Extra utilities for bytes
 */
public class BytesUtils {

  /**
   * Parse the first byte of a vint/vlong to determine the number of bytes
   * @param value the first byte of the vint/vlong
   * @return the total number of bytes (1 to 9)
   */
  public static int decodeVIntSize(byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }

  /**
   * @param n Long to make a VLong of.
   * @return VLong as bytes array.
   */
  public static byte[] vlongToBytes(long n) {
    byte [] result;
    int offset = 0;
    if (n >= -112 && n <= 127) {
      result = new byte[1];
      result[offset] = (byte) n;
      return result;
    }

    int len = -112;
    if (n < 0) {
      n ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = n;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    int size = decodeVIntSize((byte) len);

    result = new byte[size];
    result[offset++] = (byte) len;
    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      result[offset++] = (byte)((n & mask) >> shiftbits);
    }
    return result;
  }

  public static void writeVLong(ByteArrayOutputStream byteStream, long l) {
    byte[] vLongBytes = vlongToBytes(l);
    byteStream.write(vLongBytes, 0, vLongBytes.length);
  }

  /**
   * Converts a char array to a ascii byte array.
   *
   * @param chars string
   * @return the byte array
   */
  static byte[] toASCIIBytes(char[] chars) {
    byte[] buffer = new byte[chars.length];
    for (int i = 0; i < chars.length; i++) {
      buffer[i] = (byte) chars[i];
    }
    return buffer;
  }

  public static byte[][] splitPreserveAllTokens(byte[] str, char separatorChar, int[] target, int numColumns) {
    return splitWorker(str, 0, -1, separatorChar, target, numColumns);
  }

  public static byte[][] splitPreserveAllTokens(byte[] str, int offset, int length, byte[] separator, int[] target, int numColumns) {
    return splitWorker(str, offset, length, separator, target, numColumns);
  }

  public static byte[][] splitPreserveAllTokens(byte[] str, char separatorChar, int numColumns) {
    return splitWorker(str, 0, -1, separatorChar, null, numColumns);
  }

  private static byte[][] splitWorker(byte[] str, int offset, int length, char separatorChar,
                                      int[] target, int numColumns) {
    return splitWorker(str, offset, length, new byte[] {(byte)separatorChar}, target, numColumns);
  }
  
  /**
   * Performs the logic for the <code>split</code> and
   * <code>splitPreserveAllTokens</code> methods that do not return a
   * maximum array length.
   *
   * @param str  the String to parse, may be <code>null</code>
   * @param length amount of bytes to str
   * @param separator the ascii separate characters
   * @param target the projection target
   * @param numColumns number of columns to be retrieved              
   * @return an array of parsed Strings, <code>null</code> if null String input
   */
  private static byte[][] splitWorker(byte[] str, int offset, int length, byte[] separator, int[] target, int numColumns) {
    if (str == null) {
      return null;
    }
    if (length == 0) {
      return new byte[numColumns][0];
    }
    if (length < 0) {
      length = str.length - offset;
    }
    int indexMax = 0;
    if (target != null) {
      for (int index : target) {
        indexMax = Math.max(indexMax, index + 1);
      }
    } else {
      indexMax = numColumns;
    }

    int[][] indices = split(str, offset, length, separator, new int[indexMax][]);
    byte[][] result = new byte[numColumns][];

    // not-picked -> null, picked but not-exists -> byte[0]
    if (target != null) {
      for (int i : target) {
        int[] index = indices[i];
        result[i] = index == null ? new byte[0] : Arrays.copyOfRange(str, index[0], index[1]);
      }
    } else {
      for (int i = 0; i < result.length; i++) {
        int[] index = indices[i];
        result[i] = index == null ? new byte[0] : Arrays.copyOfRange(str, index[0], index[1]);
      }
    }
    return result;
  }

  public static int[][] split(byte[] str, int offset, int length, byte[] separator, int[][] indices) {
    if (indices.length == 0) {
      return indices;   // trivial
    }
    final int limit = offset + length;

    int start = offset;
    int colIndex = 0;
    for (int index = offset; index < limit;) {
      if (onDelimiter(str, index, limit, separator)) {
        indices[colIndex++] = new int[] {start, index};
        if (colIndex >= indices.length) {
          return indices;
        }
        index += separator.length;
        start = index;
      } else {
        index++;
      }
    }
    if (colIndex < indices.length) {
      indices[colIndex] = new int[]{start, limit};
    }
    return indices;
  }
  
  private static boolean onDelimiter(byte[] input, int offset, int limit, byte[] delimiter) {
    for (int i = 0; i < delimiter.length; i++) {
      if (offset + i >= limit || input[offset + i] != delimiter[i]) {
        return false;
      }
    }
    return true;
  }
  
  public static byte[][] splitTrivial(byte[] value, byte delimiter) {
    List<byte[]> split = new ArrayList<>();
    int prev = 0;
    for (int i = 0; i < value.length; i++) {
      if (value[i] == delimiter) {
        split.add(Arrays.copyOfRange(value, prev, i));
        prev = i + 1;
      }
    }
    if (prev <= value.length) {
      split.add(Arrays.copyOfRange(value, prev, value.length));
    }
    return split.toArray(new byte[split.size()][]);
  }

  /**
   * It gets the maximum length among all given the array of bytes.
   * Then, it adds padding (i.e., \0) to byte arrays which are shorter
   * than the maximum length.
   *
   * @param bytes Byte arrays to be padded
   * @return The array of padded bytes
   */
  public static byte[][] padBytes(byte []...bytes) {
    byte [][] padded = new byte[bytes.length][];

    int maxLen = Integer.MIN_VALUE;

    for (byte[] aByte : bytes) {
      maxLen = Math.max(maxLen, aByte.length);
    }

    for (int i = 0; i < bytes.length; i++) {
      int padLen = maxLen - bytes[i].length;
      if (padLen == 0) {
        padded[i] = bytes[i];
      } else if (padLen > 0) {
        padded[i] = Bytes.padTail(bytes[i], padLen);
      } else {
        throw new RuntimeException("maximum length: " + maxLen + ", bytes[" + i + "].length:" + bytes[i].length);
      }
    }

    return padded;
  }

  public static byte [] trimBytes(byte [] bytes) {
    return new String(bytes).trim().getBytes();
  }

  /**
   * this is an implementation copied from ByteBufUtil in netty4
   */
  public static int writeUtf8(ByteBuf buffer, char[] chars, boolean ignoreSurrogate) {
    int oldWriterIndex = buffer.writerIndex();
    int writerIndex = oldWriterIndex;

    // We can use the _set methods as these not need to do any index checks and reference checks.
    // This is possible as we called ensureWritable(...) before.
    for (int i = 0; i < chars.length; i++) {
      char c = chars[i];
      if (c < 0x80) {
        buffer.setByte(writerIndex++, (byte) c);
      } else if (c < 0x800) {
        buffer.setByte(writerIndex++, (byte) (0xc0 | (c >> 6)));
        buffer.setByte(writerIndex++, (byte) (0x80 | (c & 0x3f)));
      } else if (!ignoreSurrogate && isSurrogate(c)) {
        if (!Character.isHighSurrogate(c)) {
          throw new IllegalArgumentException("Invalid encoding. " +
              "Expected high (leading) surrogate at index " + i + " but got " + c);
        }
        final char c2;
        try {
          // Surrogate Pair consumes 2 characters. Optimistically try to get the next character to avoid
          // duplicate bounds checking with charAt. If an IndexOutOfBoundsException is thrown we will
          // re-throw a more informative exception describing the problem.
          c2 = chars[++i];
        } catch (IndexOutOfBoundsException e) {
          throw new IllegalArgumentException("Underflow. " +
              "Expected low (trailing) surrogate at index " + i + " but no more characters found.", e);
        }
        if (!Character.isLowSurrogate(c2)) {
          throw new IllegalArgumentException("Invalid encoding. " +
              "Expected low (trailing) surrogate at index " + i + " but got " + c2);
        }
        int codePoint = Character.toCodePoint(c, c2);
        // See http://www.unicode.org/versions/Unicode7.0.0/ch03.pdf#G2630.
        buffer.setByte(writerIndex++, (byte) (0xf0 | (codePoint >> 18)));
        buffer.setByte(writerIndex++, (byte) (0x80 | ((codePoint >> 12) & 0x3f)));
        buffer.setByte(writerIndex++, (byte) (0x80 | ((codePoint >> 6) & 0x3f)));
        buffer.setByte(writerIndex++, (byte) (0x80 | (codePoint & 0x3f)));
      } else {
        buffer.setByte(writerIndex++, (byte) (0xe0 | (c >> 12)));
        buffer.setByte(writerIndex++, (byte) (0x80 | ((c >> 6) & 0x3f)));
        buffer.setByte(writerIndex++, (byte) (0x80 | (c & 0x3f)));
      }
    }
    // update the writerIndex without any extra checks for performance reasons
    buffer.writerIndex(writerIndex);
    return writerIndex - oldWriterIndex;
  }
}
