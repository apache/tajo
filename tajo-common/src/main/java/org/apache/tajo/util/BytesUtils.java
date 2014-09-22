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

import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Extra utilities for bytes
 */
public class BytesUtils {
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

    int size = WritableUtils.decodeVIntSize((byte) len);

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

  public static byte[][] splitPreserveAllTokens(byte[] str, char separatorChar, int[] target) {
    return splitWorker(str, 0, -1, separatorChar, true, target);
  }

  public static byte[][] splitPreserveAllTokens(byte[] str, int offset, int length, char separatorChar, int[] target) {
    return splitWorker(str, offset, length, separatorChar, true, target);
  }

  public static byte[][] splitPreserveAllTokens(byte[] str, char separatorChar) {
    return splitWorker(str, 0, -1, separatorChar, true, null);
  }

  public static byte[][] splitPreserveAllTokens(byte[] str, int length, char separatorChar) {
    return splitWorker(str, 0, length, separatorChar, true, null);
  }

  /**
   * Performs the logic for the <code>split</code> and
   * <code>splitPreserveAllTokens</code> methods that do not return a
   * maximum array length.
   *
   * @param str  the String to parse, may be <code>null</code>
   * @param length amount of bytes to str
   * @param separatorChar the ascii separate character
   * @param preserveAllTokens if <code>true</code>, adjacent separators are
   * treated as empty token separators; if <code>false</code>, adjacent
   * separators are treated as one separator.
   * @param target the projection target
   * @return an array of parsed Strings, <code>null</code> if null String input
   */
  private static byte[][] splitWorker(byte[] str, int offset, int length, char separatorChar,
                                      boolean preserveAllTokens, int[] target) {
    // Performance tuned for 2.0 (JDK1.4)

    if (str == null) {
      return null;
    }
    int len = length;
    if (len == 0) {
      return new byte[1][0];
    }else if(len < 0){
      len = str.length - offset;
    }

    List list = new ArrayList();
    int i = 0, start = 0;
    boolean match = false;
    boolean lastMatch = false;
    int currentTarget = 0;
    int currentIndex = 0;
    while (i < len) {
      if (str[i + offset] == separatorChar) {
        if (match || preserveAllTokens) {
          if (target == null) {
            byte[] bytes = new byte[i - start];
            System.arraycopy(str, start + offset, bytes, 0, bytes.length);
            list.add(bytes);
          } else if (target.length > currentTarget && currentIndex == target[currentTarget]) {
            byte[] bytes = new byte[i - start];
            System.arraycopy(str, start + offset, bytes, 0, bytes.length);
            list.add(bytes);
            currentTarget++;
          } else {
            list.add(null);
          }
          currentIndex++;
          match = false;
          lastMatch = true;
        }
        start = ++i;
        continue;
      }
      lastMatch = false;
      match = true;
      i++;
    }
    if (match || (preserveAllTokens && lastMatch)) {
      if (target == null) {
        byte[] bytes = new byte[i - start];
        System.arraycopy(str, start + offset, bytes, 0, bytes.length);
        list.add(bytes);
      } else if (target.length > currentTarget && currentIndex == target[currentTarget]) {
        byte[] bytes = new byte[i - start];
        System.arraycopy(str, start + offset, bytes, 0, bytes.length);
        list.add(bytes); //str.substring(start, i));
        currentTarget++;
      } else {
        list.add(null);
      }
      currentIndex++;
    }
    return (byte[][]) list.toArray(new byte[list.size()][]);
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

    for (int i = 0; i < bytes.length; i++) {
      maxLen = Math.max(maxLen, bytes[i].length);
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
}
