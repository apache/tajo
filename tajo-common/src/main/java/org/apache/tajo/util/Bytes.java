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

package org.apache.tajo.util;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import sun.misc.Unsafe;

import java.io.*;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class that handles byte arrays, conversions to/from other types,
 * comparisons, hash code generation, manufacturing keys for HashMaps or
 * HashSets, etc.
 */
public class Bytes {

  private static final Log LOG = LogFactory.getLog(Bytes.class);

  /**
   * Size of boolean in bytes
   */
  public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

  /**
   * Size of byte in bytes
   */
  public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

  /**
   * Size of char in bytes
   */
  public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

  /**
   * Size of double in bytes
   */
  public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

  /**
   * Size of float in bytes
   */
  public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

  /**
   * Size of int in bytes
   */
  public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

  /**
   * Size of long in bytes
   */
  public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

  /**
   * Size of short in bytes
   */
  public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;


  /**
   * Estimate of size cost to pay beyond payload in jvm for instance of byte [].
   * Estimate based on study of jhat and jprofiler numbers.
   */
  // JHat says BU is 56 bytes.
  // SizeOf which uses java.lang.instrument says 24 bytes. (3 longs?)
  public static final int ESTIMATED_HEAP_TAX = 16;

  /**
   * Byte array comparator class.
   */
  public static class ByteArrayComparator implements RawComparator<byte []> {
    /**
     * Constructor
     */
    public ByteArrayComparator() {
      super();
    }
    public int compare(byte [] left, byte [] right) {
      return compareTo(left, right);
    }
    public int compare(byte [] b1, int s1, int l1, byte [] b2, int s2, int l2) {
      return LexicographicalComparerHolder.BEST_COMPARER.
        compareTo(b1, s1, l1, b2, s2, l2);
    }
  }

  /**
   * Pass this to TreeMaps where byte [] are keys.
   */
  public static Comparator<byte []> BYTES_COMPARATOR =
    new ByteArrayComparator();

  /**
   * Use comparing byte arrays, byte-by-byte
   */
  public static RawComparator<byte []> BYTES_RAWCOMPARATOR =
    new ByteArrayComparator();

  /**
   * Read byte-array written with a WritableableUtils.vint prefix.
   * @param in Input to read from.
   * @return byte array read off <code>in</code>
   * @throws java.io.IOException e
   */
  public static byte [] readByteArray(final DataInput in)
  throws IOException {
    int len = WritableUtils.readVInt(in);
    if (len < 0) {
      throw new NegativeArraySizeException(Integer.toString(len));
    }
    byte [] result = new byte[len];
    in.readFully(result, 0, len);
    return result;
  }

  /**
   * Read byte-array written with a WritableableUtils.vint prefix.
   * IOException is converted to a RuntimeException.
   * @param in Input to read from.
   * @return byte array read off <code>in</code>
   */
  public static byte [] readByteArrayThrowsRuntime(final DataInput in) {
    try {
      return readByteArray(in);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write byte-array with a WritableableUtils.vint prefix.
   * @param out output stream to be written to
   * @param b array to write
   * @throws java.io.IOException e
   */
  public static void writeByteArray(final DataOutput out, final byte [] b)
  throws IOException {
    if(b == null) {
      WritableUtils.writeVInt(out, 0);
    } else {
      writeByteArray(out, b, 0, b.length);
    }
  }

  /**
   * Write byte-array to out with a vint length prefix.
   * @param out output stream
   * @param b array
   * @param offset offset into array
   * @param length length past offset
   * @throws java.io.IOException e
   */
  public static void writeByteArray(final DataOutput out, final byte [] b,
      final int offset, final int length)
  throws IOException {
    WritableUtils.writeVInt(out, length);
    out.write(b, offset, length);
  }

  /**
   * Write byte-array from src to tgt with a vint length prefix.
   * @param tgt target array
   * @param tgtOffset offset into target array
   * @param src source array
   * @param srcOffset source offset
   * @param srcLength source length
   * @return New offset in src array.
   */
  public static int writeByteArray(final byte [] tgt, final int tgtOffset,
      final byte [] src, final int srcOffset, final int srcLength) {
    byte [] vint = vintToBytes(srcLength);
    System.arraycopy(vint, 0, tgt, tgtOffset, vint.length);
    int offset = tgtOffset + vint.length;
    System.arraycopy(src, srcOffset, tgt, offset, srcLength);
    return offset + srcLength;
  }

  /**
   * Put bytes at the specified byte array position.
   * @param tgtBytes the byte array
   * @param tgtOffset position in the array
   * @param srcBytes array to write out
   * @param srcOffset source offset
   * @param srcLength source length
   * @return incremented offset
   */
  public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes,
      int srcOffset, int srcLength) {
    System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
    return tgtOffset + srcLength;
  }

  /**
   * Write a single byte out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param b byte to write out
   * @return incremented offset
   */
  public static int putByte(byte[] bytes, int offset, byte b) {
    bytes[offset] = b;
    return offset + 1;
  }

  /**
   * Returns a new byte array, copied from the passed ByteBuffer.
   * @param bb A ByteBuffer
   * @return the byte array
   */
  public static byte[] toBytes(ByteBuffer bb) {
    int length = bb.limit();
    byte [] result = new byte[length];
    System.arraycopy(bb.array(), bb.arrayOffset(), result, 0, length);
    return result;
  }

  /**
   * @param b Presumed UTF-8 encoded byte array.
   * @return String made from <code>b</code>
   */
  public static String toString(final byte [] b) {
    if (b == null) {
      return null;
    }
    return toString(b, 0, b.length);
  }

  /**
   * Joins two byte arrays together using a separator.
   * @param b1 The first byte array.
   * @param sep The separator to use.
   * @param b2 The second byte array.
   */
  public static String toString(final byte [] b1,
                                String sep,
                                final byte [] b2) {
    return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
  }

  /**
   * This method will convert utf8 encoded bytes into a string. If
   * an UnsupportedEncodingException occurs, this method will eat it
   * and return null instead.
   *
   * @param b Presumed UTF-8 encoded byte array.
   * @param off offset into array
   * @param len length of utf-8 sequence
   * @return String made from <code>b</code> or null
   */
  public static String toString(final byte [] b, int off, int len) {
    if (b == null) {
      return null;
    }
    if (len == 0) {
      return "";
    }
    try {
      return new String(b, off, len, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.error("UTF-8 not supported?", e);
      return null;
    }
  }

  /**
   * Write a printable representation of a byte array.
   *
   * @param b byte array
   * @return string
   * @see #toStringBinary(byte[], int, int)
   */
  public static String toStringBinary(final byte [] b) {
    if (b == null)
      return "null";
    return toStringBinary(b, 0, b.length);
  }
  
  /**
   * Converts the given byte buffer, from its array offset to its limit, to
   * a string. The position and the mark are ignored.
   *
   * @param buf a byte buffer
   * @return a string representation of the buffer's binary contents
   */
  public static String toStringBinary(ByteBuffer buf) {
    if (buf == null)
      return "null";
    return toStringBinary(buf.array(), buf.arrayOffset(), buf.limit());
  }

  /**
   * Write a printable representation of a byte array. Non-printable
   * characters are hex escaped in the format \\x%02X, eg:
   * \x00 \x05 etc
   *
   * @param b array to write out
   * @param off offset to start at
   * @param len length to write
   * @return string output
   */
  public static String toStringBinary(final byte [] b, int off, int len) {
    StringBuilder result = new StringBuilder();
    try {
      String first = new String(b, off, len, "ISO-8859-1");
      for (int i = 0; i < first.length() ; ++i ) {
        int ch = first.charAt(i) & 0xFF;
        if ( (ch >= '0' && ch <= '9')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= 'a' && ch <= 'z')
            || " `~!@#$%^&*()-_=+[]{}\\|;:'\",.<>/?".indexOf(ch) >= 0 ) {
          result.append(first.charAt(i));
        } else {
          result.append(String.format("\\x%02X", ch));
        }
      }
    } catch (UnsupportedEncodingException e) {
      LOG.error("ISO-8859-1 not supported?", e);
    }
    return result.toString();
  }

  private static boolean isHexDigit(char c) {
    return
        (c >= 'A' && c <= 'F') ||
        (c >= '0' && c <= '9');
  }

  /**
   * Takes a ASCII digit in the range A-F0-9 and returns
   * the corresponding integer/ordinal value.
   * @param ch  The hex digit.
   * @return The converted hex value as a byte.
   */
  public static byte toBinaryFromHex(byte ch) {
    if ( ch >= 'A' && ch <= 'F' )
      return (byte) ((byte)10 + (byte) (ch - 'A'));
    // else
    return (byte) (ch - '0');
  }

  public static byte [] toBytesBinary(String in) {
    // this may be bigger than we need, but lets be safe.
    byte [] b = new byte[in.length()];
    int size = 0;
    for (int i = 0; i < in.length(); ++i) {
      char ch = in.charAt(i);
      if (ch == '\\') {
        // begin hex escape:
        char next = in.charAt(i+1);
        if (next != 'x') {
          // invalid escape sequence, ignore this one.
          b[size++] = (byte)ch;
          continue;
        }
        // ok, take next 2 hex digits.
        char hd1 = in.charAt(i+2);
        char hd2 = in.charAt(i+3);

        // they need to be A-F0-9:
        if (!isHexDigit(hd1) ||
            !isHexDigit(hd2)) {
          // bogus escape code, ignore:
          continue;
        }
        // turn hex ASCII digit -> number
        byte d = (byte) ((toBinaryFromHex((byte)hd1) << 4) + toBinaryFromHex((byte)hd2));

        b[size++] = d;
        i += 3; // skip 3
      } else {
        b[size++] = (byte) ch;
      }
    }
    // resize:
    byte [] b2 = new byte[size];
    System.arraycopy(b, 0, b2, 0, size);
    return b2;
  }

  /**
   * Converts a string to a UTF-8 byte array.
   * @param s string
   * @return the byte array
   */
  public static byte[] toBytes(String s) {
    try {
      return s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.error("UTF-8 not supported?", e);
      return null;
    }
  }

  /**
   * Converts a char array to a ascii byte array.
   *
   * @param chars string
   * @return the byte array
   */
  public static byte[] toASCIIBytes(char[] chars) {
    byte[] buffer = new byte[chars.length];
    for (int i = 0; i < chars.length; i++) {
      buffer[i] = (byte) chars[i];
    }
    return buffer;
  }

  /**
   * Convert a boolean to a byte array. True becomes -1
   * and false becomes 0.
   *
   * @param b value
   * @return <code>b</code> encoded in a byte array.
   */
  public static byte [] toBytes(final boolean b) {
    return new byte[] { b ? (byte) -1 : (byte) 0 };
  }

  /**
   * Reverses {@link #toBytes(boolean)}
   * @param b array
   * @return True or false.
   */
  public static boolean toBoolean(final byte [] b) {
    if (b.length != 1) {
      throw new IllegalArgumentException("Array has wrong size: " + b.length);
    }
    return b[0] != (byte) 0;
  }

  /**
   * Convert a long value to a byte array using big-endian.
   *
   * @param val value to convert
   * @return the byte array
   */
  public static byte[] toBytes(long val) {
    byte [] b = new byte[8];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * Converts a byte array to a long value. Reverses
   * {@link #toBytes(long)}
   * @param bytes array
   * @return the long value
   */
  public static long toLong(byte[] bytes) {
    return toLong(bytes, 0, SIZEOF_LONG);
  }

  /**
   * Converts a byte array to a long value. Assumes there will be
   * {@link #SIZEOF_LONG} bytes available.
   *
   * @param bytes bytes
   * @param offset offset
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset) {
    return toLong(bytes, offset, SIZEOF_LONG);
  }

  /**
   * Converts a byte array to a long value.
   *
   * @param bytes array of bytes
   * @param offset offset into array
   * @param length length of data (must be {@link #SIZEOF_LONG})
   * @return the long value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_LONG} or
   * if there's not enough room in the array at the offset indicated.
   */
  public static long toLong(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_LONG || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
    }
    long l = 0;
    for(int i = offset; i < offset + length; i++) {
      l <<= 8;
      l ^= bytes[i] & 0xFF;
    }
    return l;
  }

  private static IllegalArgumentException
    explainWrongLengthOrOffset(final byte[] bytes,
                               final int offset,
                               final int length,
                               final int expectedLength) {
    String reason;
    if (length != expectedLength) {
      reason = "Wrong length: " + length + ", expected " + expectedLength;
    } else {
     reason = "offset (" + offset + ") + length (" + length + ") exceed the"
        + " capacity of the array: " + bytes.length;
    }
    return new IllegalArgumentException(reason);
  }

  /**
   * Put a long value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putLong(byte[] bytes, int offset, long val) {
    if (bytes.length - offset < SIZEOF_LONG) {
      throw new IllegalArgumentException("Not enough room to put a long at"
          + " offset " + offset + " in a " + bytes.length + " byte array");
    }
    for(int i = offset + 7; i > offset; i--) {
      bytes[i] = (byte) val;
      val >>>= 8;
    }
    bytes[offset] = (byte) val;
    return offset + SIZEOF_LONG;
  }

  /**
   * Presumes float encoded as IEEE 754 floating-point "single format"
   * @param bytes byte array
   * @return Float made from passed byte array.
   */
  public static float toFloat(byte [] bytes) {
    return toFloat(bytes, 0);
  }

  /**
   * Presumes float encoded as IEEE 754 floating-point "single format"
   * @param bytes array to convert
   * @param offset offset into array
   * @return Float made from passed byte array.
   */
  public static float toFloat(byte [] bytes, int offset) {
    return Float.intBitsToFloat(toInt(bytes, offset, SIZEOF_INT));
  }

  /**
   * @param bytes byte array
   * @param offset offset to write to
   * @param f float value
   * @return New offset in <code>bytes</code>
   */
  public static int putFloat(byte [] bytes, int offset, float f) {
    return putInt(bytes, offset, Float.floatToRawIntBits(f));
  }

  /**
   * @param f float value
   * @return the float represented as byte []
   */
  public static byte [] toBytes(final float f) {
    // Encode it as int
    return Bytes.toBytes(Float.floatToRawIntBits(f));
  }

  /**
   * @param bytes byte array
   * @return Return double made from passed bytes.
   */
  public static double toDouble(final byte [] bytes) {
    return toDouble(bytes, 0);
  }

  /**
   * @param bytes byte array
   * @param offset offset where double is
   * @return Return double made from passed bytes.
   */
  public static double toDouble(final byte [] bytes, final int offset) {
    return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
  }

  /**
   * @param bytes byte array
   * @param offset offset to write to
   * @param d value
   * @return New offset into array <code>bytes</code>
   */
  public static int putDouble(byte [] bytes, int offset, double d) {
    return putLong(bytes, offset, Double.doubleToLongBits(d));
  }

  /**
   * Serialize a double as the IEEE 754 double format output. The resultant
   * array will be 8 bytes long.
   *
   * @param d value
   * @return the double represented as byte []
   */
  public static byte [] toBytes(final double d) {
    // Encode it as a long
    return Bytes.toBytes(Double.doubleToRawLongBits(d));
  }

  /**
   * Convert an int value to a byte array
   * @param val value
   * @return the byte array
   */
  public static byte[] toBytes(int val) {
    byte [] b = new byte[4];
    for(int i = 3; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * Converts a byte array to an int value
   * @param bytes byte array
   * @return the int value
   */
  public static int toInt(byte[] bytes) {
    return toInt(bytes, 0, SIZEOF_INT);
  }

  /**
   * Converts a byte array to an int value
   * @param bytes byte array
   * @param offset offset into array
   * @return the int value
   */
  public static int toInt(byte[] bytes, int offset) {
    return toInt(bytes, offset, SIZEOF_INT);
  }

  /**
   * Converts a byte array to an int value
   * @param bytes byte array
   * @param offset offset into array
   * @param length length of int (has to be {@link #SIZEOF_INT})
   * @return the int value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_INT} or
   * if there's not enough room in the array at the offset indicated.
   */
  public static int toInt(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_INT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
    }
    int n = 0;
    for(int i = offset; i < (offset + length); i++) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }

  /**
   * Put an int value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putInt(byte[] bytes, int offset, int val) {
    if (bytes.length - offset < SIZEOF_INT) {
      throw new IllegalArgumentException("Not enough room to put an int at"
          + " offset " + offset + " in a " + bytes.length + " byte array");
    }
    for(int i= offset + 3; i > offset; i--) {
      bytes[i] = (byte) val;
      val >>>= 8;
    }
    bytes[offset] = (byte) val;
    return offset + SIZEOF_INT;
  }

  /**
   * Convert a short value to a byte array of {@link #SIZEOF_SHORT} bytes long.
   * @param val value
   * @return the byte array
   */
  public static byte[] toBytes(short val) {
    byte[] b = new byte[SIZEOF_SHORT];
    b[1] = (byte) val;
    val >>= 8;
    b[0] = (byte) val;
    return b;
  }

  /**
   * Converts a byte array to a short value
   * @param bytes byte array
   * @return the short value
   */
  public static short toShort(byte[] bytes) {
    return toShort(bytes, 0, SIZEOF_SHORT);
  }

  /**
   * Converts a byte array to a short value
   * @param bytes byte array
   * @param offset offset into array
   * @return the short value
   */
  public static short toShort(byte[] bytes, int offset) {
    return toShort(bytes, offset, SIZEOF_SHORT);
  }

  /**
   * Converts a byte array to a short value
   * @param bytes byte array
   * @param offset offset into array
   * @param length length, has to be {@link #SIZEOF_SHORT}
   * @return the short value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_SHORT}
   * or if there's not enough room in the array at the offset indicated.
   */
  public static short toShort(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_SHORT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
    }
    short n = 0;
    n ^= bytes[offset] & 0xFF;
    n <<= 8;
    n ^= bytes[offset+1] & 0xFF;
    return n;
  }

  /**
   * This method will get a sequence of bytes from pos -> limit,
   * but will restore pos after.
   * @param buf
   * @return
   */
  public static byte[] getBytes(ByteBuffer buf) {
    int savedPos = buf.position();
    byte [] newBytes = new byte[buf.remaining()];
    buf.get(newBytes);
    buf.position(savedPos);
    return newBytes;
  }

  /**
   * Put a short value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putShort(byte[] bytes, int offset, short val) {
    if (bytes.length - offset < SIZEOF_SHORT) {
      throw new IllegalArgumentException("Not enough room to put a short at"
          + " offset " + offset + " in a " + bytes.length + " byte array");
    }
    bytes[offset+1] = (byte) val;
    val >>= 8;
    bytes[offset] = (byte) val;
    return offset + SIZEOF_SHORT;
  }

  /**
   * Convert a BigDecimal value to a byte array
   *
   * @param val
   * @return the byte array
   */
  public static byte[] toBytes(BigDecimal val) {
    byte[] valueBytes = val.unscaledValue().toByteArray();
    byte[] result = new byte[valueBytes.length + SIZEOF_INT];
    int offset = putInt(result, 0, val.scale());
    putBytes(result, offset, valueBytes, 0, valueBytes.length);
    return result;
  }


  /**
   * Converts a byte array to a BigDecimal
   *
   * @param bytes
   * @return the char value
   */
  public static BigDecimal toBigDecimal(byte[] bytes) {
    return toBigDecimal(bytes, 0, bytes.length);
  }

  /**
   * Converts a byte array to a BigDecimal value
   *
   * @param bytes
   * @param offset
   * @return the char value
   */
  public static BigDecimal toBigDecimal(byte[] bytes, int offset) {
    return toBigDecimal(bytes, offset, bytes.length);
  }

  /**
   * Converts a byte array to a BigDecimal value
   *
   * @param bytes
   * @param offset
   * @param length
   * @return the char value
   */
  public static BigDecimal toBigDecimal(byte[] bytes, int offset, final int length) {
    if (bytes == null || length < SIZEOF_INT + 1 ||
      (offset + length > bytes.length)) {
      return null;
    }

    int scale = toInt(bytes, 0);
    byte[] tcBytes = new byte[length - SIZEOF_INT];
    System.arraycopy(bytes, SIZEOF_INT, tcBytes, 0, length - SIZEOF_INT);
    return new BigDecimal(new BigInteger(tcBytes), scale);
  }

  /**
   * Put a BigDecimal value out to the specified byte array position.
   *
   * @param bytes  the byte array
   * @param offset position in the array
   * @param val    BigDecimal to write out
   * @return incremented offset
   */
  public static int putBigDecimal(byte[] bytes, int offset, BigDecimal val) {
    if (bytes == null) {
      return offset;
    }

    byte[] valueBytes = val.unscaledValue().toByteArray();
    byte[] result = new byte[valueBytes.length + SIZEOF_INT];
    offset = putInt(result, offset, val.scale());
    return putBytes(result, offset, valueBytes, 0, valueBytes.length);
  }
  
  /**
   * @param vint Integer to make a vint of.
   * @return Vint as bytes array.
   */
  public static byte [] vintToBytes(final long vint) {
    long i = vint;
    int size = WritableUtils.getVIntSize(i);
    byte [] result = new byte[size];
    int offset = 0;
    if (i >= -112 && i <= 127) {
      result[offset] = (byte) i;
      return result;
    }

    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    result[offset++] = (byte) len;

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      result[offset++] = (byte)((i & mask) >> shiftbits);
    }
    return result;
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

    int size = WritableUtils.decodeVIntSize((byte)len);

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

  /**
   * @param buffer buffer to convert
   * @return vint bytes as an integer.
   */
  public static long bytesToVint(final byte [] buffer) {
    int offset = 0;
    byte firstByte = buffer[offset++];
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = buffer[offset++];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
  }

  /**
   * Reads a zero-compressed encoded long from input stream and returns it.
   * @param buffer Binary array
   * @param offset Offset into array at which vint begins.
   * @throws java.io.IOException e
   * @return deserialized long from stream.
   */
  public static long readVLong(final byte [] buffer, final int offset)
  throws IOException {
    byte firstByte = buffer[offset];
    int length = (byte) WritableUtils.decodeVIntSize(firstByte);
    if (length == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < length - 1; idx++) {
      byte b = buffer[offset + 1 + idx];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  /**
   * @param left left operand
   * @param right right operand
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(final byte [] left, final byte [] right) {
    return LexicographicalComparerHolder.BEST_COMPARER.
      compareTo(left, 0, left.length, right, 0, right.length);
  }

  /**
   * Lexicographically compare two arrays.
   *
   * @param buffer1 left operand
   * @param buffer2 right operand
   * @param offset1 Where to start comparing in the left buffer
   * @param offset2 Where to start comparing in the right buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] buffer1, int offset1, int length1,
      byte[] buffer2, int offset2, int length2) {
    return LexicographicalComparerHolder.BEST_COMPARER.
      compareTo(buffer1, offset1, length1, buffer2, offset2, length2);
  }
  
  /**
   * The number of bytes required to represent a primitive {@code long}
   * value.
   */
  public static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
 
  interface Comparer<T> {
    abstract public int compareTo(T buffer1, int offset1, int length1,
                                  T buffer2, int offset2, int length2);
  }

  @VisibleForTesting
  static Comparer<byte[]> lexicographicalComparerJavaImpl() {
    return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
  }

  /**
   * Provides a lexicographical comparer implementation; either a Java
   * implementation or a faster implementation based on {@link sun.misc.Unsafe}.
   *
   * <p>Uses reflection to gracefully fall back to the Java implementation if
   * {@code Unsafe} isn't available.
   */
  @VisibleForTesting
  static class LexicographicalComparerHolder {
    static final String UNSAFE_COMPARER_NAME =
        LexicographicalComparerHolder.class.getName() + "$UnsafeComparer";
    
    static final Comparer<byte[]> BEST_COMPARER = getBestComparer();
    /**
     * Returns the Unsafe-using Comparer, or falls back to the pure-Java
     * implementation if unable to do so.
     */
    static Comparer<byte[]> getBestComparer() {
      try {
        Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);

        // yes, UnsafeComparer does implement Comparer<byte[]>
        @SuppressWarnings("unchecked")
        Comparer<byte[]> comparer =
          (Comparer<byte[]>) theClass.getEnumConstants()[0];
        return comparer;
      } catch (Throwable t) { // ensure we really catch *everything*
        return lexicographicalComparerJavaImpl();
      }
    }
    
    enum PureJavaComparer implements Comparer<byte[]> {
      INSTANCE;

      @Override
      public int compareTo(byte[] buffer1, int offset1, int length1,
          byte[] buffer2, int offset2, int length2) {
        // Short circuit equal case
        if (buffer1 == buffer2 &&
            offset1 == offset2 &&
            length1 == length2) {
          return 0;
        }
        // Bring WritableComparator code local
        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
          int a = (buffer1[i] & 0xff);
          int b = (buffer2[j] & 0xff);
          if (a != b) {
            return a - b;
          }
        }
        return length1 - length2;
      }
    }

    @VisibleForTesting
    enum UnsafeComparer implements Comparer<byte[]> {
      INSTANCE;

      static final Unsafe theUnsafe;

      /** The offset to the first element in a byte array. */
      static final int BYTE_ARRAY_BASE_OFFSET;

      static {
        theUnsafe = (Unsafe) AccessController.doPrivileged(
            new PrivilegedAction<Object>() {
              @Override
              public Object run() {
                try {
                  Field f = Unsafe.class.getDeclaredField("theUnsafe");
                  f.setAccessible(true);
                  return f.get(null);
                } catch (NoSuchFieldException e) {
                  // It doesn't matter what we throw;
                  // it's swallowed in getBestComparer().
                  throw new Error();
                } catch (IllegalAccessException e) {
                  throw new Error();
                }
              }
            });

        BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

        // sanity check - this should never fail
        if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
          throw new AssertionError();
        }
      }

      static final boolean littleEndian =
        ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

      /**
       * Returns true if x1 is less than x2, when both values are treated as
       * unsigned.
       */
      static boolean lessThanUnsigned(long x1, long x2) {
        return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
      }

      /**
       * Lexicographically compare two arrays.
       *
       * @param buffer1 left operand
       * @param buffer2 right operand
       * @param offset1 Where to start comparing in the left buffer
       * @param offset2 Where to start comparing in the right buffer
       * @param length1 How much to compare from the left buffer
       * @param length2 How much to compare from the right buffer
       * @return 0 if equal, < 0 if left is less than right, etc.
       */
      @Override
      public int compareTo(byte[] buffer1, int offset1, int length1,
          byte[] buffer2, int offset2, int length2) {
        // Short circuit equal case
        if (buffer1 == buffer2 &&
            offset1 == offset2 &&
            length1 == length2) {
          return 0;
        }
        int minLength = Math.min(length1, length2);
        int minWords = minLength / LONG_BYTES;
        int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
        int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

        /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
         * time is no slower than comparing 4 bytes at a time even on 32-bit.
         * On the other hand, it is substantially faster on 64-bit.
         */
        for (int i = 0; i < minWords * LONG_BYTES; i += LONG_BYTES) {
          long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
          long rw = theUnsafe.getLong(buffer2, offset2Adj + (long) i);
          long diff = lw ^ rw;

          if (diff != 0) {
            if (!littleEndian) {
              return lessThanUnsigned(lw, rw) ? -1 : 1;
            }

            // Use binary search
            int n = 0;
            int y;
            int x = (int) diff;
            if (x == 0) {
              x = (int) (diff >>> 32);
              n = 32;
            }

            y = x << 16;
            if (y == 0) {
              n += 16;
            } else {
              x = y;
            }

            y = x << 8;
            if (y == 0) {
              n += 8;
            }
            return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
          }
        }

        // The epilogue to cover the last (minLength % 8) elements.
        for (int i = minWords * LONG_BYTES; i < minLength; i++) {
          int a = (buffer1[offset1 + i] & 0xff);
          int b = (buffer2[offset2 + i] & 0xff);
          if (a != b) {
            return a - b;
          }
        }
        return length1 - length2;
      }
    }
  }

  /**
   * @param left left operand
   * @param right right operand
   * @return True if equal
   */
  public static boolean equals(final byte [] left, final byte [] right) {
    // Could use Arrays.equals?
    //noinspection SimplifiableConditionalExpression
    if (left == right) return true;
    if (left == null || right == null) return false;
    if (left.length != right.length) return false;
    if (left.length == 0) return true;
    
    // Since we're often comparing adjacent sorted data,
    // it's usual to have equal arrays except for the very last byte
    // so check that first
    if (left[left.length - 1] != right[right.length - 1]) return false;

    return compareTo(left, right) == 0;
  }
  
  public static boolean equals(final byte[] left, int leftOffset, int leftLen,
                               final byte[] right, int rightOffset, int rightLen) {
    // short circuit case
    if (left == right &&
        leftOffset == rightOffset &&
        leftLen == rightLen) {
      return true;
    }
    // different lengths fast check
    if (leftLen != rightLen) {
      return false;
    }
    if (leftLen == 0) {
      return true;
    }
    
    // Since we're often comparing adjacent sorted data,
    // it's usual to have equal arrays except for the very last byte
    // so check that first
    if (left[leftOffset + leftLen - 1] != right[rightOffset + rightLen - 1]) return false;

    return LexicographicalComparerHolder.BEST_COMPARER.
      compareTo(left, leftOffset, leftLen, right, rightOffset, rightLen) == 0;
  }
  

  /**
   * Return true if the byte array on the right is a prefix of the byte
   * array on the left.
   */
  public static boolean startsWith(byte[] bytes, byte[] prefix) {
    return bytes != null && prefix != null &&
      bytes.length >= prefix.length &&
      LexicographicalComparerHolder.BEST_COMPARER.
        compareTo(bytes, 0, prefix.length, prefix, 0, prefix.length) == 0;      
  }

  /**
   * @param b bytes to hash
   * @return Runs {@link org.apache.hadoop.io.WritableComparator#hashBytes(byte[], int)} on the
   * passed in array.  This method is what {@link org.apache.hadoop.io.Text} and
   */
  public static int hashCode(final byte [] b) {
    return hashCode(b, b.length);
  }

  /**
   * @param b value
   * @param length length of the value
   * @return Runs {@link org.apache.hadoop.io.WritableComparator#hashBytes(byte[], int)} on the
   * passed in array.  This method is what {@link org.apache.hadoop.io.Text} and
   */
  public static int hashCode(final byte [] b, final int length) {
    return WritableComparator.hashBytes(b, length);
  }

  /**
   * @param b bytes to hash
   * @return A hash of <code>b</code> as an Integer that can be used as key in
   * Maps.
   */
  public static Integer mapKey(final byte [] b) {
    return hashCode(b);
  }

  /**
   * @param b bytes to hash
   * @param length length to hash
   * @return A hash of <code>b</code> as an Integer that can be used as key in
   * Maps.
   */
  public static Integer mapKey(final byte [] b, final int length) {
    return hashCode(b, length);
  }

  /**
   * @param a lower half
   * @param b upper half
   * @return New array that has a in lower half and b in upper half.
   */
  public static byte [] add(final byte [] a, final byte [] b) {
    return add(a, b, new byte [0]);
  }

  /**
   * @param a first third
   * @param b second third
   * @param c third third
   * @return New array made from a, b and c
   */
  public static byte [] add(final byte [] a, final byte [] b, final byte [] c) {
    byte [] result = new byte[a.length + b.length + c.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    System.arraycopy(c, 0, result, a.length + b.length, c.length);
    return result;
  }

  /**
   * @param a array
   * @param length amount of bytes to grab
   * @return First <code>length</code> bytes from <code>a</code>
   */
  public static byte [] head(final byte [] a, final int length) {
    if (a.length < length) {
      return null;
    }
    byte [] result = new byte[length];
    System.arraycopy(a, 0, result, 0, length);
    return result;
  }

  /**
   * @param a array
   * @param length amount of bytes to snarf
   * @return Last <code>length</code> bytes from <code>a</code>
   */
  public static byte [] tail(final byte [] a, final int length) {
    if (a.length < length) {
      return null;
    }
    byte [] result = new byte[length];
    System.arraycopy(a, a.length - length, result, 0, length);
    return result;
  }

  /**
   * @param a array
   * @param length new array size
   * @return Value in <code>a</code> plus <code>length</code> prepended 0 bytes
   */
  public static byte [] padHead(final byte [] a, final int length) {
    byte [] padding = new byte[length];
    for (int i = 0; i < length; i++) {
      padding[i] = 0;
    }
    return add(padding,a);
  }

  /**
   * @param a array
   * @param length new array size
   * @return Value in <code>a</code> plus <code>length</code> appended 0 bytes
   */
  public static byte [] padTail(final byte [] a, final int length) {
    byte [] padding = new byte[length];
    for (int i = 0; i < length; i++) {
      padding[i] = 0;
    }
    return add(a,padding);
  }

  /**
   * Split passed range.  Expensive operation relatively.  Uses BigInteger math.
   * Useful splitting ranges for MapReduce jobs.
   * @param a Beginning of range
   * @param b End of range
   * @param num Number of times to split range.  Pass 1 if you want to split
   * the range in two; i.e. one split.
   * @return Array of dividing values
   */
  public static byte [][] split(final byte [] a, final byte [] b, final int num) {
    byte[][] ret = new byte[num+2][];
    int i = 0;
    Iterable<byte[]> iter = iterateOnSplits(a, b, num);
    if (iter == null) return null;
    for (byte[] elem : iter) {
      ret[i++] = elem;
    }
    return ret;
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
   * @param target the projection target
   * treated as empty token separators; if <code>false</code>, adjacent
   * separators are treated as one separator.
   * @return an array of parsed Strings, <code>null</code> if null String input
   */
  private static byte[][] splitWorker(byte[] str, int offset, int length, char separatorChar, boolean preserveAllTokens, int[] target) {
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
   * Iterate over keys within the passed inclusive range.
   */
  public static Iterable<byte[]> iterateOnSplits(
      final byte[] a, final byte[]b, final int num)
  {  
    byte [] aPadded;
    byte [] bPadded;
    if (a.length < b.length) {
      aPadded = padTail(a, b.length - a.length);
      bPadded = b;
    } else if (b.length < a.length) {
      aPadded = a;
      bPadded = padTail(b, a.length - b.length);
    } else {
      aPadded = a;
      bPadded = b;
    }
    if (compareTo(aPadded,bPadded) >= 0) {
      throw new IllegalArgumentException("b <= a");
    }
    if (num <= 0) {
      throw new IllegalArgumentException("num cannot be < 0");
    }
    byte [] prependHeader = {1, 0};
    final BigInteger startBI = new BigInteger(add(prependHeader, aPadded));
    final BigInteger stopBI = new BigInteger(add(prependHeader, bPadded));
    final BigInteger diffBI = stopBI.subtract(startBI);
    final BigInteger splitsBI = BigInteger.valueOf(num + 1);
    if(diffBI.compareTo(splitsBI) < 0) {
      return null;
    }
    final BigInteger intervalBI;
    try {
      intervalBI = diffBI.divide(splitsBI);
    } catch(Exception e) {
      LOG.error("Exception caught during division", e);
      return null;
    }

    final Iterator<byte[]> iterator = new Iterator<byte[]>() {
      private int i = -1;
      
      @Override
      public boolean hasNext() {
        return i < num+1;
      }

      @Override
      public byte[] next() {
        i++;
        if (i == 0) return a;
        if (i == num + 1) return b;
        
        BigInteger curBI = startBI.add(intervalBI.multiply(BigInteger.valueOf(i)));
        byte [] padded = curBI.toByteArray();
        if (padded[1] == 0)
          padded = tail(padded, padded.length - 2);
        else
          padded = tail(padded, padded.length - 1);
        return padded;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
    
    return new Iterable<byte[]>() {
      @Override
      public Iterator<byte[]> iterator() {
        return iterator;
      }
    };
  }

  /**
   * @param t operands
   * @return Array of byte arrays made from passed array of Text
   */
  public static byte [][] toByteArrays(final String [] t) {
    byte [][] result = new byte[t.length][];
    for (int i = 0; i < t.length; i++) {
      result[i] = Bytes.toBytes(t[i]);
    }
    return result;
  }

  /**
   * @param column operand
   * @return A byte array of a byte array where first and only entry is
   * <code>column</code>
   */
  public static byte [][] toByteArrays(final String column) {
    return toByteArrays(toBytes(column));
  }

  /**
   * @param column operand
   * @return A byte array of a byte array where first and only entry is
   * <code>column</code>
   */
  public static byte [][] toByteArrays(final byte [] column) {
    byte [][] result = new byte[1][];
    result[0] = column;
    return result;
  }

  /**
   * Binary search for keys in indexes.
   *
   * @param arr array of byte arrays to search for
   * @param key the key you want to find
   * @param offset the offset in the key you want to find
   * @param length the length of the key
   * @param comparator a comparator to compare.
   * @return zero-based index of the key, if the key is present in the array.
   *         Otherwise, a value -(i + 1) such that the key is between arr[i -
   *         1] and arr[i] non-inclusively, where i is in [0, i], if we define
   *         arr[-1] = -Inf and arr[N] = Inf for an N-element array. The above
   *         means that this function can return 2N + 1 different values
   *         ranging from -(N + 1) to N - 1.
   */
  public static int binarySearch(byte [][]arr, byte []key, int offset,
      int length, RawComparator<byte []> comparator) {
    int low = 0;
    int high = arr.length - 1;

    while (low <= high) {
      int mid = (low+high) >>> 1;
      // we have to compare in this order, because the comparator order
      // has special logic when the 'left side' is a special key.
      int cmp = comparator.compare(key, offset, length,
          arr[mid], 0, arr[mid].length);
      // key lives above the midpoint
      if (cmp > 0)
        low = mid + 1;
      // key lives below the midpoint
      else if (cmp < 0)
        high = mid - 1;
      // BAM. how often does this really happen?
      else
        return mid;
    }
    return - (low+1);
  }

  /**
   * Bytewise binary increment/deincrement of long contained in byte array
   * on given amount.
   *
   * @param value - array of bytes containing long (length <= SIZEOF_LONG)
   * @param amount value will be incremented on (deincremented if negative)
   * @return array of bytes containing incremented long (length == SIZEOF_LONG)
   */
  public static byte [] incrementBytes(byte[] value, long amount)
  {
    byte[] val = value;
    if (val.length < SIZEOF_LONG) {
      // Hopefully this doesn't happen too often.
      byte [] newvalue;
      if (val[0] < 0) {
        newvalue = new byte[]{-1, -1, -1, -1, -1, -1, -1, -1};
      } else {
        newvalue = new byte[SIZEOF_LONG];
      }
      System.arraycopy(val, 0, newvalue, newvalue.length - val.length,
        val.length);
      val = newvalue;
    } else if (val.length > SIZEOF_LONG) {
      throw new IllegalArgumentException("Increment Bytes - value too big: " +
        val.length);
    }
    if(amount == 0) return val;
    if(val[0] < 0){
      return binaryIncrementNeg(val, amount);
    }
    return binaryIncrementPos(val, amount);
  }

  /* increment/deincrement for positive value */
  private static byte [] binaryIncrementPos(byte [] value, long amount) {
    long amo = amount;
    int sign = 1;
    if (amount < 0) {
      amo = -amount;
      sign = -1;
    }
    for(int i=0;i<value.length;i++) {
      int cur = ((int)amo % 256) * sign;
      amo = (amo >> 8);
      int val = value[value.length-i-1] & 0x0ff;
      int total = val + cur;
      if(total > 255) {
        amo += sign;
        total %= 256;
      } else if (total < 0) {
        amo -= sign;
      }
      value[value.length-i-1] = (byte)total;
      if (amo == 0) return value;
    }
    return value;
  }

  /* increment/deincrement for negative value */
  private static byte [] binaryIncrementNeg(byte [] value, long amount) {
    long amo = amount;
    int sign = 1;
    if (amount < 0) {
      amo = -amount;
      sign = -1;
    }
    for(int i=0;i<value.length;i++) {
      int cur = ((int)amo % 256) * sign;
      amo = (amo >> 8);
      int val = ((~value[value.length-i-1]) & 0x0ff) + 1;
      int total = cur - val;
      if(total >= 0) {
        amo += sign;
      } else if (total < -256) {
        amo -= sign;
        total %= 256;
      }
      value[value.length-i-1] = (byte)total;
      if (amo == 0) return value;
    }
    return value;
  }

  /**
   * Writes a string as a fixed-size field, padded with zeros.
   */
  public static void writeStringFixedSize(final DataOutput out, String s,
      int size) throws IOException {
    byte[] b = toBytes(s);
    if (b.length > size) {
      throw new IOException("Trying to write " + b.length + " bytes (" +
          toStringBinary(b) + ") into a field of length " + size);
    }

    out.writeBytes(s);
    for (int i = 0; i < size - s.length(); ++i)
      out.writeByte(0);
  }

  /**
   * Reads a fixed-size field and interprets it as a string padded with zeros.
   */
  public static String readStringFixedSize(final DataInput in, int size) 
      throws IOException {
    byte[] b = new byte[size];
    in.readFully(b);
    int n = b.length;
    while (n > 0 && b[n - 1] == 0)
      --n;

    return toString(b, 0, n);
  }

  public static int readFully(InputStream is, byte[] buffer, int offset, int length)
      throws IOException {
    int nread = 0;
    while (nread < length) {
      int nbytes = is.read(buffer, offset + nread, length - nread);
      if (nbytes < 0) {
        return nread > 0 ? nread : nbytes;
      }
      nread += nbytes;
    }
    return nread;
  }

  /**
   * Similar to readFully(). Skips bytes in a loop.
   * @param in The DataInput to skip bytes from
   * @param len number of bytes to skip.
   * @throws IOException if it could not skip requested number of bytes
   * for any reason (including EOF)
   */
  public static void skipFully(DataInput in, int len) throws IOException {
    int amt = len;
    while (amt > 0) {
      long ret = in.skipBytes(amt);
      if (ret == 0) {
        // skip may return 0 even if we're not at EOF.  Luckily, we can
        // use the read() method to figure out if we're at the end.
        int b = in.readByte();
        if (b == -1) {
          throw new EOFException( "Premature EOF from inputStream after " +
              "skipping " + (len - amt) + " byte(s).");
        }
        ret = 1;
      }
      amt -= ret;
    }
  }

  /**
   * Parses the byte array argument as if it was an int value and returns the
   * result. Throws NumberFormatException if the byte array does not represent an
   * int quantity.
   *
   * @return int the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an int quantity.
   */
  public static int parseInt(byte[] bytes, int start, int length) {
    return parseInt(bytes, start, length, 10);
  }

  /**
   * Parses the byte array argument as if it was an int value and returns the
   * result. Throws NumberFormatException if the byte array does not represent an
   * int quantity. The second argument specifies the radix to use when parsing
   * the value.
   *
   * @param radix  the base to use for conversion.
   * @return the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an int quantity.
   */
  public static int parseInt(byte[] bytes, int start, int length, int radix) {
    if (bytes == null) {
      throw new NumberFormatException("String is null");
    }
    if (radix < Character.MIN_RADIX || radix > Character.MAX_RADIX) {
      throw new NumberFormatException("Invalid radix: " + radix);
    }
    if (length == 0) {
      throw new NumberFormatException("Empty byte array!");
    }
    int offset = start;
    boolean negative = bytes[start] == '-';
    if (negative || bytes[start] == '+') {
      offset++;
      if (length == 1) {
        throw new NumberFormatException(new String(bytes, start,
            length));
      }
    }

    return parse(bytes, start, length, offset, radix, negative);
  }

  /**
   * @param bytes
   * @param start
   * @param length
   * @param radix    the base to use for conversion.
   * @param offset   the starting position after the sign (if exists)
   * @param radix    the base to use for conversion.
   * @param negative whether the number is negative.
   * @return the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an int quantity.
   */
  private static int parse(byte[] bytes, int start, int length, int offset,
                           int radix, boolean negative) {
    byte separator = '.';
    int max = Integer.MIN_VALUE / radix;
    int result = 0, end = start + length;
    while (offset < end) {
      int digit = digit(bytes[offset++], radix);
      if (digit == -1) {
        if (bytes[offset - 1] == separator) {
          // We allow decimals and will return a truncated integer in that case.
          // Therefore we won't throw an exception here (checking the fractional
          // part happens below.)
          break;
        }
        throw new NumberFormatException(new String(bytes, start,
            length));
      }
      if (max > result) {
        throw new NumberFormatException(new String(bytes, start,
            length));
      }
      int next = result * radix - digit;
      if (next > result) {
        throw new NumberFormatException(new String(bytes, start,
            length));
      }
      result = next;
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well formed.
    while (offset < end) {
      int digit = digit(bytes[offset++], radix);
      if (digit == -1) {
        throw new NumberFormatException(new String(bytes, start,
            length));
      }
    }

    if (!negative) {
      result = -result;
      if (result < 0) {
        throw new NumberFormatException(new String(bytes, start,
            length));
      }
    }
    return result;
  }


  /**
   * Returns the digit represented by character b.
   *
   * @param b     The ascii code of the character
   * @param radix The radix
   * @return -1 if it's invalid
   */
  private static int digit(int b, int radix) {
    int r = -1;
    if (b >= '0' && b <= '9') {
      r = b - '0';
    } else if (b >= 'A' && b <= 'Z') {
      r = b - 'A' + 10;
    } else if (b >= 'a' && b <= 'z') {
      r = b - 'a' + 10;
    }
    if (r >= radix) {
      r = -1;
    }
    return r;
  }

  /**
   * Returns the digit represented by character b, radix is 10
   *
   * @param b The ascii code of the character
   * @return -1 if it's invalid
   */
  private static boolean isDigit(int b) {
    return (b >= '0' && b <= '9');
  }

  private static final int maxExponent = 511;	/* Largest possible base 10 exponent.  Any
                                               * exponent larger than this will already
                                               * produce underflow or overflow, so there's
                                               * no need to worry about additional digits.
                                               */
  public static final double powersOf10[] = {	/* Table giving binary powers of 10.  Entry */
      10.,		 	                              /* is 10^2^i.  Used to convert decimal */
      100.,		 	                              /* exponents into floating-point numbers. */
      1.0e4,
      1.0e8,
      1.0e16,
      1.0e32,
      1.0e64,
      1.0e128,
      1.0e256
  };

  /**
   * Parses the byte array argument as if it was a double value and returns the
   * result. Throws NumberFormatException if the byte array does not represent a
   * double value.
   *
   * @return double, the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as a double
   */
  public static double parseDouble(byte[] bytes, int start, int length) {
    if (bytes == null) {
      throw new NumberFormatException("String is null");
    }
    if (length == 0) {
      throw new NumberFormatException("Empty byte array!");
    }

    /*
     * Strip off leading blanks
     */
    int offset = start;
    int end = start + length;

    while (offset < end && bytes[offset] == ' ') {
      offset++;
    }
    if (offset == end) {
      throw new NumberFormatException("blank byte array!");
    }

    /*
     * check for a sign.
     */
    boolean sign = false;
    if (bytes[offset] == '-') {
      sign = true;
      offset++;
    } else if (bytes[offset] == '+') {
      offset++;
    }
    if (offset == end) {
      throw new NumberFormatException("the byte array only has a sign!");
    }

    /*
     * Count the number of digits in the mantissa (including the decimal
     * point), and also locate the decimal point.
     */
    int mantSize = 0;		      /* Number of digits in mantissa. */
    int decicalOffset = -1;   /* Number of mantissa digits BEFORE decimal point. */
    for (; offset < end; offset++) {
      if (!isDigit(bytes[offset])) {
        if ((bytes[offset] != '.') || (decicalOffset >= 0)) {
          break;
        }
        decicalOffset = mantSize;
      }
      mantSize++;
    }

    int exponentOffset = offset; /* Temporarily holds location of exponent in bytes. */

    /*
     * Now suck up the digits in the mantissa.  Use two integers to
     * collect 9 digits each (this is faster than using floating-point).
     * If the mantissa has more than 18 digits, ignore the extras, since
     * they can't affect the value anyway.
     */
    offset -= mantSize;
    if (decicalOffset < 0) {
      decicalOffset = mantSize;
    } else {
      mantSize -= 1;			       /* One of the digits was the decimal point. */
    }
    int fracExponent;            /* Exponent that derives from the fractional
                                  * part.  Under normal circumstatnces, it is
				                          * the negative of the number of digits in F.
				                          * However, if I is very long, the last digits
				                          * of I get dropped (otherwise a long I with a
				                          * large negative exponent could cause an
				                          * unnecessary overflow on I alone).  In this
				                          * case, fracExp is incremented one for each
				                          * dropped digit. */
    if (mantSize > 18) {
      fracExponent = decicalOffset - 18;
      mantSize = 18;
    } else {
      fracExponent = decicalOffset - mantSize;
    }

    if (mantSize == 0) {
      return 0.0;
    }

    int frac1 = 0;
    for (; mantSize > 9; mantSize--) {
      int b = bytes[offset];
      offset++;
      if (b == '.') {
        b = bytes[offset];
        offset++;
      }
      frac1 = 10 * frac1 + (b - '0');
    }
    int frac2 = 0;
    for (; mantSize > 0; mantSize--) {
      int b = bytes[offset];
      offset++;
      if (b == '.') {
        b = bytes[offset];
        offset++;
      }
      frac2 = 10 * frac2 + (b - '0');
    }
    double fraction = (1.0e9 * frac1) + frac2;

    /*
     * Skim off the exponent.
     */
    int exponent = 0;            /* Exponent read from "EX" field. */
    offset = exponentOffset;
    boolean expSign = false;

    if (offset < end) {
      if ((bytes[offset] != 'E') && (bytes[offset] != 'e')) {
        throw new NumberFormatException(new String(bytes, start,
            length));
      }

      // (bytes[offset] == 'E') || (bytes[offset] == 'e')
      offset++;

      if (bytes[offset] == '-') {
        expSign = true;
        offset++;
      } else if (bytes[offset] == '+') {
        offset++;
      }

      for (; offset < end; offset++) {
        if (isDigit(bytes[offset])) {
          exponent = exponent * 10 + (bytes[offset] - '0');
        } else {
          throw new NumberFormatException(new String(bytes, start,
              length));
        }
      }
    }

    exponent = expSign ? (fracExponent - exponent) : (fracExponent + exponent);

    /*
     * Generate a floating-point number that represents the exponent.
     * Do this by processing the exponent one bit at a time to combine
     * many powers of 2 of 10. Then combine the exponent with the
     * fraction.
     */
    if (exponent < 0) {
      expSign = true;
      exponent = -exponent;
    } else {
      expSign = false;
    }
    if (exponent > maxExponent) {
      throw new NumberFormatException(new String(bytes, start,
          length));
    }

    double dblExp = 1.0;
    for (int i = 0; exponent != 0; exponent >>= 1, i++) {
      if ((exponent & 01) == 01) {
        dblExp *= powersOf10[i];
      }
    }

    fraction = (expSign) ? (fraction / dblExp) : (fraction * dblExp);

    return sign ? (-fraction) : fraction;
  }

}
