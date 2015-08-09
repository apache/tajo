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

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

// this is an implementation copied from LazyPrimitives in hive
public class NumberUtil {

  public static final double[] powersOf10 = {	/* Table giving binary powers of 10.  Entry */
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
  private static final int maxExponent = 511;	/* Largest possible base 10 exponent.  Any
                                               * exponent larger than this will already
                                               * produce underflow or overflow, so there's
                                               * no need to worry about additional digits.
                                               */

  /** When we encode strings, we always specify UTF8 encoding */
  public static final String UTF8_ENCODING = "UTF-8";

  /** When we encode strings, we always specify UTF8 encoding */
  public static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);

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

  public static long unsigned32(int n) {
    return n & 0xFFFFFFFFL;
  }

  public static int unsigned16(short n) {
    return n & 0xFFFF;
  }

  public static byte[] toAsciiBytes(Number i) {
    return BytesUtils.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  public static byte[] toAsciiBytes(short i) {
    return BytesUtils.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  public static byte[] toAsciiBytes(int i) {
    return BytesUtils.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  public static byte[] toAsciiBytes(long i) {
    return BytesUtils.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  public static byte[] toAsciiBytes(float i) {
    return BytesUtils.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  public static byte[] toAsciiBytes(double i) {
    return BytesUtils.toASCIIBytes(String.valueOf(i).toCharArray());
  }

  /**
   * Returns the digit represented by character b.
   *
   * @param b     The ascii code of the character
   * @param radix The radix
   * @return -1 if it's invalid
   */
  static int digit(int b, int radix) {
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
   * @param radix the base to use for conversion.
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

    return parseIntInternal(bytes, start, length, offset, radix, negative);
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
  private static int parseIntInternal(byte[] bytes, int start, int length, int offset,
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
   * Parses the string argument as if it was a long value and returns the
   * result. Throws NumberFormatException if the string does not represent a
   * long quantity.
   *
   * @param bytes
   * @param start
   * @param length a UTF-8 encoded string representation of a long quantity.
   * @return long the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as a long quantity.
   */
  public static long parseLong(byte[] bytes, int start, int length) {
    return parseLong(bytes, start, length, 10);
  }

  /**
   * Parses the string argument as if it was an long value and returns the
   * result. Throws NumberFormatException if the string does not represent an
   * long quantity. The second argument specifies the radix to use when parsing
   * the value.
   *
   * @param bytes
   * @param start
   * @param length a UTF-8 encoded string representation of a long quantity.
   * @param radix  the base to use for conversion.
   * @return the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an long quantity.
   */
  public static long parseLong(byte[] bytes, int start, int length, int radix) {
    if (bytes == null) {
      throw new NumberFormatException("String is null");
    }
    if (radix < Character.MIN_RADIX || radix > Character.MAX_RADIX) {
      throw new NumberFormatException("Invalid radix: " + radix);
    }
    if (length == 0) {
      throw new NumberFormatException("Empty string!");
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

    return parseLongInternal(bytes, start, length, offset, radix, negative);
  }

  /**
   * /** Parses the string argument as if it was an long value and returns the
   * result. Throws NumberFormatException if the string does not represent an
   * long quantity. The second argument specifies the radix to use when parsing
   * the value.
   *
   * @param bytes
   * @param start
   * @param length   a UTF-8 encoded string representation of a long quantity.
   * @param offset   the starting position after the sign (if exists)
   * @param radix    the base to use for conversion.
   * @param negative whether the number is negative.
   * @return the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an long quantity.
   */
  private static long parseLongInternal(byte[] bytes, int start, int length, int offset,
                                        int radix, boolean negative) {
    byte separator = '.';
    long max = Long.MIN_VALUE / radix;
    long result = 0, end = start + length;
    while (offset < end) {
      int digit = digit(bytes[offset++], radix);
      if (digit == -1 || max > result) {
        if (bytes[offset - 1] == separator) {
          // We allow decimals and will return a truncated integer in that case.
          // Therefore we won't throw an exception here (checking the fractional
          // part happens below.)
          break;
        }
        throw new NumberFormatException(new String(bytes, start,
            length));
      }
      long next = result * radix - digit;
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
   * Writes out the text representation of an integer using base 10 to an
   * OutputStream in UTF-8 encoding.
   * <p/>
   * Note: division by a constant (like 10) is much faster than division by a
   * variable. That's one of the reasons that we don't make radix a parameter
   * here.
   *
   * @param out the outputstream to write to
   * @param i   an int to write out
   * @throws IOException
   */
  public static void writeUTF8(OutputStream out, int i) throws IOException {
    if (i == 0) {
      out.write('0');
      return;
    }

    boolean negative = i < 0;
    if (negative) {
      out.write('-');
    } else {
      // negative range is bigger than positive range, so there is no risk
      // of overflow here.
      i = -i;
    }

    int start = 1000000000;
    while (i / start == 0) {
      start /= 10;
    }

    while (start > 0) {
      out.write('0' - (i / start % 10));
      start /= 10;
    }
  }

  /**
   * Writes out the text representation of an integer using base 10 to an
   * OutputStream in UTF-8 encoding.
   * <p/>
   * Note: division by a constant (like 10) is much faster than division by a
   * variable. That's one of the reasons that we don't make radix a parameter
   * here.
   *
   * @param out the outputstream to write to
   * @param i   an int to write out
   * @throws java.io.IOException
   */
  public static void writeUTF8(OutputStream out, long i) throws IOException {
    if (i == 0) {
      out.write('0');
      return;
    }

    boolean negative = i < 0;
    if (negative) {
      out.write('-');
    } else {
      // negative range is bigger than positive range, so there is no risk
      // of overflow here.
      i = -i;
    }

    long start = 1000000000000000000L;
    while (i / start == 0) {
      start /= 10;
    }

    while (start > 0) {
      out.write('0' - (int) ((i / start) % 10));
      start /= 10;
    }
  }

  /**
   * Parses the byte array argument as if it was a double value and returns the
   * result. Throws NumberFormatException if the byte buffer does not represent a
   * double value.
   *
   * @return double, the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as a double
   */
  public static double parseDouble(ByteBuf bytes) {
    return parseDouble(bytes, bytes.readerIndex(), bytes.readableBytes());
  }

  /**
   * Parses the byte array argument as if it was a double value and returns the
   * result. Throws NumberFormatException if the byte buffer does not represent a
   * double value.
   *
   * @return double, the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as a double
   */
  public static double parseDouble(ByteBuf bytes, int start, int length) {
    if (bytes == null) {
      throw new NumberFormatException("String is null");
    }

    if (!bytes.hasMemoryAddress()) {
      return parseDouble(bytes.array(), start, length);
    }

    if (length == 0 || bytes.writerIndex() < start + length) {
      throw new NumberFormatException("Empty string or Invalid buffer!");
    }


    long memoryAddress = bytes.memoryAddress();
    /*
     * Strip off leading blanks
     */
    int offset = start;
    int end = start + length;

    while (offset < end && PlatformDependent.getByte(memoryAddress + offset) == ' ') {
      offset++;
    }
    if (offset == end) {
      throw new NumberFormatException("blank byte array!");
    }

    /*
     * check for a sign.
     */
    boolean sign = false;
    if (PlatformDependent.getByte(memoryAddress + offset) == '-') {
      sign = true;
      offset++;
    } else if (PlatformDependent.getByte(memoryAddress + offset) == '+') {
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
      if (!isDigit(PlatformDependent.getByte(memoryAddress + offset))) {
        if ((PlatformDependent.getByte(memoryAddress + offset) != '.') || (decicalOffset >= 0)) {
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
      int b = PlatformDependent.getByte(memoryAddress + offset);
      offset++;
      if (b == '.') {
        b = PlatformDependent.getByte(memoryAddress + offset);
        offset++;
      }
      frac1 = 10 * frac1 + (b - '0');
    }
    int frac2 = 0;
    for (; mantSize > 0; mantSize--) {
      int b = PlatformDependent.getByte(memoryAddress + offset);
      offset++;
      if (b == '.') {
        b = PlatformDependent.getByte(memoryAddress + offset);
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
      if ((PlatformDependent.getByte(memoryAddress + offset) != 'E')
          && (PlatformDependent.getByte(memoryAddress + offset) != 'e')) {
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }

      // (bytes[offset] == 'E') || (bytes[offset] == 'e')
      offset++;

      if (PlatformDependent.getByte(memoryAddress + offset) == '-') {
        expSign = true;
        offset++;
      } else if (PlatformDependent.getByte(memoryAddress + offset) == '+') {
        offset++;
      }

      for (; offset < end; offset++) {
        if (isDigit(PlatformDependent.getByte(memoryAddress + offset))) {
          exponent = exponent * 10 + (PlatformDependent.getByte(memoryAddress + offset) - '0');
        } else {
          throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
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
      throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
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


  /**
   * Parses the byte buffer argument as if it was an int value and returns the
   * result. Throws NumberFormatException if the byte array does not represent an
   * int quantity.
   *
   * @return int the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an int quantity.
   */
  public static int parseInt(ByteBuf bytes) {
    return parseInt(bytes, bytes.readerIndex(), bytes.readableBytes());
  }

  /**
   * Parses the byte buffer argument as if it was an int value and returns the
   * result. Throws NumberFormatException if the byte array does not represent an
   * int quantity.
   *
   * @return int the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an int quantity.
   */
  public static int parseInt(ByteBuf bytes, int start, int length) {
    return parseInt(bytes, start, length, 10);
  }

  /**
   * Parses the byte buffer argument as if it was an int value and returns the
   * result. Throws NumberFormatException if the byte array does not represent an
   * int quantity. The second argument specifies the radix to use when parsing
   * the value.
   *
   * @param radix the base to use for conversion.
   * @return the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an int quantity.
   */
  public static int parseInt(ByteBuf bytes, int start, int length, int radix) {
    if (bytes == null) {
      throw new NumberFormatException("String is null");
    }

    if (!bytes.hasMemoryAddress()) {
      return parseInt(bytes.array(), start, length);
    }

    if (radix < Character.MIN_RADIX || radix > Character.MAX_RADIX) {
      throw new NumberFormatException("Invalid radix: " + radix);
    }
    if (length == 0 || bytes.writerIndex() < start + length) {
      throw new NumberFormatException("Empty string or Invalid buffer!");
    }

    long memoryAddress = bytes.memoryAddress();

    int offset = start;
    boolean negative = PlatformDependent.getByte(memoryAddress + start) == '-';
    if (negative || PlatformDependent.getByte(memoryAddress + start) == '+') {
      offset++;
      if (length == 1) {
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
    }

    return parseIntInternal(bytes, memoryAddress, start, length, offset, radix, negative);
  }

  /**
   * @param bytes         the string byte buffer
   * @param memoryAddress the offheap memory address
   * @param start
   * @param length
   * @param radix         the base to use for conversion.
   * @param offset        the starting position after the sign (if exists)
   * @param radix         the base to use for conversion.
   * @param negative      whether the number is negative.
   * @return the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an int quantity.
   */
  private static int parseIntInternal(ByteBuf bytes, long memoryAddress, int start, int length, int offset,
                                      int radix, boolean negative) {
    byte separator = '.';
    int max = Integer.MIN_VALUE / radix;
    int result = 0, end = start + length;
    while (offset < end) {
      int digit = digit(PlatformDependent.getByte(memoryAddress + offset++), radix);
      if (digit == -1) {
        if (PlatformDependent.getByte(memoryAddress + offset - 1) == separator) {
          // We allow decimals and will return a truncated integer in that case.
          // Therefore we won't throw an exception here (checking the fractional
          // part happens below.)
          break;
        }
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
      if (max > result) {
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
      int next = result * radix - digit;
      if (next > result) {
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
      result = next;
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well formed.
    while (offset < end) {
      int digit = digit(PlatformDependent.getByte(memoryAddress + offset++), radix);
      if (digit == -1) {
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
    }

    if (!negative) {
      result = -result;
      if (result < 0) {
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
    }
    return result;
  }

  /**
   * Parses the byte buffer argument as if it was a long value and returns the
   * result. Throws NumberFormatException if the string does not represent a
   * long quantity.
   *
   * @param bytes the string byte buffer
   * @return long the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as a long quantity.
   */
  public static long parseLong(ByteBuf bytes) {
    return parseLong(bytes, bytes.readerIndex(), bytes.readableBytes());
  }

  /**
   * Parses the byte buffer argument as if it was a long value and returns the
   * result. Throws NumberFormatException if the string does not represent a
   * long quantity.
   *
   * @param bytes  the string byte buffer
   * @param start
   * @param length a UTF-8 encoded string representation of a long quantity.
   * @return long the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as a long quantity.
   */
  public static long parseLong(ByteBuf bytes, int start, int length) {
    return parseLong(bytes, start, length, 10);
  }

  /**
   * Parses the byte buffer argument as if it was an long value and returns the
   * result. Throws NumberFormatException if the string does not represent an
   * long quantity. The second argument specifies the radix to use when parsing
   * the value.
   *
   * @param bytes  the string byte buffer
   * @param start
   * @param length a UTF-8 encoded string representation of a long quantity.
   * @param radix  the base to use for conversion.
   * @return the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an long quantity.
   */
  public static long parseLong(ByteBuf bytes, int start, int length, int radix) {
    if (bytes == null) {
      throw new NumberFormatException("String is null");
    }

    if (!bytes.hasMemoryAddress()) {
      return parseInt(bytes.array(), start, length);
    }

    if (radix < Character.MIN_RADIX || radix > Character.MAX_RADIX) {
      throw new NumberFormatException("Invalid radix: " + radix);
    }
    if (length == 0 || bytes.writerIndex() < start + length) {
      throw new NumberFormatException("Empty string or Invalid buffer!");
    }

    long memoryAddress = bytes.memoryAddress();

    int offset = start;
    boolean negative = PlatformDependent.getByte(memoryAddress + start) == '-';
    if (negative || PlatformDependent.getByte(memoryAddress + start) == '+') {
      offset++;
      if (length == 1) {
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
    }

    return parseLongInternal(bytes, memoryAddress, start, length, offset, radix, negative);
  }

  /**
   * /** Parses the byte buffer argument as if it was an long value and returns the
   * result. Throws NumberFormatException if the string does not represent an
   * long quantity. The second argument specifies the radix to use when parsing
   * the value.
   *
   * @param bytes         the string byte buffer
   * @param memoryAddress the offheap memory address
   * @param start
   * @param length        a UTF-8 encoded string representation of a long quantity.
   * @param offset        the starting position after the sign (if exists)
   * @param radix         the base to use for conversion.
   * @param negative      whether the number is negative.
   * @return the value represented by the argument
   * @throws NumberFormatException if the argument could not be parsed as an long quantity.
   */
  private static long parseLongInternal(ByteBuf bytes, long memoryAddress, int start, int length, int offset,
                                        int radix, boolean negative) {
    byte separator = '.';
    long max = Long.MIN_VALUE / radix;
    long result = 0, end = start + length;
    while (offset < end) {
      int digit = digit(PlatformDependent.getByte(memoryAddress + offset++), radix);
      if (digit == -1 || max > result) {
        if (PlatformDependent.getByte(memoryAddress + offset - 1) == separator) {
          // We allow decimals and will return a truncated integer in that case.
          // Therefore we won't throw an exception here (checking the fractional
          // part happens below.)
          break;
        }
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
      long next = result * radix - digit;
      if (next > result) {
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
      result = next;
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well formed.
    while (offset < end) {
      int digit = digit(PlatformDependent.getByte(memoryAddress + offset++), radix);
      if (digit == -1) {
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
    }

    if (!negative) {
      result = -result;
      if (result < 0) {
        throw new NumberFormatException(bytes.toString(start, length, Charset.defaultCharset()));
      }
    }
    return result;
  }

  public static Number numberValue(Class<?> numberClazz, String value) {
    Number returnNumber = null;

    if (numberClazz == null && value == null) {
      return returnNumber;
    }

    if (Number.class.isAssignableFrom(numberClazz)) {
        try {
          Constructor<?> constructor = numberClazz.getConstructor(String.class);
          returnNumber = (Number) constructor.newInstance(value);
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception ignored) {
        }
      
    }

    return returnNumber;
  }

  public static int compare(long x, long y) {
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
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
   * Returns a new byte array, copied from the given {@code buf},
   * from the index 0 (inclusive) to the limit (exclusive),
   * regardless of the current position.
   * The position and the other index parameters are not changed.
   *
   * @param buf a byte buffer
   * @return the byte array
   * @see #getBytes(ByteBuffer)
   */
  public static byte[] toBytes(ByteBuffer buf) {
    ByteBuffer dup = buf.duplicate();
    dup.position(0);
    return readBytes(dup);
  }

  private static byte[] readBytes(ByteBuffer buf) {
    byte [] result = new byte[buf.remaining()];
    buf.get(result);
    return result;
  }

  /**
   * Converts a string to a UTF-8 byte array.
   * @param s string
   * @return the byte array
   */
  public static byte[] toBytes(String s) {
    return s.getBytes(UTF8_CHARSET);
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
   * Convert an int value to a byte array.  Big-endian.  Same as what DataOutputStream.writeInt
   * does.
   *
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
   * Converts a byte array to an int value
   * @param bytes byte array
   * @param offset offset into array
   * @param length how many bytes should be considered for creating int
   * @return the int value
   * @throws IllegalArgumentException if there's not enough room in the array at the offset
   * indicated.
   */
  public static int readAsInt(byte[] bytes, int offset, final int length) {
    if (offset + length > bytes.length) {
      throw new IllegalArgumentException("offset (" + offset + ") + length (" + length
          + ") exceed the" + " capacity of the array: " + bytes.length);
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
   * Returns a new byte array, copied from the given {@code buf},
   * from the position (inclusive) to the limit (exclusive).
   * The position and the other index parameters are not changed.
   *
   * @param buf a byte buffer
   * @return the byte array
   * @see #toBytes(ByteBuffer)
   */
  public static byte[] getBytes(ByteBuffer buf) {
    return readBytes(buf.duplicate());
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
   * Put an int value as short out to the specified byte array position. Only the lower 2 bytes of
   * the short will be put into the array. The caller of the API need to make sure they will not
   * loose the value by doing so. This is useful to store an unsigned short which is represented as
   * int in other parts.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val value to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putAsShort(byte[] bytes, int offset, int val) {
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
   * @param length
   * @return the char value
   */
  public static BigDecimal toBigDecimal(byte[] bytes, int offset, final int length) {
    if (bytes == null || length < SIZEOF_INT + 1 ||
        (offset + length > bytes.length)) {
      return null;
    }

    int scale = toInt(bytes, offset);
    byte[] tcBytes = new byte[length - SIZEOF_INT];
    System.arraycopy(bytes, offset + SIZEOF_INT, tcBytes, 0, length - SIZEOF_INT);
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
}
