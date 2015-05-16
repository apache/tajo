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

import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;

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

  public static long mergeToLong(int value1, int value2) {
    return (long)value1 << 32 | value2 & 0xffffffffl;
  }

  public static int toHighInt(long value) {
    return (int)(value >> 32);
  }

  public static int toLowInt(long value) {
    return (int)value;
  }

  public static class PrimitiveLongs implements Iterable<Long> {
    int index;
    long[] longArray;

    public PrimitiveLongs(int initLength) {
      longArray = new long[initLength];
    }
    public void add(long value) {
      reserve(1)[index++] = value;
    }
    public void add(long[] value) {
      System.arraycopy(value, 0, reserve(value.length), index, value.length);
      index += value.length;
    }
    public long[] backingArray() {
      return longArray;
    }
    public long[] toArray() {
      return Arrays.copyOfRange(longArray, 0, index);
    }
    public int size() {
      return index;
    }
    private long[] reserve(int reserve) {
      if (index + reserve < longArray.length) {
        return longArray;
      }
      int newLength = Math.max(index + reserve, longArray.length << 1);
      long[] newLongArray = new long[newLength];
      System.arraycopy(longArray, 0, newLongArray, 0, index);
      return longArray = newLongArray;
    }

    @Override
    public Iterator<Long> iterator() {
      return Longs.asList(toArray()).iterator();
    }
  }
}
