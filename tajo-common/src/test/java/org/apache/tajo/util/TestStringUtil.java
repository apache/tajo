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

import java.nio.charset.Charset;

import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestStringUtil {

  @Test
  public void testUnicodeEscapedDelimiter() {
    for (int i = 0; i < 128; i++) {
      char c = (char) i;
      String delimiter = CharUtils.unicodeEscaped(c);
      String escapedDelimiter = StringUtils.unicodeEscapedDelimiter(delimiter);
      assertEquals(delimiter, escapedDelimiter);
      assertEquals(1, StringEscapeUtils.unescapeJava(escapedDelimiter).length());
      assertEquals(c, StringEscapeUtils.unescapeJava(escapedDelimiter).charAt(0));
    }
  }

  @Test
  public void testUnescapedDelimiter() {
    for (int i = 0; i < 128; i++) {
      char c = (char) i;
      String delimiter = String.valueOf(c);
      String escapedDelimiter = StringUtils.unicodeEscapedDelimiter(delimiter);
      assertEquals(CharUtils.unicodeEscaped(c), escapedDelimiter);
      assertEquals(1, StringEscapeUtils.unescapeJava(escapedDelimiter).length());
      assertEquals(c, StringEscapeUtils.unescapeJava(escapedDelimiter).charAt(0));
    }
  }

  @Test
  public void testVariousDelimiter() {
    /*
    * Character         ASCII    Unicode
    *
    * Horizontal tab    9        <U0009>
    * Space Bar         32       <U0020>
    * 1                 49       <U0031>
    * |                 124      <U007c>
    *
    * */


    String escapedDelimiter = "\\u0031";

    assertEquals(escapedDelimiter, StringUtils.unicodeEscapedDelimiter("1"));
    assertEquals(escapedDelimiter, StringUtils.unicodeEscapedDelimiter("\\1"));
    assertEquals(escapedDelimiter, StringUtils.unicodeEscapedDelimiter("\\u0031"));
    assertEquals(escapedDelimiter, StringUtils.unicodeEscapedDelimiter((char)49));
    assertNotEquals(escapedDelimiter, StringUtils.unicodeEscapedDelimiter('\001'));

    String delimiter = "|";
    assertEquals("\\u007c", StringUtils.unicodeEscapedDelimiter(delimiter));
    assertEquals(delimiter, StringEscapeUtils.unescapeJava(StringUtils.unicodeEscapedDelimiter(delimiter)));


    String commaDelimiter = ",";
    assertEquals("\\u002c", StringUtils.unicodeEscapedDelimiter(commaDelimiter));
    assertEquals(commaDelimiter, StringEscapeUtils.unescapeJava(StringUtils.unicodeEscapedDelimiter(commaDelimiter)));

    String tabDelimiter = "\t";
    assertEquals("\\u0009", StringUtils.unicodeEscapedDelimiter(tabDelimiter));
    assertEquals(tabDelimiter, StringEscapeUtils.unescapeJava(StringUtils.unicodeEscapedDelimiter(tabDelimiter)));

    String spaceDelimiter = " ";
    assertEquals("\\u0020", StringUtils.unicodeEscapedDelimiter(spaceDelimiter));
    assertEquals(spaceDelimiter, StringEscapeUtils.unescapeJava(StringUtils.unicodeEscapedDelimiter(spaceDelimiter)));
  }

  @Test
  public void testAsciiBytes() {
    String asciiText = "abcde 12345 ABCDE";
    assertArrayEquals(asciiText.getBytes(), BytesUtils.toASCIIBytes(asciiText.toCharArray()));
  }

  @Test
  public void testSplitBytes() {
    String text = "abcde|12345|ABCDE";
    char separatorChar = '|';

    String[] textArray = org.apache.commons.lang.StringUtils.splitPreserveAllTokens(text, separatorChar);
    byte[][] bytesArray =  BytesUtils.splitPreserveAllTokens(text.getBytes(), separatorChar, 3);

    assertEquals(textArray.length, bytesArray.length);
    for (int i = 0; i < textArray.length; i++){
      assertArrayEquals(textArray[i].getBytes(), bytesArray[i]);
    }
  }

  @Test
  public void testSplitProjectionBytes() {
    String text = "abcde|12345|ABCDE";
    int[] target = new int[]{ 1 };
    char separatorChar = '|';

    String[] textArray = org.apache.commons.lang.StringUtils.splitPreserveAllTokens(text, separatorChar);
    byte[][] bytesArray =  BytesUtils.splitPreserveAllTokens(text.getBytes(), separatorChar, target, 3);

    assertEquals(textArray.length, bytesArray.length);

    assertNull(bytesArray[0]);
    assertNotNull(bytesArray[1]);
    assertArrayEquals(textArray[1].getBytes(), bytesArray[1]);
    assertNull(bytesArray[2]);
  }
  
  @Test
  public void testConvertBytesToChars() {
    byte[] testStringArray = new byte[] { 0x74, 0x61, 0x6A, 0x6F };
    assertArrayEquals(new char[] { 't', 'a', 'j', 'o' },
        StringUtils.convertBytesToChars(testStringArray, Charset.forName("UTF-8")));

    testStringArray = new byte[] { (byte) 0xED, (byte) 0x83, (byte) 0x80, (byte) 0xEC, (byte) 0xA1, (byte) 0xB0 };
    assertArrayEquals(new char[] { '\ud0c0', '\uc870' },
        StringUtils.convertBytesToChars(testStringArray, Charset.forName("UTF-8")));
  }
  
  @Test
  public void testConvertCharsToBytes() {
    char[] testStringArray = new char[] { 't', 'a', 'j', 'o' };
    assertArrayEquals(new byte[] { 0x74, 0x61, 0x6A, 0x6F },
        StringUtils.convertCharsToBytes(testStringArray, Charset.forName("UTF-8")));

    testStringArray = new char[] { '\ud0c0', '\uc870' };
    assertArrayEquals(new byte[] { (byte) 0xED, (byte) 0x83, (byte) 0x80, (byte) 0xEC, (byte) 0xA1, (byte) 0xB0 },
        StringUtils.convertCharsToBytes(testStringArray, Charset.forName("UTF-8")));
  }
}
