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

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBytes {

  @Test
  public void testAsciiBytes() {
    String asciiText = "abcde 12345 ABCDE";
    assertArrayEquals(asciiText.getBytes(), Bytes.toASCIIBytes(asciiText.toCharArray()));
  }

  @Test
  public void testSplitBytes() {
    String text = "abcde|12345|ABCDE";
    char separatorChar = '|';

    String[] textArray = StringUtils.splitPreserveAllTokens(text, separatorChar);
    byte[][] bytesArray =  Bytes.splitPreserveAllTokens(text.getBytes(), separatorChar);

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

    String[] textArray = StringUtils.splitPreserveAllTokens(text, separatorChar);
    byte[][] bytesArray =  Bytes.splitPreserveAllTokens(text.getBytes(), separatorChar, target);

    assertEquals(textArray.length, bytesArray.length);

    assertNull(bytesArray[0]);
    assertNotNull(bytesArray[1]);
    assertArrayEquals(textArray[1].getBytes(), bytesArray[1]);
    assertNull(bytesArray[2]);
  }
}
