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

package org.apache.tajo.datum;

import org.apache.tajo.type.Type;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.nio.charset.Charset;

import static org.junit.Assert.*;

public class TestTextDatum {

	@Test
	public final void testAsInt4() {
		Datum d = DatumFactory.createText("12345");
		assertEquals(12345,d.asInt4());
	}

	@Test
	public final void testAsInt8() {
		Datum d = DatumFactory.createText("12345");
		assertEquals(12345l,d.asInt8());
	}

	@Test
	public final void testAsFloat4() {
		Datum d = DatumFactory.createText("12345");
		assertTrue(12345.0f == d.asFloat4());
	}

	@Test
	public final void testAsFloat8() {
		Datum d = DatumFactory.createText("12345");
		assertTrue(12345.0d == d.asFloat8());
	}

	@Test
	public final void testAsText() {
		Datum d = DatumFactory.createText("12345");
		assertEquals("12345", d.asChars());
	}

	@Test
  public final void testSize() {
	  Datum d = DatumFactory.createText("12345");
    assertEquals(5, d.size());
  }

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createText("12345");
    assertArrayEquals(d.asByteArray(), d.asTextBytes());
  }

  @Test
  public final void testTextEncoding() {
    String text = "나랏말싸미 듕귁에 달아 문자와로 서르 사맛디 아니할쎄";
    TextDatum test = new TextDatum(text);

    TextDatum fromUTF8 = new TextDatum(text.getBytes(Charset.forName("UTF-8")));
    assertEquals(test, fromUTF8);

    Charset systemCharSet = Charset.defaultCharset();
    //hack for testing
    Whitebox.setInternalState(Charset.class, "defaultCharset", Charset.forName("EUC-KR"));
    assertEquals(Charset.forName("EUC-KR"), Charset.defaultCharset());

    assertEquals(text, test.asChars());
    assertNotEquals(new String(test.asByteArray()), test.asChars());

    //restore
    Whitebox.setInternalState(Charset.class, "defaultCharset", systemCharSet);
    assertEquals(systemCharSet, Charset.defaultCharset());
  }
  
  @Test
  public void testAsUnicodeChars() {
    byte[] testStringArray = new byte[] {0x74, 0x61, 0x6A, 0x6F};
    TextDatum test = new TextDatum(testStringArray);
    
    assertArrayEquals(new char[] {'t', 'a', 'j', 'o'}, test.asUnicodeChars());
    
    testStringArray = new byte[] {(byte) 0xED, (byte) 0x83, (byte) 0x80, (byte) 0xEC, 
        (byte) 0xA1, (byte) 0xB0};
    test = new TextDatum(testStringArray);
    
    assertArrayEquals(new char[] {'\ud0c0', '\uc870'}, test.asUnicodeChars());
  }
}
