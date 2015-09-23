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

import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.json.CommonGsonHelper;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class TestDateDatum {
  private static String DATE = "1980-04-01";

	@Test
	public final void testType() {
		Datum d = DatumFactory.createDate(DATE);
    assertEquals(Type.DATE, d.type());
	}

  @Test
	public final void testAsInt4() {
    Datum d = DatumFactory.createDate(DATE);
    Datum copy = DatumFactory.createDate(d.asInt4());
    assertEquals(d, copy);
	}

	@Test
	public final void testAsInt8() {
    Datum d = DatumFactory.createDate(DATE);
    Datum copy = DatumFactory.createDate((int) d.asInt8());
    assertEquals(d, copy);
	}

  @Test(expected = TajoRuntimeException.class)
	public final void testAsFloat4() {
    Datum d = DatumFactory.createDate(DATE);
    d.asFloat4();
	}

  @Test(expected = TajoRuntimeException.class)
	public final void testAsFloat8() {
    Datum d = DatumFactory.createDate(DATE);
    d.asFloat8();
	}

	@Test
	public final void testAsText() {
    Datum d = DatumFactory.createDate(DATE);
    Datum copy = DatumFactory.createDate(d.asChars());
    assertEquals(d, copy);
	}

	@Test
  public final void testSize() {
    Datum d = DatumFactory.createDate(DATE);
    assertEquals(DateDatum.SIZE, d.asByteArray().length);
  }

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createDate(DATE);
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());
  }

  @Test
  public final void testToJson() {
    Datum d = DatumFactory.createDate(DATE);
    Datum copy = CommonGsonHelper.fromJson(d.toJson(), Datum.class);
    assertEquals(d, copy);
  }

  @Test
  public final void testInstance() {
    DateDatum d = DatumFactory.createDate(DATE);
    DateDatum copy = new DateDatum(d.asInt4());
    assertEquals(d, copy);
  }

  @Test
  public final void testGetFields() {
    DateDatum d = DatumFactory.createDate(DATE);
    assertEquals(1980, d.getYear());
    assertEquals(4, d.getMonthOfYear());
    assertEquals(1, d.getDayOfMonth());
  }

  @Test
  public final void testNull() {
    Datum d = DatumFactory.createDate(DATE);
    assertEquals(Boolean.FALSE,d.equals(DatumFactory.createNullDatum()));
    assertEquals(DatumFactory.createNullDatum(),d.equalsTo(DatumFactory.createNullDatum()));
    assertEquals(-1,d.compareTo(DatumFactory.createNullDatum()));
  }
  
  @Test
  public void testCompareTo() {
    DateDatum theday = DatumFactory.createDate("2014-11-12");
    DateDatum thedaybefore = DatumFactory.createDate("2014-11-11");
    
    assertThat(theday.compareTo(thedaybefore) > 0, is(true));
    assertThat(thedaybefore.compareTo(theday) > 0, is(false));
    
    TimestampDatum timestamp = DatumFactory.createTimestamp("2014-11-12 15:00:00.68");
    
    assertThat(timestamp.compareTo(theday) > 0, is(true));
  }
}
