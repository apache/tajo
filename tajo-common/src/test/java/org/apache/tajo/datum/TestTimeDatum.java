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
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.json.CommonGsonHelper;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestTimeDatum {
  private static String TIME = "12:34:56";

  @Test
  public final void testType() {
    Datum d = DatumFactory.createTime(TIME);
    assertEquals(Type.TIME, d.type());
  }

  @Test(expected = TajoRuntimeException.class)
  public final void testAsInt4() {
    Datum d = DatumFactory.createTime(TIME);
    Datum copy = DatumFactory.createTime(d.asInt4());
    assertEquals(d, copy);
  }

  @Test
  public final void testAsInt8() {
    Datum d = DatumFactory.createTime(TIME);
    Datum copy = DatumFactory.createTime(d.asInt8());
    assertEquals(d, copy);
  }

  @Test(expected = TajoRuntimeException.class)
  public final void testAsFloat4() {
    Datum d = DatumFactory.createTime(TIME);
    d.asFloat4();
  }

  @Test(expected = TajoRuntimeException.class)
  public final void testAsFloat8() {
    Datum d = DatumFactory.createTime(TIME);
    d.asFloat8();
  }

  @Test
  public final void testAsText() {
    Datum d = DatumFactory.createTime(TIME);
    Datum copy = DatumFactory.createTime(d.asChars());
    assertEquals(d, copy);
  }

  @Test
  public final void testSize() {
    Datum d = DatumFactory.createTime(TIME);
    assertEquals(TimeDatum.SIZE, d.asByteArray().length);
  }

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createTime(TIME);
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());
  }

  @Test
  public final void testToJson() {
    Datum d = DatumFactory.createTime(TIME);
    Datum copy = CommonGsonHelper.fromJson(d.toJson(), Datum.class);
    assertEquals(d, copy);
  }

  @Test
  public final void testInstance() {
    TimeDatum d = DatumFactory.createTime(TIME);
    TimeDatum copy = new TimeDatum(d.asInt8());
    assertEquals(d, copy);
  }

  @Test
  public final void testGetFields() {
    TimeDatum d = DatumFactory.createTime(TIME);
    assertEquals(12, d.getHourOfDay());
    assertEquals(34, d.getMinuteOfHour());
    assertEquals(56, d.getSecondOfMinute());
  }

  @Test
  public final void testTimeDatumFromCreateFromInt8() {
    TimeDatum d = DatumFactory.createTime(TIME);
    DataType type = DataType.newBuilder().setType(Type.TIME).build();
    TimeDatum copy = (TimeDatum)DatumFactory.createFromInt8(type, d.asInt8());

    assertEquals(d, copy);
    assertEquals(12, copy.getHourOfDay());
    assertEquals(34, copy.getMinuteOfHour());
    assertEquals(56, copy.getSecondOfMinute());
  }

  @Test
  public final void testNull() {
    Datum d = DatumFactory.createTime(TIME);
    assertEquals(Boolean.FALSE,d.equals(DatumFactory.createNullDatum()));
    assertEquals(DatumFactory.createNullDatum(),d.equalsTo(DatumFactory.createNullDatum()));
    assertEquals(-1,d.compareTo(DatumFactory.createNullDatum()));
  }
}
