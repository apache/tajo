/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.tajo.plan.util;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.Writable;
import org.apache.tajo.datum.DateDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedDataTypeException;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.junit.Assert.*;

public class TestWritableTypeConverter {
  @Test
  public void testDateTypeConverting() throws UnsupportedDataTypeException {
    Datum testDatum;
    TimeMeta testTM = new TimeMeta();

    testTM.years = 1977;
    testTM.monthOfYear = 6;
    testTM.dayOfMonth = 28;

    testDatum = new DateDatum(testTM);

    Writable resultWritable = WritableTypeConverter.convertDatum2Writable(testDatum);
    assertEquals("1977-06-28", ((DateWritable)resultWritable).get().toString());

    Datum resultDatum = WritableTypeConverter.convertWritable2Datum(resultWritable);
    assertEquals(testDatum, resultDatum);
  }

  @Test
  public void testTimestampTypeConverting() throws UnsupportedDataTypeException {
    Datum testDatum;
    long currentMills = System.currentTimeMillis();

    testDatum = new TimestampDatum(DateTimeUtil.javaTimeToJulianTime(currentMills));

    Writable resultWritable = WritableTypeConverter.convertDatum2Writable(testDatum);
    assertEquals(currentMills / 1000, ((TimestampWritable)resultWritable).getSeconds());

    Datum resultDatum = WritableTypeConverter.convertWritable2Datum(resultWritable);
    assertEquals(testDatum, resultDatum);
  }

  private static class DummyWritable implements Writable {
    @Override public void write(DataOutput dataOutput) throws IOException {}
    @Override public void readFields(DataInput dataInput) throws IOException {}
  }

  @Test
  public void testNonExistingType() {
    try {
      WritableTypeConverter.convertWritableToTajoType(DummyWritable.class);
    } catch (Exception e) {
      assertEquals(UnsupportedDataTypeException.class, e.getClass());
    }
  }
}
