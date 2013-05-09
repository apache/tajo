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

package tajo.engine.function;

import org.junit.Test;
import tajo.datum.Datum;
import tajo.datum.Int8Datum;
import tajo.datum.TextDatum;
import tajo.engine.function.builtin.Date;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.util.Calendar;

import static org.junit.Assert.assertEquals;

public class TestGeneralFunction {

  @Test
  public void testDate() {
    Date date = new Date();
    Tuple tuple = new VTuple(new Datum[] {new TextDatum("25/12/2012 00:00:00")});
    Int8Datum unixtime = (Int8Datum) date.eval(tuple);
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(unixtime.asInt8());
    assertEquals(2012, c.get(Calendar.YEAR));
    assertEquals(11, c.get(Calendar.MONTH));
    assertEquals(25, c.get(Calendar.DAY_OF_MONTH));
    assertEquals(0, c.get(Calendar.HOUR_OF_DAY));
    assertEquals(0, c.get(Calendar.MINUTE));
    assertEquals(0, c.get(Calendar.SECOND));
  }
}
