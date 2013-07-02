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

package org.apache.tajo.catalog.statistics;

import org.junit.Test;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.DatumFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestColumnStat {

  @Test
  public final void testColumnStat() {
    ColumnStat stat = new ColumnStat(new Column("test", Type.INT8));
    stat.setNumDistVals(1000);
    stat.setNumNulls(999);
    
    assertTrue(1000 == stat.getNumDistValues());
    assertTrue(999 == stat.getNumNulls());
    
    ColumnStat stat2 = new ColumnStat(stat.getProto());
    assertTrue(1000 == stat2.getNumDistValues());
    assertTrue(999 == stat2.getNumNulls());
  }

  @Test
  public final void testEqualsObject() {
    ColumnStat stat = new ColumnStat(new Column("test", Type.INT8));
    stat.setNumDistVals(1000);
    stat.setNumNulls(999);
    stat.setMinValue(DatumFactory.createInt8(5));
    stat.setMaxValue(DatumFactory.createInt8(10));
    
    ColumnStat stat2 = new ColumnStat(stat.getProto());
    assertEquals(stat, stat2);
  }

  @Test
  public final void testClone() throws CloneNotSupportedException {
    ColumnStat stat = new ColumnStat(new Column("test", Type.INT8));
    stat.setNumDistVals(1000);
    stat.setNumNulls(999);
    stat.setMinValue(DatumFactory.createInt8(5));
    stat.setMaxValue(DatumFactory.createInt8(10));
    
    ColumnStat stat2 = (ColumnStat) stat.clone();
    assertEquals(stat, stat2);
  }
}
