/***
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

package org.apache.tajo.storage.directmem;

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDirectRowBlock {

  @Test
  public void testPutInt() {
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);

    int vecSize = 4096;

    long allocateStart = System.currentTimeMillis();
    DirectRowBlock rowBlock = new DirectRowBlock(schema, 40000);
    long allocatedEnd = System.currentTimeMillis();
    System.out.println(FileUtil.humanReadableByteCount(rowBlock.totalMem(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    UnSafeTuple tuple = new UnSafeTuple();
    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      rowBlock.startRow();
      rowBlock.putInt2((short) 1);
      rowBlock.putInt4(i);
      rowBlock.putInt8(i);
      rowBlock.putFloat4(i);
      rowBlock.putFloat8(i);
      rowBlock.endRow();
      assertEquals(i + "th written", 50, UnsafeUtil.unsafe.getInt(rowBlock.address()));

      /*
      rowBlock.resetRowCursor();
      int j = 0;
      while(rowBlock.next(tuple)) {
        assertTrue(1 == tuple.getInt2(0));
        assertEquals(j, tuple.getInt4(1));
        assertEquals(j, tuple.getInt8(2));
        assertTrue(j == tuple.getFloat4(3));
        assertTrue(j == tuple.getFloat8(4));
        j++;
      }*/
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    long readStart = System.currentTimeMillis();
    tuple = new UnSafeTuple();
    int i = 0;
    rowBlock.resetRowCursor();
    while(rowBlock.next(tuple)) {
      assertTrue(1 == tuple.getInt2(0));
      assertEquals(i, tuple.getInt4(1));
      assertEquals(i, tuple.getInt8(2));
      assertTrue(i == tuple.getFloat4(3));
      assertTrue(i == tuple.getFloat8(4));
      i++;
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(readEnd - readStart + " read msec");

    rowBlock.free();
  }

  @Test
  public void testPutVTuple() {
    Schema schema = new Schema();
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);

    int vecSize = 1024;

    long allocateStart = System.currentTimeMillis();

    List<VTuple> rowBlock = Lists.newArrayList();
    long writeStart = System.currentTimeMillis();
    VTuple tuple;
    for (int i = 0; i < vecSize; i++) {
      tuple = new VTuple(schema.size());
      tuple.put(0, DatumFactory.createInt2((short) 1));
      tuple.put(1, DatumFactory.createInt4(i));
      tuple.put(2, DatumFactory.createInt8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createFloat8(i));
      rowBlock.add(tuple);
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    long readStart = System.currentTimeMillis();
    int k = 0;
    for (VTuple t : rowBlock) {
      assertTrue(1 == t.getInt2(0));
      assertEquals(k, t.getInt4(1));
      assertEquals(k, t.getInt8(2));
      assertTrue(k == t.getFloat4(3));
      assertTrue(k == t.getFloat8(4));

      k++;
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(readEnd - readStart + " read msec");

    int count = 0;
    for (int i = 0; i < rowBlock.size(); i++) {
      for(int j = 0; j < schema.size(); j++ ) {
        if (rowBlock.get(i).get(j).type() == TajoDataTypes.Type.INT4) {
          count ++;
        }
      }
    }

    System.out.println(count);
  }
}