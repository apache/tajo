/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.tuple.memory;

import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.tuple.RowBlockReader;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestUnSafeTupleBytesComparator {

  @Test
  public void testCompare() {
    MemoryRowBlock rowBlock = null;
    try {
      rowBlock = new MemoryRowBlock(new DataType[]{
          DataType.newBuilder().setType(Type.TEXT).build()
      });
      RowWriter builder = rowBlock.getWriter();
      builder.startRow();
      builder.putText("CÔTE D'IVOIRE");
      builder.endRow();
      builder.startRow();
      builder.putText("CANADA");
      builder.endRow();

      RowBlockReader reader = rowBlock.getReader();

      UnSafeTuple t1 = new UnSafeTuple();
      UnSafeTuple t2 = new UnSafeTuple();
      reader.next(t1);
      reader.next(t2);

      // 'CÔTE D'IVOIRE' should occur later than 'CANADA' in ascending order.
      int compare = UnSafeTupleBytesComparator.compare(t1.getFieldAddr(0), t2.getFieldAddr(0));
      assertTrue(compare > 0);
    } finally {
      if (rowBlock != null) {
        rowBlock.release();
      }
    }
  }
}
