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

package org.apache.tajo.querymaster;

import org.apache.tajo.util.NumberUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestIntermediateEntry {
  @Test
  public void testPage() {
    Task.IntermediateEntry interm = new Task.IntermediateEntry(-1, -1, 1, null);

    NumberUtil.PrimitiveLongs pages = new NumberUtil.PrimitiveLongs(10);
    pages.add(new long[]{0L, 1441275});
    pages.add(new long[]{1441275L, 1447446});
    pages.add(new long[]{2888721L, 1442507});

    interm.setPages(pages.toArray());

    long splitBytes = 3 * 1024 * 1024;

    long[] splits = interm.split(splitBytes, splitBytes);
    assertEquals(2 << 1, splits.length);

    long[][] expected = { {0, 1441275 + 1447446}, {1441275 + 1447446, 1442507} };
    for (int i = 0; i < 2; i++) {
      assertEquals(expected[i][0], splits[i << 1]);
      assertEquals(expected[i][1], splits[(i << 1) + 1]);
    }
  }
}
