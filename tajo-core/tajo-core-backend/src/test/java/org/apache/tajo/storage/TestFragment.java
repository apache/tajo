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

package org.apache.tajo.storage;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFragment {
  private Schema schema1;
  private Path path;
  
  @Before
  public final void setUp() throws Exception {
    schema1 = new Schema();
    schema1.addColumn("id", Type.INT4);
    schema1.addColumn("name", Type.TEXT);
    path = CommonTestingUtil.getTestDir();
  }

  @Test
  public final void testGetAndSetFields() {
    Fragment fragment1 = new Fragment("table1_1", new Path(path, "table0"), 0, 500);
    fragment1.setDistCached();

    assertEquals("table1_1", fragment1.getName());
    assertEquals(new Path(path, "table0"), fragment1.getPath());
    assertTrue(fragment1.isDistCached());
    assertTrue(0 == fragment1.getStartOffset());
    assertTrue(500 == fragment1.getLength());
  }

  @Test
  public final void testTabletTabletProto() {
    Fragment fragment0 = new Fragment("table1_1", new Path(path, "table0"), 0, 500);
    fragment0.setDistCached();
    Fragment fragment1 = new Fragment(fragment0.getProto());
    assertEquals("table1_1", fragment1.getName());
    assertEquals(new Path(path, "table0"), fragment1.getPath());
    assertTrue(fragment1.isDistCached());
    assertTrue(0 == fragment1.getStartOffset());
    assertTrue(500 == fragment1.getLength());
  }

  @Test
  public final void testCompareTo() {
    final int num = 10;
    Fragment [] tablets = new Fragment[num];
    for (int i = num - 1; i >= 0; i--) {
      tablets[i] = new Fragment("tablet1_"+i, new Path(path, "tablet0"), i * 500, (i+1) * 500);
    }
    
    Arrays.sort(tablets);

    for(int i = 0; i < num; i++) {
      assertEquals("tablet1_"+i, tablets[i].getName());
    }
  }

  @Test
  public final void testCompareTo2() {
    final int num = 1860;
    Fragment [] tablets = new Fragment[num];
    for (int i = num - 1; i >= 0; i--) {
      tablets[i] = new Fragment("tablet1_"+i, new Path(path, "tablet0"), (long)i * 6553500, (long)(i+1) * 6553500);
    }

    SortedSet sortedSet = Sets.newTreeSet();
    for (Fragment frag : tablets) {
      sortedSet.add(frag);
    }
    assertEquals(num, sortedSet.size());
  }
}
