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
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.storage.fragment.PartitionedFileFragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPartitionedFileFragment {
  private Path path;
  
  @Before
  public final void setUp() throws Exception {
    path = CommonTestingUtil.getTestDir();
  }

  @Test
  public final void testGetAndSetFields() {
    PartitionedFileFragment fragment1 = new PartitionedFileFragment("table1_1", new Path(path, "table0/col1=1"),
      0, 500, "col1=1");

    assertEquals("table1_1", fragment1.getTableName());
    assertEquals(new Path(path, "table0/col1=1"), fragment1.getPath());
    assertEquals("col1=1", fragment1.getPartitionKeys());
    assertTrue(0 == fragment1.getStartKey());
    assertTrue(500 == fragment1.getLength());
  }

  @Test
  public final void testGetProtoAndRestore() {
    PartitionedFileFragment fragment = new PartitionedFileFragment("table1_1", new Path(path, "table0/col1=1"), 0,
      500, "col1=1");

    PartitionedFileFragment fragment1 = FragmentConvertor.convert(PartitionedFileFragment.class, fragment.getProto());
    assertEquals("table1_1", fragment1.getTableName());
    assertEquals(new Path(path, "table0/col1=1"), fragment1.getPath());
    assertEquals("col1=1", fragment1.getPartitionKeys());
    assertTrue(0 == fragment1.getStartKey());
    assertTrue(500 == fragment1.getLength());
  }

  @Test
  public final void testCompareTo() {
    final int num = 10;
    PartitionedFileFragment[] tablets = new PartitionedFileFragment[num];
    for (int i = num - 1; i >= 0; i--) {
      tablets[i] = new PartitionedFileFragment("tablet1", new Path(path, "tablet0/col1=" + i), i * 500, (i+1) * 500
      , "col1=" + i);
    }
    
    Arrays.sort(tablets);

    for(int i = 0; i < num; i++) {
      assertEquals("col1=" + i, tablets[i].getPartitionKeys());
    }
  }

  @Test
  public final void testCompareTo2() {
    final int num = 1860;
    PartitionedFileFragment[] tablets = new PartitionedFileFragment[num];
    for (int i = num - 1; i >= 0; i--) {
      tablets[i] = new PartitionedFileFragment("tablet1", new Path(path, "tablet/col1=" +i), (long)i * 6553500,
        (long) (i+1) * 6553500, "col1=" + i);
    }

    SortedSet sortedSet = Sets.newTreeSet();
    for (PartitionedFileFragment frag : tablets) {
      sortedSet.add(frag);
    }
    assertEquals(num, sortedSet.size());
  }
}
