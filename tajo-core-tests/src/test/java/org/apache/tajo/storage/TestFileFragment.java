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
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.fragment.BuiltinFragmentKinds;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFileFragment {
  private TajoConf conf;
  private Path path;
  
  @Before
  public final void setUp() throws Exception {
    conf = new TajoConf();
    path = CommonTestingUtil.getTestDir();
  }

  @Test
  public final void testGetAndSetFields() {
    FileFragment fragment1 = new FileFragment("table1_1", new Path(path, "table0"), 0, 500);

    assertEquals("table1_1", fragment1.getInputSourceId());
    assertEquals(new Path(path, "table0"), fragment1.getPath());
    assertTrue(0 == fragment1.getStartKey());
    assertTrue(500 == fragment1.getLength());
  }

  @Test
  public final void testGetProtoAndRestore() {
    FileFragment fragment = new FileFragment("table1_1", new Path(path, "table0"), 0, 500);

    FileFragment fragment1 = FragmentConvertor.convert(conf, BuiltinFragmentKinds.FILE,
        FragmentConvertor.toFragmentProto(conf, fragment));
    assertEquals("table1_1", fragment1.getInputSourceId());
    assertEquals(new Path(path, "table0"), fragment1.getPath());
    assertTrue(0 == fragment1.getStartKey());
    assertTrue(500 == fragment1.getLength());
  }

  @Test
  public final void testCompareTo() {
    final int num = 10;
    FileFragment[] tablets = new FileFragment[num];
    for (int i = num - 1; i >= 0; i--) {
      tablets[i] = new FileFragment("tablet1_"+i, new Path(path, "tablet0"), i * 500, (i+1) * 500);
    }
    
    Arrays.sort(tablets);

    for(int i = 0; i < num; i++) {
      assertEquals("tablet1_"+i, tablets[i].getInputSourceId());
    }
  }

  @Test
  public final void testCompareTo2() {
    final int num = 1860;
    FileFragment[] tablets = new FileFragment[num];
    for (int i = num - 1; i >= 0; i--) {
      tablets[i] = new FileFragment("tablet1_"+i, new Path(path, "tablet0"), (long)i * 6553500, (long)(i+1) * 6553500);
    }

    SortedSet sortedSet = Sets.newTreeSet();
    Collections.addAll(sortedSet, tablets);
    assertEquals(num, sortedSet.size());
  }
}
