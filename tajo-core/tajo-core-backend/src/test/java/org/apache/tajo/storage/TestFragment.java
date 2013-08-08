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
import org.junit.Before;
import org.junit.Test;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;

import java.util.Arrays;
import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFragment {
  private Schema schema1;
  private TableMeta meta1;
  
  @Before
  public final void setUp() throws Exception {
    schema1 = new Schema();
    schema1.addColumn("id", Type.INT4);
    schema1.addColumn("name", Type.TEXT);
    meta1 = CatalogUtil.newTableMeta(schema1, StoreType.CSV);
  }

  @Test
  public final void testGetAndSetFields() {    
    Fragment fragment1 = new Fragment("table1_1", new Path("/table0"),
        meta1, 0, 500, null);
    fragment1.setDistCached();

    assertEquals("table1_1", fragment1.getId());
    assertEquals(new Path("/table0"), fragment1.getPath());
    assertEquals(meta1.getStoreType(), fragment1.getMeta().getStoreType());
    assertEquals(meta1.getSchema().getColumnNum(), 
        fragment1.getMeta().getSchema().getColumnNum());
    assertTrue(fragment1.isDistCached());
    for(int i=0; i < meta1.getSchema().getColumnNum(); i++) {
      assertEquals(meta1.getSchema().getColumn(i).getColumnName(), 
          fragment1.getMeta().getSchema().getColumn(i).getColumnName());
      assertEquals(meta1.getSchema().getColumn(i).getDataType(), 
          fragment1.getMeta().getSchema().getColumn(i).getDataType());
    }
    assertTrue(0 == fragment1.getStartOffset());
    assertTrue(500 == fragment1.getLength());
  }

  @Test
  public final void testTabletTabletProto() {
    Fragment fragment0 = new Fragment("table1_1", new Path("/table0"), meta1, 0, 500, null);
    
    Fragment fragment1 = new Fragment(fragment0.getProto());
    assertEquals("table1_1", fragment1.getId());
    assertEquals(new Path("/table0"), fragment1.getPath());
    assertEquals(meta1.getStoreType(), fragment1.getMeta().getStoreType());
    assertEquals(meta1.getSchema().getColumnNum(), 
        fragment1.getMeta().getSchema().getColumnNum());
    for(int i=0; i < meta1.getSchema().getColumnNum(); i++) {
      assertEquals(meta1.getSchema().getColumn(i).getColumnName(), 
          fragment1.getMeta().getSchema().getColumn(i).getColumnName());
      assertEquals(meta1.getSchema().getColumn(i).getDataType(), 
          fragment1.getMeta().getSchema().getColumn(i).getDataType());
    }
    assertTrue(0 == fragment1.getStartOffset());
    assertTrue(500 == fragment1.getLength());
  }

  @Test
  public final void testCompareTo() {
    final int num = 10;
    Fragment [] tablets = new Fragment[num];
    for (int i = num - 1; i >= 0; i--) {
      tablets[i]
          = new Fragment("tablet1_"+i, new Path("tablet0"), meta1, i * 500, 
              (i+1) * 500, null);
    }
    
    Arrays.sort(tablets);

    for(int i = 0; i < num; i++) {
      assertEquals("tablet1_"+i, tablets[i].getId());
    }
  }

  @Test
  public final void testCompareTo2() {
    final int num = 1860;
    Fragment [] tablets = new Fragment[num];
    for (int i = num - 1; i >= 0; i--) {
      tablets[i]
          = new Fragment("tablet1_"+i, new Path("tablet0"), meta1, (long)i * 6553500,
          (long)(i+1) * 6553500, null);
    }

    SortedSet sortedSet = Sets.newTreeSet();
    for (Fragment frag : tablets) {
      sortedSet.add(frag);
    }
    assertEquals(num, sortedSet.size());
  }
}
