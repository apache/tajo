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

package org.apache.tajo.catalog;

import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestTableMeta {
  TableMeta meta = null;
  
  @Before
  public void setUp() {
    meta = CatalogUtil.newTableMeta("CSV");
  }
  
  @Test
  public void testTableMetaTableProto() {    
    Schema schema1 = new Schema();
    schema1.addColumn("name", Type.BLOB);
    schema1.addColumn("addr", Type.TEXT);
    TableMeta meta1 = CatalogUtil.newTableMeta("CSV");
    
    TableMeta meta2 = new TableMeta(meta1.getProto());
    assertEquals(meta1, meta2);
  }
  
  @Test
  public final void testClone() throws CloneNotSupportedException {
    Schema schema1 = new Schema();
    schema1.addColumn("name", Type.BLOB);
    schema1.addColumn("addr", Type.TEXT);
    TableMeta meta1 = CatalogUtil.newTableMeta("CSV");
    
    TableMeta meta2 = (TableMeta) meta1.clone();
    assertEquals(meta1.getStoreType(), meta2.getStoreType());
    assertEquals(meta1, meta2);
  }
  
  @Test
  public void testSchema() throws CloneNotSupportedException {
    Schema schema1 = new Schema();
    schema1.addColumn("name", Type.BLOB);
    schema1.addColumn("addr", Type.TEXT);
    TableMeta meta1 = CatalogUtil.newTableMeta("CSV");
    
    TableMeta meta2 = (TableMeta) meta1.clone();
    
    assertEquals(meta1, meta2);
  }
  
  @Test
  public void testGetStorageType() {
    assertEquals("CSV", meta.getStoreType());
  }
  
  @Test
  public void testEqualsObject() {   
    Schema schema2 = new Schema();
    schema2.addColumn("name", Type.BLOB);
    schema2.addColumn("addr", Type.TEXT);
    TableMeta meta2 = CatalogUtil.newTableMeta("CSV");


    assertTrue(meta.equals(meta2));
    assertNotSame(meta, meta2);
  }

	@Test
	public void testEqualsObject2() {
		//This testcases should insert more 2 items into one slot.
		//HashMap's default slot count is 16
		//so max_count is 17

		int MAX_COUNT = 17;

		TableMeta meta1 = CatalogUtil.newTableMeta(StoreType.CSV.toString());
		for (int i = 0; i < MAX_COUNT; i++) {
			meta1.putOption("key"+i, "value"+i);
		}

		PrimitiveProtos.KeyValueSetProto.Builder optionBuilder = PrimitiveProtos.KeyValueSetProto.newBuilder();
		for (int i = 1; i <= MAX_COUNT; i++) {
			PrimitiveProtos.KeyValueProto.Builder keyValueBuilder = PrimitiveProtos.KeyValueProto.newBuilder();
			keyValueBuilder.setKey("key"+(MAX_COUNT-i)).setValue("value"+(MAX_COUNT-i));
			optionBuilder.addKeyval(keyValueBuilder);
		}
		TableProto.Builder builder = TableProto.newBuilder();
		builder.setStoreType(StoreType.CSV.toString());
		builder.setParams(optionBuilder);
		TableMeta meta2 = new TableMeta(builder.build());
		assertTrue(meta1.equals(meta2));
	}
  
  @Test
  public void testGetProto() {
    TableProto proto = meta.getProto();
    TableMeta newMeta = new TableMeta(proto);
    assertEquals(meta, newMeta);
  }

  @Test
  public void testToJson() {
    String json = meta.toJson();
    TableMeta fromJson = CatalogGsonHelper.fromJson(json, TableMeta.class);
    assertEquals(meta, fromJson);
    assertEquals(meta.getProto(), fromJson.getProto());
  }
}
