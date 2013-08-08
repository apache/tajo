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
import org.apache.tajo.catalog.statistics.ColumnStat;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestTableMeta {
  TableMeta meta = null;
  Schema schema = null;
  
  @Before
  public void setUp() {    
    schema = new Schema();
    schema.addColumn("name", Type.BLOB);
    schema.addColumn("addr", Type.TEXT);
    meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);

    TableStat stat = new TableStat();
    stat.setNumRows(957685);
    stat.setNumBytes(1023234);
    stat.setNumBlocks(3123);
    stat.setNumPartitions(5);
    stat.setAvgRows(80000);

    int numCols = 2;
    ColumnStat[] cols = new ColumnStat[numCols];
    for (int i = 0; i < numCols; i++) {
      cols[i] = new ColumnStat(schema.getColumn(i));
      cols[i].setNumDistVals(1024 * i);
      cols[i].setNumNulls(100 * i);
      stat.addColumnStat(cols[i]);
    }
    meta.setStat(stat);
  }
  
  @Test
  public void testTableMetaTableProto() {    
    Schema schema1 = new Schema();
    schema1.addColumn("name", Type.BLOB);
    schema1.addColumn("addr", Type.TEXT);
    TableMeta meta1 = CatalogUtil.newTableMeta(schema1, StoreType.CSV);
    
    TableMeta meta2 = new TableMetaImpl(meta1.getProto());
    assertEquals(meta1, meta2);
  }
  
  @Test
  public final void testClone() throws CloneNotSupportedException {
    Schema schema1 = new Schema();
    schema1.addColumn("name", Type.BLOB);
    schema1.addColumn("addr", Type.TEXT);
    TableMeta meta1 = CatalogUtil.newTableMeta(schema1, StoreType.CSV);
    
    TableMetaImpl meta2 = (TableMetaImpl) meta1.clone();
    assertEquals(meta1.getSchema(), meta2.getSchema());
    assertEquals(meta1.getStoreType(), meta2.getStoreType());
    assertEquals(meta1, meta2);
  }
  
  @Test
  public void testSchema() throws CloneNotSupportedException {
    Schema schema1 = new Schema();
    schema1.addColumn("name", Type.BLOB);
    schema1.addColumn("addr", Type.TEXT);
    TableMeta meta1 = CatalogUtil.newTableMeta(schema1, StoreType.CSV);
    
    TableMeta meta2 = (TableMeta) meta1.clone();
    
    assertEquals(meta1, meta2);
  }
  
  @Test
  public void testGetStorageType() {
    assertEquals(StoreType.CSV, meta.getStoreType());
  }
  
  @Test
  public void testGetSchema() {
    Schema schema2 = new Schema();
    schema2.addColumn("name", Type.BLOB);
    schema2.addColumn("addr", Type.TEXT);
    
    assertEquals(schema, schema2);
  }
  
  @Test
  public void testSetSchema() {
    Schema schema2 = new Schema();
    schema2.addColumn("name", Type.BLOB);
    schema2.addColumn("addr", Type.TEXT);
    schema2.addColumn("age", Type.INT4);
    
    assertNotSame(meta.getSchema(), schema2);
    meta.setSchema(schema2);
    assertEquals(meta.getSchema(), schema2);
  }
  
  @Test
  public void testEqualsObject() {   
    Schema schema2 = new Schema();
    schema2.addColumn("name", Type.BLOB);
    schema2.addColumn("addr", Type.TEXT);
    TableMeta meta2 = CatalogUtil.newTableMeta(schema2, StoreType.CSV);

    TableStat stat = new TableStat();
    stat.setNumRows(957685);
    stat.setNumBytes(1023234);
    stat.setNumBlocks(3123);
    stat.setNumPartitions(5);
    stat.setAvgRows(80000);

    int numCols = 2;
    ColumnStat[] cols = new ColumnStat[numCols];
    for (int i = 0; i < numCols; i++) {
      cols[i] = new ColumnStat(schema2.getColumn(i));
      cols[i].setNumDistVals(1024 * i);
      cols[i].setNumNulls(100 * i);
      stat.addColumnStat(cols[i]);
    }
    meta2.setStat(stat);


    assertTrue(meta.equals(meta2));
    assertNotSame(meta, meta2);
  }
  
  @Test
  public void testGetProto() {
    TableProto proto = meta.getProto();
    TableMeta newMeta = new TableMetaImpl(proto);
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
