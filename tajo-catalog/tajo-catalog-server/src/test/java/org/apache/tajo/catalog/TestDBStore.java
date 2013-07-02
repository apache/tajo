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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.catalog.store.DBStore;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.CommonTestingUtil;

import java.io.File;

import static org.junit.Assert.*;

public class TestDBStore {
  private static final Log LOG = LogFactory.getLog(TestDBStore.class);  
  private static Configuration conf;
  private static DBStore store;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new TajoConf();
    Path testDir = CommonTestingUtil.getTestDir("target/test-data/TestDBSTore");
    File absolutePath = new File(testDir.toUri());
    conf.set(CatalogConstants.JDBC_URI, "jdbc:derby:"+absolutePath.getAbsolutePath()+"/db");
    LOG.info("derby repository is set to "+conf.get(CatalogConstants.JDBC_URI));
    store = new DBStore(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    store.close();
  }

  @Test
  public final void testAddAndDeleteTable() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
    .addColumn("name", Type.TEXT)
    .addColumn("age", Type.INT4)
    .addColumn("score", Type.FLOAT8);
    
    String tableName = "addedtable";
    Options opts = new Options();
    opts.put("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.CSV, opts);
    TableDesc desc = new TableDescImpl(tableName, meta, new Path("/addedtable"));
    assertFalse(store.existTable(tableName));
    store.addTable(desc);
    assertTrue(store.existTable(tableName));

    TableDesc retrieved = store.getTable(tableName);
    // Schema order check
    assertSchemaOrder(desc.getMeta().getSchema(), retrieved.getMeta().getSchema());
    store.deleteTable(tableName);
    assertFalse(store.existTable(tableName));
  }
  
  @Test
  public final void testGetTable() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("gettable.id", Type.INT4)
    .addColumn("gettable.name", Type.TEXT)
    .addColumn("gettable.age", Type.INT4)
    .addColumn("gettable.score", Type.FLOAT8);
    
    String tableName = "gettable";
    Options opts = new Options();
    opts.put("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.CSV, opts);

    TableStat stat = new TableStat();
    stat.setNumRows(957685);
    stat.setNumBytes(1023234);
    meta.setStat(stat);

    TableDesc desc = new TableDescImpl(tableName, meta, new Path("/gettable"));

    store.addTable(desc);
    TableDesc retrieved = store.getTable(tableName);
    assertEquals(",", retrieved.getMeta().getOption("file.delimiter"));
    assertEquals(desc, retrieved);
    assertTrue(957685 == desc.getMeta().getStat().getNumRows());
    assertTrue(1023234 == desc.getMeta().getStat().getNumBytes());
    // Schema order check
    assertSchemaOrder(desc.getMeta().getSchema(), retrieved.getMeta().getSchema());
    store.deleteTable(tableName);
  }
  
  @Test
  public final void testGetAllTableNames() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
    .addColumn("name", Type.TEXT)
    .addColumn("age", Type.INT4)
    .addColumn("score", Type.FLOAT8);
    
    int numTables = 5;
    for (int i = 0; i < numTables; i++) {
      String tableName = "tableA_" + i;
      TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
      TableDesc desc = new TableDescImpl(tableName, meta, 
          new Path("/tableA_" + i));
      store.addTable(desc);
    }
    
    assertEquals(numTables, store.getAllTableNames().size());
  }  
  
  @Test
  public final void testAddAndDeleteIndex() throws Exception {
    TableDesc table = prepareTable();
    store.addTable(table);
    
    store.addIndex(TestCatalog.desc1.getProto());
    assertTrue(store.existIndex(TestCatalog.desc1.getName()));
    store.delIndex(TestCatalog.desc1.getName());
    assertFalse(store.existIndex(TestCatalog.desc1.getName()));
    
    store.deleteTable(table.getId());
  }
  
  @Test
  public final void testGetIndex() throws Exception {
    
    TableDesc table = prepareTable();
    store.addTable(table);
    
    store.addIndex(TestCatalog.desc2.getProto());
    assertEquals(
        new IndexDesc(TestCatalog.desc2.getProto()),
        new IndexDesc(store.getIndex(TestCatalog.desc2.getName())));
    store.delIndex(TestCatalog.desc2.getName());
    
    store.deleteTable(table.getId());
  }
  
  @Test
  public final void testGetIndexByTableAndColumn() throws Exception {
    
    TableDesc table = prepareTable();
    store.addTable(table);
    
    store.addIndex(TestCatalog.desc2.getProto());
    
    String tableId = TestCatalog.desc2.getTableId();
    String columnName = "score";
    assertEquals(
        new IndexDesc(TestCatalog.desc2.getProto()),
        new IndexDesc(store.getIndex(tableId, columnName)));
    store.delIndex(TestCatalog.desc2.getName());
    
    store.deleteTable(table.getId());
  }
  
  @Test
  public final void testGetAllIndexes() throws Exception {
    
    TableDesc table = prepareTable();
    store.addTable(table);
    
    store.addIndex(TestCatalog.desc1.getProto());
    store.addIndex(TestCatalog.desc2.getProto());
        
    assertEquals(2, 
        store.getIndexes(TestCatalog.desc2.getTableId()).length);
    store.delIndex(TestCatalog.desc1.getName());
    store.delIndex(TestCatalog.desc2.getName());
    
    store.deleteTable(table.getId());
  }
  
  public static TableDesc prepareTable() {
    Schema schema = new Schema();
    schema.addColumn("indexed.id", Type.INT4)
    .addColumn("indexed.name", Type.TEXT)
    .addColumn("indexed.age", Type.INT4)
    .addColumn("indexed.score", Type.FLOAT8);
    
    String tableName = "indexed";
    
    TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    return new TableDescImpl(tableName, meta, new Path("/indexed"));
  }

  public static void assertSchemaOrder(Schema s1, Schema s2) {
    // Schema order check
    assertEquals(s1.getColumnNum(),
        s2.getColumnNum());

    for (int i = 0; i < s1.getColumnNum(); i++) {
      assertEquals(s1.getColumn(i).getColumnName(),
          s2.getColumn(i).getColumnName());
    }
  }
}
