/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.catalog;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;

import static org.junit.Assert.assertEquals;

/**
 * @author Hyunsik Choi
 */
public class TestCatalogStore {
  
  @Test
  public final void test() throws Exception {
    TajoConf conf = new TajoConf();
    conf.set(TConstants.STORE_CLASS,
        "tajo.catalog.store.DBStore");
    CatalogServer server = new CatalogServer();
    server.init(conf);
    server.start();
    CatalogService catalog = new LocalCatalog(server);
    
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT)
    .addColumn("name", DataType.STRING)
    .addColumn("age", DataType.INT)
    .addColumn("score", DataType.DOUBLE);
    
    int numTables = 5;
    for (int i = 0; i < numTables; i++) {
      String tableName = "tableA_" + i;
      TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
      TableDesc desc = TCatUtil.newTableDesc(tableName, meta,
          new Path("/tableA_" + i));
      catalog.addTable(desc);
    }
    
    assertEquals(numTables, catalog.getAllTableNames().size());    
    server.stop();

    server = new CatalogServer();
    server.init(conf);
    server.start();
    catalog = new LocalCatalog(server);
    assertEquals(numTables, catalog.getAllTableNames().size());
    
    server.stop();
  }
}
