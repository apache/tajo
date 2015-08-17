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

package org.apache.tajo.catalog.store;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHCatalogStore {
  private static final String DB_NAME = "test_hcatalog";
  private static final String CUSTOMER = "customer";

  private static HCatalogStore store;
  private static Path warehousePath;

  @BeforeClass
  public static void setUp() throws Exception {
    Path testPath = CommonTestingUtil.getTestDir();
    warehousePath = new Path(testPath, "warehouse");

    //create local hiveMeta
    HiveConf conf = new HiveConf();
    String jdbcUri = "jdbc:derby:;databaseName="+testPath.toUri().getPath()+"metastore_db;create=true";
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehousePath.toUri().toString());
    conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, jdbcUri);
    conf.set(TajoConf.ConfVars.WAREHOUSE_DIR.varname, warehousePath.toUri().toString());

    // create local HiveCatalogStore.
    TajoConf tajoConf = new TajoConf(conf);
    store = new HCatalogStore(tajoConf);
    store.createDatabase(DB_NAME, null);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    store.close();
  }

  @Test
  public void testTableUsingTextFile() throws Exception {
    TableMeta meta = new TableMeta("TEXT", new KeyValueSet());

    org.apache.tajo.catalog.Schema schema = new org.apache.tajo.catalog.Schema();
    schema.addColumn("c_custkey", TajoDataTypes.Type.INT4);
    schema.addColumn("c_name", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_address", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_nationkey", TajoDataTypes.Type.INT4);
    schema.addColumn("c_phone", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_acctbal", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("c_mktsegment", TajoDataTypes.Type.TEXT);
    schema.addColumn("c_comment", TajoDataTypes.Type.TEXT);

    TableDesc table = new TableDesc(CatalogUtil.buildFQName(DB_NAME, CUSTOMER), schema, meta,
      new Path(warehousePath, new Path(DB_NAME, CUSTOMER)).toUri());
    store.createTable(table.getProto());
    assertTrue(store.existTable(DB_NAME, CUSTOMER));

    TableDesc table1 = new TableDesc(store.getTable(DB_NAME, CUSTOMER));
    assertEquals(table.getName(), table1.getName());
    assertEquals(table.getUri(), table1.getUri());
    assertEquals(table.getSchema().size(), table1.getSchema().size());
    for (int i = 0; i < table.getSchema().size(); i++) {
      assertEquals(table.getSchema().getColumn(i).getSimpleName(), table1.getSchema().getColumn(i).getSimpleName());
    }

    assertEquals(StringEscapeUtils.escapeJava(StorageConstants.DEFAULT_FIELD_DELIMITER),
      table1.getMeta().getOption(StorageConstants.TEXT_DELIMITER));
    store.dropTable(DB_NAME, CUSTOMER);
  }

}
