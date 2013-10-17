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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Before;
import org.junit.Test;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestTableDesc {
	TableMeta info;
	TableDesc desc;
  Path path;
	
	@Before
	public void setup() throws IOException {
	  Schema schema = new Schema();
    schema.addColumn("name", Type.BLOB);
    schema.addColumn("addr", Type.TEXT);
    info = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    path = new Path(CommonTestingUtil.getTestDir(), "table1");
    desc = new TableDescImpl("table1", info, path);
	}

  @Test
  public void test() throws CloneNotSupportedException, IOException {
    Schema schema = new Schema();
    schema.addColumn("name", Type.BLOB);
    schema.addColumn("addr", Type.TEXT);
    TableMeta info = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    testClone(info);

    Path path = new Path(CommonTestingUtil.getTestDir(), "tajo");

    TableDesc desc = new TableDescImpl("table1", info, path);
    assertEquals("table1", desc.getName());
    
    assertEquals(path, desc.getPath());
    assertEquals(info, desc.getMeta());
    testClone(desc);
  }

  @Test
  public void testGetProto() throws CloneNotSupportedException, IOException {
    Path path = new Path(CommonTestingUtil.getTestDir(), "tajo");
    TableDesc desc = new TableDescImpl("table1", info, path);
    CatalogProtos.TableDescProto proto = (CatalogProtos.TableDescProto) desc.getProto();

    TableDesc fromProto = new TableDescImpl(proto);
    assertEquals("equality check the object deserialized from json", desc, fromProto);
  }

  @Test
  public void testToJson() throws CloneNotSupportedException, IOException {
    Path path = new Path(CommonTestingUtil.getTestDir(), "tajo");
    TableDesc desc = new TableDescImpl("table1", info, path);
    String json = desc.toJson();

    TableDesc fromJson = CatalogGsonHelper.fromJson(json, TableDesc.class);
    assertEquals("equality check the object deserialized from json", desc, fromJson);
    assertEquals("equality between protos", desc.getProto(), fromJson.getProto());
  }

  public TableDesc testClone(TableDesc desc) throws CloneNotSupportedException {
    TableDesc copy = (TableDesc) desc.clone();
    assertEquals("clone check", desc, copy);
    return copy;
  }
  
  public void testClone(TableMeta meta) throws CloneNotSupportedException {
    TableMeta copy = (TableMeta) meta.clone();
    assertEquals("clone check", meta, copy);
  }
}