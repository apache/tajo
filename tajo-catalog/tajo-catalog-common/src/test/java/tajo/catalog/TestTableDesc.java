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

import com.google.gson.Gson;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.json.GsonCreator;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;

import static org.junit.Assert.assertEquals;

public class TestTableDesc {
	TableMeta info;
	TableDesc desc;
	
	@Before
	public void setup() {
	  Schema schema = new Schema();
    schema.addColumn("name", DataType.BYTE);
    schema.addColumn("addr", DataType.STRING);
    info = TCatUtil.newTableMeta(schema, StoreType.CSV);

    desc = new TableDescImpl("table1", info, new Path("/nta/data"));
	}

  @Test
  public void test() throws CloneNotSupportedException {
    Schema schema = new Schema();
    schema.addColumn("name", DataType.BYTE);
    schema.addColumn("addr", DataType.STRING);
    TableMeta info = TCatUtil.newTableMeta(schema, StoreType.CSV);
    testClone(info);

    TableDesc desc = new TableDescImpl("table1", info, new Path("/nta/data"));
    assertEquals("table1", desc.getId());
    
    assertEquals(new Path("/nta/data"), desc.getPath());
    assertEquals(info, desc.getMeta());
    testClone(desc);
  }
  
  @Test
  public void testTableMetaToJson() throws CloneNotSupportedException {
    TableMeta meta = new TableMetaImpl(info.getProto());
    Gson gson = GsonCreator.getInstance();
    String json = meta.toJSON();
    System.out.println(json);
    TableMeta jsonMeta = gson.fromJson(json, TableMeta.class);
    assertEquals(meta.getSchema(), jsonMeta.getSchema());
    assertEquals(meta.getStoreType(), jsonMeta.getStoreType());
    assertEquals(meta, jsonMeta);
    testClone(meta);
  }
  
  @Test
  public void testTableDescToJson() throws CloneNotSupportedException {
    Gson gson = GsonCreator.getInstance();

    TableDesc desc = new TableDescImpl("table1", info, new Path("/nta/data"));
    testClone(desc);

    String json = desc.toJSON();
    System.out.println(json);
    TableDesc fromJson = gson.fromJson(json, TableDesc.class);
    assertEquals(desc.getId(), fromJson.getId());
    assertEquals(desc.getPath(), fromJson.getPath());
    assertEquals(desc.getMeta(), fromJson.getMeta());
    testClone(fromJson);
  }

  public void testClone(TableDesc desc) throws CloneNotSupportedException {
    TableDesc copy = (TableDesc) desc.clone();
    assertEquals(desc, copy);
  }
  
  public void testClone(TableMeta meta) throws CloneNotSupportedException {
    TableMeta copy = (TableMeta) meta.clone();
    assertEquals(meta, copy);
  }
}