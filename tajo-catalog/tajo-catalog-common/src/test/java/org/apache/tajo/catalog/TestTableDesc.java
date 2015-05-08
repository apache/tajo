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
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestTableDesc {
	TableMeta info;
  Schema schema;
	TableDesc desc;
  Path path;
  TableStats stats;
	
	@Before
	public void setup() throws IOException {
	  schema = new Schema();
    schema.addColumn("name", Type.BLOB);
    schema.addColumn("addr", Type.TEXT);
    info = CatalogUtil.newTableMeta("CSV");
    path = new Path(CommonTestingUtil.getTestDir(), "table1");
    desc = new TableDesc("table1", schema, info, path.toUri());
    stats = new TableStats();
    stats.setNumRows(957685);
    stats.setNumBytes(1023234);
    stats.setNumBlocks(3123);
    stats.setNumShuffleOutputs(5);
    stats.setAvgRows(80000);

    int numCols = 2;
    ColumnStats[] cols = new ColumnStats[numCols];
    for (int i = 0; i < numCols; i++) {
      cols[i] = new ColumnStats(schema.getColumn(i));
      cols[i].setNumDistVals(1024 * i);
      cols[i].setNumNulls(100 * i);
      stats.addColumnStat(cols[i]);
    }
    desc.setStats(stats);
	}

  @Test
  public void test() throws CloneNotSupportedException, IOException {
    Schema schema = new Schema();
    schema.addColumn("name", Type.BLOB);
    schema.addColumn("addr", Type.TEXT);
    TableMeta info = CatalogUtil.newTableMeta("CSV");
    testClone(info);

    Path path = new Path(CommonTestingUtil.getTestDir(), "tajo");

    TableDesc desc = new TableDesc("table1", schema, info, path.toUri());
    assertEquals("table1", desc.getName());
    
    assertEquals(path.toUri(), desc.getPath());
    assertEquals(info, desc.getMeta());
    testClone(desc);
  }

  @Test
  public void testGetProto() throws CloneNotSupportedException, IOException {
    Path path = new Path(CommonTestingUtil.getTestDir(), "tajo");
    TableDesc desc = new TableDesc("table1", schema, info, path.toUri());
    desc.setStats(stats);
    CatalogProtos.TableDescProto proto = desc.getProto();

    TableDesc fromProto = new TableDesc(proto);
    assertEquals("equality check the object deserialized from json", desc, fromProto);
  }

  @Test
  public void testToJson() throws CloneNotSupportedException, IOException {
    Path path = new Path(CommonTestingUtil.getTestDir(), "tajo");
    TableDesc desc = new TableDesc("table1", schema, info, path.toUri());
    desc.setStats(stats);
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