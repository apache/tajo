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

package org.apache.tajo.client;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;


public class TestDDLBuilder {
  @Test
  public void testBuildDDL() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("name", TajoDataTypes.Type.BLOB);
    schema.addColumn("addr", TajoDataTypes.Type.TEXT);
    TableMeta meta = CatalogUtil.newTableMeta(schema, CatalogProtos.StoreType.CSV);
    meta.putOption("csv.delimiter", "|");
    meta.putOption(TableMeta.COMPRESSION_CODEC, GzipCodec.class.getName());


    TableDesc desc = new TableDescImpl("table1", meta, new Path("/table1"));

    assertEquals(FileUtil.readTextFile(new File("src/test/results/testBuildDDL.result")), DDLBuilder.buildDDL(desc));
  }
}
