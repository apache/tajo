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

package org.apache.tajo.storage.json;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.*;

public class TestJsonSerDe {
  private static Schema schema = new Schema();

  static {
    schema.addColumn("col1", TajoDataTypes.Type.BOOLEAN);
    schema.addColumn("col2", TajoDataTypes.Type.CHAR, 7);
    schema.addColumn("col3", TajoDataTypes.Type.INT2);
    schema.addColumn("col4", TajoDataTypes.Type.INT4);
    schema.addColumn("col5", TajoDataTypes.Type.INT8);
    schema.addColumn("col6", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col7", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col8", TajoDataTypes.Type.TEXT);
    schema.addColumn("col9", TajoDataTypes.Type.BLOB);
    schema.addColumn("col10", TajoDataTypes.Type.INET4);
    schema.addColumn("col11", TajoDataTypes.Type.NULL_TYPE);
  }

  public static Path getResourcePath(String path, String suffix) {
    URL resultBaseURL = ClassLoader.getSystemResource(path);
    return new Path(resultBaseURL.toString(), suffix);
  }

  @Test
  public void testVarioutType() throws IOException {
    TajoConf conf = new TajoConf();

    TableMeta meta = CatalogUtil.newTableMeta("JSON");
    Path tablePath = new Path(getResourcePath("dataset", "TestJsonSerDe"), "testVariousType.json");
    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment);
    scanner.init();

    Tuple tuple = scanner.next();
    assertNotNull(tuple);
    assertNull(scanner.next());
    scanner.close();

    Tuple baseTuple = new VTuple(new Datum[] {
        DatumFactory.createBool(true),                  // 0
        DatumFactory.createChar("hyunsik"),             // 1
        DatumFactory.createInt2((short) 17),            // 2
        DatumFactory.createInt4(59),                    // 3
        DatumFactory.createInt8(23l),                   // 4
        DatumFactory.createFloat4(77.9f),               // 5
        DatumFactory.createFloat8(271.9d),              // 6
        DatumFactory.createText("hyunsik"),             // 7
        DatumFactory.createBlob("hyunsik".getBytes()),  // 8
        DatumFactory.createInet4("192.168.0.1"),        // 9
        NullDatum.get(),                                // 10
    });

    assertEquals(baseTuple, tuple);
  }
}
