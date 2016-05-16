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
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.FileFragment;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.*;

public class TestJsonSerDe {
  private static Schema schema;

  static {
    schema = SchemaBuilder.builder()
        .add("col1", TajoDataTypes.Type.BOOLEAN)
        .add("col2", CatalogUtil.newDataTypeWithLen(TajoDataTypes.Type.CHAR, 7))
        .add("col3", TajoDataTypes.Type.INT2)
        .add("col4", TajoDataTypes.Type.INT4)
        .add("col5", TajoDataTypes.Type.INT8)
        .add("col6", TajoDataTypes.Type.FLOAT4)
        .add("col7", TajoDataTypes.Type.FLOAT8)
        .add("col8", TajoDataTypes.Type.TEXT)
        .add("col9", TajoDataTypes.Type.BLOB)
        .add("col10", TajoDataTypes.Type.NULL_TYPE)
        .build();
  }

  public static Path getResourcePath(String path, String suffix) {
    URL resultBaseURL = ClassLoader.getSystemResource(path);
    return new Path(resultBaseURL.toString(), suffix);
  }

  @Test
  public void testVarioutType() throws IOException {
    TajoConf conf = new TajoConf();

    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.JSON, conf);
    Path tablePath = new Path(getResourcePath("dataset", "TestJsonSerDe"), "testVariousType.json");
    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
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
        DatumFactory.createInt8(23L),                   // 4
        DatumFactory.createFloat4(77.9f),               // 5
        DatumFactory.createFloat8(271.9d),              // 6
        DatumFactory.createText("hyunsik"),             // 7
        DatumFactory.createBlob("hyunsik".getBytes()),  // 8
        NullDatum.get(),                                // 9
    });

    assertEquals(baseTuple, tuple);
  }

  @Test
  public void testUnicodeWithControlChar() throws IOException {
    TajoConf conf = new TajoConf();

    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.JSON, conf);
    Path tablePath = new Path(getResourcePath("dataset", "TestJsonSerDe"), "testUnicodeWithControlChar.json");
    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());

    Schema schema = SchemaBuilder.builder()
        .add("col1", TajoDataTypes.Type.TEXT)
        .add("col2", TajoDataTypes.Type.TEXT)
        .add("col3", TajoDataTypes.Type.TEXT)
        .build();
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
    scanner.init();

    Tuple tuple = scanner.next();
    assertNotNull(tuple);
    assertNull(scanner.next());
    scanner.close();


    Tuple baseTuple = new VTuple(new Datum[] {
        DatumFactory.createText("tajo"),
        DatumFactory.createText("타조"),
        DatumFactory.createText("타\n조")
    });

    assertEquals(baseTuple, tuple);
  }
}
