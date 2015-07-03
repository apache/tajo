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

package org.apache.tajo.storage.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.orc.objectinspector.ObjectInspectorFactory;
import org.apache.tajo.util.KeyValueSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URL;
import java.util.List;

public class TestOrc {
  private OrcScanner orcScanner;

  public static Path getResourcePath(String path, String suffix) {
    URL resultBaseURL = ClassLoader.getSystemResource(path);
    return new Path(resultBaseURL.toString(), suffix);
  }

  private static FileFragment getFileFragment(Configuration conf, String fileName) throws IOException {
    Path tablePath = new Path(getResourcePath("dataset", "."), fileName);
    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(tablePath);
    return new FileFragment("table", tablePath, 0, status.getLen());
  }

  @Before
  public void setup() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("userid", TajoDataTypes.Type.INT4);
    schema.addColumn("movieid", TajoDataTypes.Type.INT4);
    schema.addColumn("rating", TajoDataTypes.Type.INT2);
    schema.addColumn("unixtimestamp", TajoDataTypes.Type.TEXT);
    schema.addColumn("faketime", TajoDataTypes.Type.TIMESTAMP);

    Configuration conf = new TajoConf();

    TableMeta meta = new TableMeta(CatalogProtos.StoreType.ORC, new KeyValueSet());

    Fragment fragment = getFileFragment(conf, "u_data_20.orc");

    orcScanner = new OrcScanner(conf, schema, meta, fragment);

    orcScanner.init();
  }

  @Test
  public void testReadTuple() {
    try {
      Tuple tuple = orcScanner.next();

      assertEquals(tuple.getInt4(0), 196);
      assertEquals(tuple.getInt4(1), 242);
      assertEquals(tuple.getInt2(2), 3);
      assertEquals(tuple.getText(3), "881250949");

      // Timestamp test
      TimestampDatum timestamp = (TimestampDatum)tuple.get(4);

      assertEquals(timestamp.getYear(), 2008);
      assertEquals(timestamp.getMonthOfYear(), 12);
      assertEquals(timestamp.getDayOfMonth(), 12);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @After
  public void end() {
    try {
      orcScanner.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testWrite() {
    Schema schema = new Schema();
    schema.addColumn("movieid", TajoDataTypes.Type.INT4);
    schema.addColumn("rating", TajoDataTypes.Type.INT2);
    schema.addColumn("comment", TajoDataTypes.Type.TEXT);
    schema.addColumn("showtime", TajoDataTypes.Type.TIMESTAMP);

    StructObjectInspector structOI = ObjectInspectorFactory.buildStructObjectInspector(schema);
    List<? extends StructField> fieldList = structOI.getAllStructFieldRefs();
    StructField midField = fieldList.get(0);

    assertEquals("movieid", midField.getFieldName());
  }
}