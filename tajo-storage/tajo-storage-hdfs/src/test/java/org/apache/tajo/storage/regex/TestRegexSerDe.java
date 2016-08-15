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

package org.apache.tajo.storage.regex;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.tajo.schema.Field;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

import static org.apache.tajo.schema.QualifiedIdentifier.$;
import static org.apache.tajo.type.Type.Text;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestRegexSerDe {
  private Schema schema;
  private Tuple[] rows;
  private Path testDir;
  private String apacheWeblogPattern;

  @Before
  public void setup() throws IOException {
    apacheWeblogPattern = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") " +
        "(-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\"))?";

    Field f1 = Field.Field($("host"), Text);
    Field f2 = Field.Field($("identity"), Text);
    Field f3 = Field.Field($("user"), Text);
    Field f4 = Field.Field($("time"), Text);
    Field f5 = Field.Field($("request"), Text);
    Field f6 = Field.Field($("status"), Text);
    Field f7 = Field.Field($("size"), Text);
    Field f8 = Field.Field($("referer"), Text);
    Field f9 = Field.Field($("agent"), Text);

    schema = SchemaBuilder.builder().addAll2(
        org.apache.tajo.schema.Schema.Schema(f1, f2, f3, f4, f5, f6, f7, f8, f9)).build();

    rows = new VTuple[]{new VTuple(new Datum[]{
        DatumFactory.createText("127.0.0.1"),
        DatumFactory.createText("-"),
        DatumFactory.createText("frank"),
        DatumFactory.createText("[10/Oct/2000:13:55:36 -0700]"),
        DatumFactory.createText("\"GET /apache_pb.gif HTTP/1.0\""),
        DatumFactory.createText("200"),
        DatumFactory.createText("2326"),
        NullDatum.get(),
        NullDatum.get(),
    }), new VTuple(new Datum[]{
        DatumFactory.createText("127.0.0.1"),
        DatumFactory.createText("-"),
        DatumFactory.createText("frank"),
        DatumFactory.createText("[10/Oct/2000:13:55:36 -0700]"),
        DatumFactory.createText("\"GET /apache_pb.gif HTTP/1.0\""),
        DatumFactory.createText("200"),
        DatumFactory.createText("2326"),
        DatumFactory.createText("-"),
        DatumFactory.createText("\"Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/525.19 " +
            "(KHTML, like Gecko) Chrome/1.0.154.65 Safari/525.19\""),
    })};

    final String TEST_PATH = "target/test-data/TestStorages";
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
  }

  @After
  public void tearDown() throws IOException {
    FileSystem.getLocal(new Configuration()).delete(testDir, true);
  }

  public static Path getResourcePath(String path, String suffix) {
    URL resultBaseURL = ClassLoader.getSystemResource(path);
    return new Path(resultBaseURL.toString(), suffix);
  }

  @Test
  public void testApacheAccessLogScanner() throws IOException {
    TajoConf conf = new TajoConf();

    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.REGEX, conf);
    Path tablePath = new Path(getResourcePath("dataset", "TestRegexSerDe"), "access.log");
    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
    meta.putProperty(StorageConstants.TEXT_REGEX, apacheWeblogPattern);
    scanner.init();

    Tuple tuple = scanner.next();
    assertEquals(rows[0], tuple);

    assertNotNull(tuple = scanner.next());
    assertEquals(rows[1], tuple);

    scanner.close();
  }

  @Test
  public void testProjection() throws IOException {
    Schema target = SchemaBuilder.builder()
        .add("time", TajoDataTypes.Type.TEXT)
        .add("status", TajoDataTypes.Type.TEXT)
        .build();

    Tuple[] rows = new VTuple[]{new VTuple(new Datum[]{
        DatumFactory.createText("[10/Oct/2000:13:55:36 -0700]"),
        DatumFactory.createText("200")
    }), new VTuple(new Datum[]{
        DatumFactory.createText("[10/Oct/2000:13:55:36 -0700]"),
        DatumFactory.createText("200")
    })};

    TajoConf conf = new TajoConf();

    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.REGEX, conf);
    Path tablePath = new Path(getResourcePath("dataset", "TestRegexSerDe"), "access.log");
    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, target);
    meta.putProperty(StorageConstants.TEXT_REGEX, apacheWeblogPattern);
    scanner.init();

    Tuple tuple = scanner.next();
    assertEquals(2, tuple.size());
    assertEquals(rows[0], tuple);

    assertNotNull(tuple = scanner.next());
    assertEquals(2, tuple.size());
    assertEquals(rows[1], tuple);

    scanner.close();
  }

  @Test
  public void testSerializer() throws IOException {
    TajoConf conf = new TajoConf();

    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.REGEX, conf);
    meta.putProperty(StorageConstants.TEXT_REGEX_OUTPUT_FORMAT_STRING, "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s");

    FileTablespace sm = TablespaceManager.getLocalFs();
    Path tablePath = new Path(testDir, "testSerializer.data");
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();

    appender.addTuple(rows[0]);
    appender.addTuple(rows[1]);
    appender.close();

    FileStatus status = tablePath.getFileSystem(conf).getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());

    meta = CatalogUtil.newTableMeta(BuiltinStorages.REGEX, conf);
    meta.putProperty(StorageConstants.TEXT_REGEX, apacheWeblogPattern);
    Scanner scanner =  sm.getScanner(meta, schema, fragment, null);
    scanner.init();

    Tuple tuple = scanner.next();
    assertEquals(rows[0], tuple);
    assertNotNull(tuple = scanner.next());
    assertEquals(rows[1], tuple);
    assertNull(scanner.next());
    scanner.close();
  }
}
