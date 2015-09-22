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

package org.apache.tajo.storage;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.*;

public class TestDelimitedTextFile {
  private static final Log LOG = LogFactory.getLog(TestDelimitedTextFile.class);

  private static Schema schema = new Schema();

  private static Tuple baseTuple;

  static {
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.CHAR, 7);
    schema.addColumn("col3", Type.INT2);
    schema.addColumn("col4", Type.INT4);
    schema.addColumn("col5", Type.INT8);
    schema.addColumn("col6", Type.FLOAT4);
    schema.addColumn("col7", Type.FLOAT8);
    schema.addColumn("col8", Type.TEXT);
    schema.addColumn("col9", Type.BLOB);
    schema.addColumn("col10", Type.INET4);

    baseTuple = new VTuple(new Datum[] {
        DatumFactory.createBool(true),                // 0
        DatumFactory.createChar("hyunsik"),           // 1
        DatumFactory.createInt2((short) 17),          // 2
        DatumFactory.createInt4(59),                  // 3
        DatumFactory.createInt8(23l),                 // 4
        DatumFactory.createFloat4(77.9f),             // 5
        DatumFactory.createFloat8(271.9d),            // 6
        DatumFactory.createText("hyunsik"),           // 7
        DatumFactory.createBlob("hyunsik".getBytes()),// 8
        DatumFactory.createInet4("192.168.0.1"),      // 9
    });
  }

  public static Path getResourcePath(String path, String suffix) {
    URL resultBaseURL = ClassLoader.getSystemResource(path);
    return new Path(resultBaseURL.toString(), suffix);
  }

  public static Path getResultPath(Class clazz, String fileName) {
    return new Path (getResourcePath("results", clazz.getSimpleName()), fileName);
  }

  public static String getResultText(Class clazz, String fileName) throws IOException {
    FileSystem localFS = FileSystem.getLocal(new Configuration());
    Path path = getResultPath(clazz, fileName);
    Preconditions.checkState(localFS.exists(path) && localFS.isFile(path));
    return FileUtil.readTextFile(new File(path.toUri()));
  }

  private static final FileFragment getFileFragment(String fileName) throws IOException {
    TajoConf conf = new TajoConf();
    Path tablePath = new Path(getResourcePath("dataset", "TestDelimitedTextFile"), fileName);
    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(tablePath);
    return new FileFragment("table", tablePath, 0, status.getLen());
  }

  @Test
  public void testIgnoreAllErrors() throws IOException {
    TajoConf conf = new TajoConf();

    TableMeta meta = CatalogUtil.newTableMeta("JSON");
    meta.putOption(StorageUtil.TEXT_ERROR_TOLERANCE_MAXNUM, "-1");
    FileFragment fragment =  getFileFragment("testErrorTolerance1.json");
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
    scanner.init();

    Tuple tuple;
    int i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(baseTuple, tuple);
      i++;
    }
    assertEquals(3, i);
    scanner.close();
  }

  @Test
  public void testIgnoreOneErrorTolerance() throws IOException {


    TajoConf conf = new TajoConf();

    TableMeta meta = CatalogUtil.newTableMeta("JSON");
    meta.putOption(StorageUtil.TEXT_ERROR_TOLERANCE_MAXNUM, "1");
    FileFragment fragment =  getFileFragment("testErrorTolerance1.json");
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
    scanner.init();

    assertNotNull(scanner.next());
    assertNotNull(scanner.next());
    try {
      scanner.next();
    } catch (IOException ioe) {
      LOG.error(ioe);
      return;
    } finally {
      scanner.close();
    }
    fail();
  }

  @Test
  public void testNoErrorTolerance() throws IOException {
    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta("JSON");
    meta.putOption(StorageUtil.TEXT_ERROR_TOLERANCE_MAXNUM, "0");
    FileFragment fragment =  getFileFragment("testErrorTolerance2.json");
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
    scanner.init();

    try {
      scanner.next();
    } catch (IOException ioe) {
      return;
    } finally {
      scanner.close();
    }
    fail();
  }

  @Test
  public void testIgnoreTruncatedValueErrorTolerance() throws IOException {
    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta("JSON");
    meta.putOption(StorageUtil.TEXT_ERROR_TOLERANCE_MAXNUM, "1");
    FileFragment fragment = getFileFragment("testErrorTolerance3.json");
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
    scanner.init();

    try {
      Tuple tuple = scanner.next();
      assertNull(tuple);
    } finally {
      scanner.close();
    }
  }

  @Test
  public void testSkippingHeaderWithJson() throws IOException {
    TableMeta meta = CatalogUtil.newTableMeta("JSON");
    meta.putOption(StorageConstants.TEXT_SKIP_HEADER_LINE, "2");
    FileFragment fragment = getFileFragment("testNormal.json");
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);

    scanner.init();

    int lines = 0;

    try {
      while (true) {
        Tuple tuple = scanner.next();
        if (tuple != null) {
          assertEquals(19+lines, tuple.getInt2(2));
          lines++;
        }
        else break;
      }
    } finally {
      assertEquals(4, lines);
      scanner.close();
    }
  }

  @Test
  public void testSkippingHeaderWithText() throws IOException {
    TableMeta meta = CatalogUtil.newTableMeta("TEXT");
    meta.putOption(StorageConstants.TEXT_SKIP_HEADER_LINE, "1");
    meta.putOption(StorageConstants.TEXT_DELIMITER, ",");
    FileFragment fragment = getFileFragment("testSkip.txt");
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
    
    scanner.init();

    int lines = 0;

    try {
      while (true) {
        Tuple tuple = scanner.next();
        if (tuple != null) {
          assertEquals(17+lines, tuple.getInt2(2));
          lines++;
        }
        else break;
      }
    } finally {
      assertEquals(6, lines);
      scanner.close();
    }
  }
}
