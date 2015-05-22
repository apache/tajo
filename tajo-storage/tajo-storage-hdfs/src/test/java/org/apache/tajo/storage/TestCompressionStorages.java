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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.sequencefile.SequenceFileScanner;
import org.apache.tajo.storage.text.DelimitedTextFile;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestCompressionStorages {
  private TajoConf conf;
  private static String TEST_PATH = "target/test-data/TestCompressionStorages";

  private String storeType;
  private Path testDir;
  private FileSystem fs;

  public TestCompressionStorages(String type) throws IOException {
    this.storeType = type;
    conf = new TajoConf();

    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {"CSV"},
        {"RCFILE"},
        {"SEQUENCEFILE"},
        {"TEXT"}
    });
  }

  @Test
  public void testDeflateCodecCompressionData() throws IOException {
    storageCompressionTest(storeType, DeflateCodec.class);
  }

  @Test
  public void testGzipCodecCompressionData() throws IOException {
    if (storeType.equalsIgnoreCase("RCFILE")) {
      if( ZlibFactory.isNativeZlibLoaded(conf)) {
        storageCompressionTest(storeType, GzipCodec.class);
      }
    } else if (storeType.equalsIgnoreCase("SEQUENCEFILE")) {
      if( ZlibFactory.isNativeZlibLoaded(conf)) {
        storageCompressionTest(storeType, GzipCodec.class);
      }
    } else {
      storageCompressionTest(storeType, GzipCodec.class);
    }
  }

  @Test
  public void testSnappyCodecCompressionData() throws IOException {
    if (SnappyCodec.isNativeCodeLoaded()) {
      storageCompressionTest(storeType, SnappyCodec.class);
    }
  }

  @Test
  public void testLz4CodecCompressionData() throws IOException {
    if(NativeCodeLoader.isNativeCodeLoaded() && Lz4Codec.isNativeCodeLoaded())
    storageCompressionTest(storeType, Lz4Codec.class);
  }

  private void storageCompressionTest(String storeType, Class<? extends CompressionCodec> codec) throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.FLOAT4);
    schema.addColumn("name", Type.TEXT);

    TableMeta meta = CatalogUtil.newTableMeta(storeType);
    meta.putOption("compression.codec", codec.getCanonicalName());
    meta.putOption("compression.type", SequenceFile.CompressionType.BLOCK.name());
    meta.putOption("rcfile.serde", TextSerializerDeserializer.class.getName());
    meta.putOption("sequencefile.serde", TextSerializerDeserializer.class.getName());

    String fileName = "Compression_" + codec.getSimpleName();
    Path tablePath = new Path(testDir, fileName);
    Appender appender = ((FileTablespace) TableSpaceManager.getFileStorageManager(conf)).getAppender(meta, schema, tablePath);
    appender.enableStats();

    appender.init();

    String extension = "";
    if (appender instanceof CSVFile.CSVAppender) {
      extension = ((CSVFile.CSVAppender) appender).getExtension();
    } else if (appender instanceof DelimitedTextFile.DelimitedTextFileAppender) {
      extension = ((DelimitedTextFile.DelimitedTextFileAppender) appender).getExtension();
    }

    int tupleNum = 100000;
    VTuple vTuple;

    for (int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(3);
      vTuple.put(0, DatumFactory.createInt4(i + 1));
      vTuple.put(1, DatumFactory.createFloat4((float) i));
      vTuple.put(2, DatumFactory.createText(String.valueOf(i)));
      appender.addTuple(vTuple);
    }
    appender.close();

    TableStats stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());
    tablePath = tablePath.suffix(extension);
    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    FileFragment[] tablets = new FileFragment[1];
    tablets[0] = new FileFragment(fileName, tablePath, 0, fileLen);

    Scanner scanner = TableSpaceManager.getFileStorageManager(conf).getScanner(meta, schema, tablets[0], schema);

    if (storeType.equalsIgnoreCase("CSV")) {
      if (SplittableCompressionCodec.class.isAssignableFrom(codec)) {
        assertTrue(scanner.isSplittable());
      } else {
        assertFalse(scanner.isSplittable());
      }
    }
    scanner.init();

    if (storeType.equalsIgnoreCase("SEQUENCEFILE")) {
      assertTrue(scanner instanceof SequenceFileScanner);
      Writable key = ((SequenceFileScanner) scanner).getKey();
      assertEquals(key.getClass().getCanonicalName(), LongWritable.class.getCanonicalName());
    }

    int tupleCnt = 0;
    Tuple tuple;
    while ((tuple = scanner.next()) != null) {
      tupleCnt++;
    }
    scanner.close();
    assertEquals(tupleNum, tupleCnt);
    assertNotSame(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());
  }
}
