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

package org.apache.tajo.storage.parquet;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestReadWrite {
  private static final String HELLO = "hello";

  private Path createTmpFile() throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();

    // it prevents accessing HDFS namenode of TajoTestingCluster.
    LocalFileSystem localFS = LocalFileSystem.getLocal(new Configuration());
    return localFS.makeQualified(new Path(tmp.getPath()));
  }

  private Schema createAllTypesSchema() {
    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("myboolean", Type.BOOLEAN));
    columns.add(new Column("mybit", Type.BIT));
    columns.add(new Column("mychar", Type.CHAR));
    columns.add(new Column("myint2", Type.INT2));
    columns.add(new Column("myint4", Type.INT4));
    columns.add(new Column("myint8", Type.INT8));
    columns.add(new Column("myfloat4", Type.FLOAT4));
    columns.add(new Column("myfloat8", Type.FLOAT8));
    columns.add(new Column("mytext", Type.TEXT));
    columns.add(new Column("myblob", Type.BLOB));
    columns.add(new Column("mynull", Type.NULL_TYPE));
    Column[] columnsArray = new Column[columns.size()];
    columnsArray = columns.toArray(columnsArray);
    return new Schema(columnsArray);
  }

  @Test
  public void testAll() throws Exception {
    Path file = createTmpFile();
    Schema schema = createAllTypesSchema();
    Tuple tuple = new VTuple(schema.size());
    tuple.put(0, DatumFactory.createBool(true));
    tuple.put(1, DatumFactory.createBit((byte)128));
    tuple.put(2, DatumFactory.createChar('t'));
    tuple.put(3, DatumFactory.createInt2((short)2048));
    tuple.put(4, DatumFactory.createInt4(4096));
    tuple.put(5, DatumFactory.createInt8(8192L));
    tuple.put(6, DatumFactory.createFloat4(0.2f));
    tuple.put(7, DatumFactory.createFloat8(4.1));
    tuple.put(8, DatumFactory.createText(HELLO));
    tuple.put(9, DatumFactory.createBlob(HELLO.getBytes(Charsets.UTF_8)));
    tuple.put(10, NullDatum.get());

    TajoParquetWriter writer = new TajoParquetWriter(file, schema);
    writer.write(tuple);
    writer.close();

    TajoParquetReader reader = new TajoParquetReader(file, schema);
    tuple = reader.read();

    assertNotNull(tuple);
    assertEquals(true, tuple.getBool(0));
    assertEquals((byte)128, tuple.getByte(1));
    assertTrue(String.valueOf('t').equals(String.valueOf(tuple.getChar(2))));
    assertEquals((short)2048, tuple.getInt2(3));
    assertEquals(4096, tuple.getInt4(4));
    assertEquals(8192L, tuple.getInt8(5));
    assertEquals(new Float(0.2f), new Float(tuple.getFloat4(6)));
    assertEquals(new Double(4.1), new Double(tuple.getFloat8(7)));
    assertTrue(HELLO.equals(tuple.getText(8)));
    assertArrayEquals(HELLO.getBytes(Charsets.UTF_8), tuple.getBytes(9));
    assertTrue(tuple.isBlankOrNull(10));
  }
}
