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

package org.apache.tajo.storage.avro;

import org.apache.avro.Schema;
import org.apache.tajo.HttpFileServer;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.JavaResourceUtil;
import org.apache.tajo.util.NetUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.*;

/**
 * Tests for {@link org.apache.tajo.storage.avro.AvroUtil}.
 */
public class TestAvroUtil {
  private Schema expected;
  private URL schemaUrl;

  @Before
  public void setUp() throws Exception {
    schemaUrl = JavaResourceUtil.getResourceURL("dataset/testVariousTypes.avsc");
    assertNotNull(schemaUrl);

    File file = new File(schemaUrl.getPath());
    assertTrue(file.exists());

    expected = new Schema.Parser().parse(file);
  }

  @Test
  public void testGetSchema() throws IOException, URISyntaxException {
    TableMeta meta = CatalogUtil.newTableMeta("AVRO");
    meta.putOption(StorageConstants.AVRO_SCHEMA_LITERAL, FileUtil.readTextFile(new File(schemaUrl.getPath())));
    Schema schema = AvroUtil.getAvroSchema(meta, new TajoConf());
    assertEquals(expected, schema);

    meta = CatalogUtil.newTableMeta("AVRO");
    meta.putOption(StorageConstants.AVRO_SCHEMA_URL, schemaUrl.getPath());
    schema = AvroUtil.getAvroSchema(meta, new TajoConf());
    assertEquals(expected, schema);

    HttpFileServer server = new HttpFileServer(NetUtils.createSocketAddr("127.0.0.1:0"));
    try {
      server.start();
      InetSocketAddress addr = server.getBindAddress();

      String url = "http://127.0.0.1:" + addr.getPort() + schemaUrl.getPath();
      meta = CatalogUtil.newTableMeta("AVRO");
      meta.putOption(StorageConstants.AVRO_SCHEMA_URL, url);
      schema = AvroUtil.getAvroSchema(meta, new TajoConf());
    } finally {
      server.stop();
    }
    assertEquals(expected, schema);
  }

  @Test
  public void testGetSchemaFromHttp() throws IOException, URISyntaxException {
    HttpFileServer server = new HttpFileServer(NetUtils.createSocketAddr("127.0.0.1:0"));
    try {
      server.start();
      InetSocketAddress addr = server.getBindAddress();

      Schema schema = AvroUtil.getAvroSchemaFromHttp("http://127.0.0.1:" + addr.getPort() + schemaUrl.getPath());
      assertEquals(expected, schema);
    } finally {
      server.stop();
    }
  }

  @Test
  public void testGetSchemaFromFileSystem() throws IOException, URISyntaxException {
    Schema schema = AvroUtil.getAvroSchemaFromFileSystem(schemaUrl.toString(), new TajoConf());

    assertEquals(expected, schema);
  }
}
