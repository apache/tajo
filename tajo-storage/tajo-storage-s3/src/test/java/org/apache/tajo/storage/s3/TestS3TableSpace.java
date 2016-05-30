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

package org.apache.tajo.storage.s3;

import net.minidev.json.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

public class TestS3TableSpace {
  public static final String S3_SPACENAME = "s3_cluster";
  public static final String S3N_SPACENAME = "s3N_cluster";
  public static final String S3A_SPACENAME = "s3A_cluster";

  public static final String S3_URI = "s3://tajo-test/";
  public static final String S3N_URI = "s3n://tajo-test/";
  public static final String S3A_URI = "s3a://tajo-test/";

  @BeforeClass
  public static void setUp() throws Exception {
    // Add tablespace for s3 prefix
    S3TableSpace s3TableSpace = new S3TableSpace(S3_SPACENAME, URI.create(S3_URI), new JSONObject());
    TajoConf tajoConf = new TajoConf();
    tajoConf.set("fs.s3.impl", MockS3FileSystem.class.getName());
    tajoConf.set("fs.s3.awsAccessKeyId", "test_access_key_id");
    tajoConf.set("fs.s3.awsSecretAccessKey", "test_secret_access_key");
    s3TableSpace.init(tajoConf);
    TablespaceManager.addTableSpaceForTest(s3TableSpace);

    // Add tablespace for s3n prefix
    S3TableSpace s3nTableSpace = new S3TableSpace(S3N_SPACENAME, URI.create(S3N_URI), new JSONObject());
    tajoConf = new TajoConf();
    tajoConf.set("fs.s3n.impl", MockS3FileSystem.class.getName());
    tajoConf.set("fs.s3n.awsAccessKeyId", "test_access_key_id");
    tajoConf.set("fs.s3n.awsSecretAccessKey", "test_secret_access_key");
    s3nTableSpace.init(tajoConf);
    TablespaceManager.addTableSpaceForTest(s3nTableSpace);

    // Add tablespace for s3a prefix
    S3TableSpace s3aTableSpace = new S3TableSpace(S3A_SPACENAME, URI.create(S3A_URI), new JSONObject());
    tajoConf = new TajoConf();
    tajoConf.set("fs.s3a.impl", MockS3FileSystem.class.getName());
    tajoConf.set("fs.s3a.access.key", "test_access_key_id");
    tajoConf.set("fs.s3a.secret.key", "test_secret_access_key");
    s3aTableSpace.init(tajoConf);
    TablespaceManager.addTableSpaceForTest(s3aTableSpace);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    TablespaceManager.removeTablespaceForTest(S3_SPACENAME);
    TablespaceManager.removeTablespaceForTest(S3N_SPACENAME);
    TablespaceManager.removeTablespaceForTest(S3A_SPACENAME);
  }

  @Test
  public void testTablespaceHandler() throws Exception {
    // Verify the tablespace for s3 prefix
    assertTrue((TablespaceManager.getByName(S3_SPACENAME)) instanceof S3TableSpace);
    assertEquals(S3_SPACENAME, (TablespaceManager.getByName(S3_SPACENAME).getName()));
    assertTrue((TablespaceManager.get(S3_URI)) instanceof S3TableSpace);
    assertEquals(S3_URI, TablespaceManager.get(S3_URI).getUri().toASCIIString());

    // Verify the tablespace for s3n prefix
    assertTrue((TablespaceManager.getByName(S3N_SPACENAME)) instanceof S3TableSpace);
    assertEquals(S3N_SPACENAME, (TablespaceManager.getByName(S3N_SPACENAME).getName()));
    assertTrue((TablespaceManager.get(S3N_URI)) instanceof S3TableSpace);
    assertEquals(S3N_URI, TablespaceManager.get(S3N_URI).getUri().toASCIIString());

    // Verify the tablespace for s3a prefix
    assertTrue((TablespaceManager.getByName(S3A_SPACENAME)) instanceof S3TableSpace);
    assertEquals(S3A_SPACENAME, (TablespaceManager.getByName(S3A_SPACENAME).getName()));
    assertTrue((TablespaceManager.get(S3A_URI)) instanceof S3TableSpace);
    assertEquals(S3A_URI, TablespaceManager.get(S3A_URI).getUri().toASCIIString());
  }

  @Test
  public void testCalculateSizeWithS3Prefix() throws Exception {
    Path path = new Path(S3_URI, "test");
    assertTrue((TablespaceManager.getByName(S3_SPACENAME)) instanceof S3TableSpace);
    S3TableSpace tableSpace = TablespaceManager.get(path.toUri());
    tableSpace.setAmazonS3Client(new MockAmazonS3());
    long size = tableSpace.calculateSize(path);
    assertEquals(30L, size);
  }

  @Test
  public void testCalculateSizeWithS3NPrefix() throws Exception {
    Path path = new Path(S3N_URI, "test");
    assertTrue((TablespaceManager.getByName(S3N_SPACENAME)) instanceof S3TableSpace);
    S3TableSpace tableSpace = TablespaceManager.get(path.toUri());
    tableSpace.setAmazonS3Client(new MockAmazonS3());
    long size = tableSpace.calculateSize(path);
    assertEquals(30L, size);
  }

  @Test
  public void testCalculateSizeWithS3APrefix() throws Exception {
    Path path = new Path(S3A_URI, "test");
    assertTrue((TablespaceManager.getByName(S3A_SPACENAME)) instanceof S3TableSpace);
    S3TableSpace tableSpace = TablespaceManager.get(path.toUri());
    tableSpace.setAmazonS3Client(new MockAmazonS3());
    long size = tableSpace.calculateSize(path);
    assertEquals(30L, size);
  }

}
