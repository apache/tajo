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
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestS3TableSpace {
  public static final String SPACENAME = "s3_cluster";
  public static final String S3_URI = "s3n://tajo-test/";

  @BeforeClass
  public static void setUp() throws Exception {
    S3TableSpace tablespace = new S3TableSpace(SPACENAME, URI.create(S3_URI), new JSONObject());

    TajoConf tajoConf = new TajoConf();
    tajoConf.set("fs.s3n.awsSecretAccessKey", "test_secret_access_key");
    tajoConf.set("fs.s3n.awsAccessKeyId", "test_access_key_id");
    tablespace.init(tajoConf);

    TablespaceManager.addTableSpaceForTest(tablespace);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    TablespaceManager.removeTablespaceForTest(SPACENAME);
  }

  @Test
  public void testTablespaceHandler() throws Exception {
    assertTrue((TablespaceManager.getByName(SPACENAME)) instanceof S3TableSpace);
    assertEquals(SPACENAME, (TablespaceManager.getByName(SPACENAME).getName()));

    assertTrue((TablespaceManager.get(S3_URI)) instanceof S3TableSpace);
    assertEquals(S3_URI, TablespaceManager.get(S3_URI).getUri().toASCIIString());
  }
}
