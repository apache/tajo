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

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.base.Throwables;
import net.minidev.json.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.*;

public class TestS3TableSpace {
  public static final String SPACENAME = "s3_cluster";
  public static final String S3_URI = "s3://tajo-test/";

  @BeforeClass
  public static void setUp() throws Exception {
    S3TableSpace tablespace = new S3TableSpace(SPACENAME, URI.create(S3_URI), new JSONObject());

    TajoConf tajoConf = new TajoConf();
    tajoConf.set("fs.s3.impl", MockS3FileSystem.class.getName());
    tajoConf.set("fs.s3a.access.key", "test_access_key_id");
    tajoConf.set("fs.s3a.secret.key", "test_secret_access_key");
    tablespace.init(tajoConf);
    tablespace.setAmazonS3Client(new MockAmazonS3());

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

  @Test
  public void testInstanceCredentialsEnabled() throws Exception {
    assertTrue((TablespaceManager.getByName(SPACENAME)) instanceof S3TableSpace);
    S3TableSpace tableSpace = TablespaceManager.getByName(SPACENAME);

    assertNotNull(tableSpace.getAmazonS3Client());
    assertTrue((tableSpace.getAmazonS3Client()) instanceof AmazonS3Client);

    assertTrue(getAwsCredentialsProvider(tableSpace.getAmazonS3Client())
      instanceof InstanceProfileCredentialsProvider);
  }

  private AWSCredentialsProvider getAwsCredentialsProvider(AmazonS3 s3) {
    return getFieldValue(s3, "awsCredentialsProvider", AWSCredentialsProvider.class);
  }

  @SuppressWarnings("unchecked")
  private <T> T getFieldValue(Object instance, String name, Class<T> type) {
    try {
      Field field = instance.getClass().getDeclaredField(name);
      checkArgument(field.getType() == type, "expected %s but found %s", type, field.getType());
      field.setAccessible(true);
      return (T) field.get(instance);
    }
    catch (ReflectiveOperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Test(expected = AmazonClientException.class)
  public void testCalculateSize() throws Exception {
    assertTrue((TablespaceManager.getByName(SPACENAME)) instanceof S3TableSpace);
    S3TableSpace tableSpace = TablespaceManager.getByName(SPACENAME);

    assertNotNull(tableSpace.getAmazonS3Client());
    assertTrue((tableSpace.getAmazonS3Client()) instanceof AmazonS3Client);
    tableSpace.calculateSize(new Path("s3n://test-bucket/test"));
  }
}
