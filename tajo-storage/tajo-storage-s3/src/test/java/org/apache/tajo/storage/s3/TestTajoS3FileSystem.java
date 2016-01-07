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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.junit.Assert.assertEquals;

public class TestTajoS3FileSystem {

  @Test
  public void testInstanceCredentialsEnabled() throws Exception {
    // the static credentials should be preferred
    try (TajoS3FileSystem fs = new TajoS3FileSystem()) {
      fs.initialize(new URI("s3n://test-bucket/"), getConfiguration());
      assertInstanceOf(getAwsCredentialsProvider(fs), InstanceProfileCredentialsProvider.class);
    }
  }

  @Test
  public void testInitialization() throws IOException {
    initializationTest("s3://a:b@c", "s3://a:b@c");
    initializationTest("s3://a:b@c/", "s3://a:b@c");
    initializationTest("s3://a:b@c/path", "s3://a:b@c");
    initializationTest("s3://a@c", "s3://a@c");
    initializationTest("s3://a@c/", "s3://a@c");
    initializationTest("s3://a@c/path", "s3://a@c");
    initializationTest("s3://c", "s3://c");
    initializationTest("s3://c/", "s3://c");
    initializationTest("s3://c/path", "s3://c");
  }

  private void initializationTest(String initializationUri, String expectedUri)
    throws IOException {
    TajoS3FileSystem fs = new TajoS3FileSystem();
    fs.initialize(URI.create(initializationUri), getConfiguration());
    assertEquals(URI.create(expectedUri), fs.getUri());
  }

  private Configuration getConfiguration() {
    Configuration config = new Configuration();
    config.set("fs.s3a.access.key", "test_access_key_id");
    config.set("fs.s3a.secret.key", "test_secret_access_key");

    return config;
  }

  private static AWSCredentialsProvider getAwsCredentialsProvider(TajoS3FileSystem fs) {
    return getFieldValue(fs.getS3Client(), "awsCredentialsProvider", AWSCredentialsProvider.class);
  }

  @SuppressWarnings("unchecked")
  private static <T> T getFieldValue(Object instance, String name, Class<T> type) {
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
}