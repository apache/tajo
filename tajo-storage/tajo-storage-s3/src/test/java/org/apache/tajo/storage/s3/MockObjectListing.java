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

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;

import java.util.List;

public class MockObjectListing extends ObjectListing {

  @Override
  public List<S3ObjectSummary> getObjectSummaries() {
    final String bucketName = "tajo-test";

    List<S3ObjectSummary> objectSummaries = Lists.newArrayList();
    objectSummaries.add(getS3ObjectSummary(bucketName, "test/data01", 10L));
    objectSummaries.add(getS3ObjectSummary(bucketName, "test/data02", 10L));
    objectSummaries.add(getS3ObjectSummary(bucketName, "test/data03", 10L));

    return objectSummaries;
  }

  private S3ObjectSummary getS3ObjectSummary(String bucketName, String key, long size) {
    S3ObjectSummary objectSummary = new S3ObjectSummary();
    objectSummary.setBucketName(bucketName);
    objectSummary.setKey(key);
    objectSummary.setSize(size);
    return objectSummary;
  }
}
