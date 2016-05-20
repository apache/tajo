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

import java.io.IOException;
import java.net.URI;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.FileTablespace;

import net.minidev.json.JSONObject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static org.apache.tajo.storage.s3.TajoS3Constants.*;

public class S3TableSpace extends FileTablespace {
  private final static Log LOG = LogFactory.getLog(S3TableSpace.class);

  private AmazonS3 s3;
  private int maxKeys;

  public S3TableSpace(String spaceName, URI uri, JSONObject config) {
    super(spaceName, uri, config);
  }

  @Override
  public void init(TajoConf tajoConf) throws IOException {
    super.init(tajoConf);

    maxKeys = conf.getInt(MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS);
    int maxErrorRetries = conf.getInt(MAX_ERROR_RETRIES, DEFAULT_MAX_ERROR_RETRIES);
    boolean sslEnabled = conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS);

    int connectTimeout = conf.getInt(ESTABLISH_TIMEOUT, DEFAULT_ESTABLISH_TIMEOUT);
    int socketTimeout = conf.getInt(SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
    int maxConnections = conf.getInt(MAXIMUM_CONNECTIONS, DEFAULT_MAXIMUM_CONNECTIONS);

    ClientConfiguration configuration = new ClientConfiguration()
      .withMaxErrorRetry(maxErrorRetries)
      .withProtocol(sslEnabled ? Protocol.HTTPS : Protocol.HTTP)
      .withConnectionTimeout(connectTimeout)
      .withSocketTimeout(socketTimeout)
      .withMaxConnections(maxConnections);

    this.s3 = createAmazonS3Client(uri, conf, configuration);

    if (s3 != null) {
      String endPoint = conf.getTrimmed(ENDPOINT,"");
      try {
        if (!endPoint.isEmpty()) {
          s3.setEndpoint(endPoint);
        }
      } catch (IllegalArgumentException e) {
        String msg = "Incorrect endpoint: "  + e.getMessage();
        LOG.error(msg);
        throw new IllegalArgumentException(msg, e);
      }

      LOG.info("Amazon3Client is initialized.");
    }
  }

  private AmazonS3Client createAmazonS3Client(URI uri, Configuration hadoopConfig, ClientConfiguration clientConfig) {
    AWSCredentialsProvider credentials = getAwsCredentialsProvider(uri, hadoopConfig);
    AmazonS3Client client = new AmazonS3Client(credentials, clientConfig);
    return client;
  }

  private AWSCredentialsProvider getAwsCredentialsProvider(URI uri, Configuration conf) {
    // first try credentials from URI or static properties
    try {
      return new StaticCredentialsProvider(getAwsCredentials(uri, conf));
    } catch (IllegalArgumentException ignored) {
    }

    return new InstanceProfileCredentialsProvider();
  }

  private static AWSCredentials getAwsCredentials(URI uri, Configuration conf) {
    TajoS3Credentials credentials = new TajoS3Credentials();
    credentials.initialize(uri, conf);
    return new BasicAWSCredentials(credentials.getAccessKey(), credentials.getSecretAccessKey());
  }

  @Override
  public long calculateSize(Path path) throws IOException {
    String key = keyFromPath(path);

    final FileStatus fileStatus =  fs.getFileStatus(path);
    long totalBucketSize = 0L;

    if (fileStatus.isDirectory()) {
      if (!key.isEmpty()) {
        key = key + "/";
      }

      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(uri.getHost());
      request.setPrefix(key);
      request.setMaxKeys(maxKeys);

      if (LOG.isDebugEnabled()) {
        LOG.debug("listStatus: doing listObjects for directory " + key);
      }

      ObjectListing objects = s3.listObjects(request);

      while (true) {
        for (S3ObjectSummary summary : objects.getObjectSummaries()) {
          Path keyPath = keyToPath(summary.getKey()).makeQualified(uri, fs.getWorkingDirectory());

          // Skip over keys that are ourselves and old S3N _$folder$ files
          if (keyPath.equals(path) || summary.getKey().endsWith(S3N_FOLDER_SUFFIX)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Ignoring: " + keyPath);
            }
            continue;
          }

          if (!objectRepresentsDirectory(summary.getKey(), summary.getSize())) {
            totalBucketSize += summary.getSize();
          }
        }

        if (objects.isTruncated()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("listStatus: list truncated - getting next batch");
          }
          objects = s3.listNextBatchOfObjects(objects);
        } else {
          break;
        }
      }
    } else {
      return fileStatus.getLen();
    }

    return totalBucketSize;
  }

  private boolean objectRepresentsDirectory(final String name, final long size) {
    return !name.isEmpty() && name.charAt(name.length() - 1) == '/' && size == 0L;
  }

  private Path keyToPath(String key) {
    return new Path("/" + key);
  }

  private String keyFromPath(Path path)
  {
    checkArgument(path.isAbsolute(), "Path is not absolute: %s", path);
    String key = nullToEmpty(path.toUri().getPath());
    if (key.startsWith("/")) {
      key = key.substring(1);
    }
    if (key.endsWith("/")) {
      key = key.substring(0, key.length() - 1);
    }
    return key;
  }

  @VisibleForTesting
  public AmazonS3 getAmazonS3Client() {
    return s3;
  }

  @VisibleForTesting
  public void setAmazonS3Client(AmazonS3 s3) {
    this.s3 = s3;
  }
}
