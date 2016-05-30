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
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.FileTablespace;

import net.minidev.json.JSONObject;

import static org.apache.tajo.storage.s3.TajoS3Constants.*;

public class S3TableSpace extends FileTablespace {
  private final static Log LOG = LogFactory.getLog(S3TableSpace.class);

  private AmazonS3 s3;
  private boolean s3Enabled;
  private int maxKeys;

  public S3TableSpace(String spaceName, URI uri, JSONObject config) {
    super(spaceName, uri, config);
  }

  @Override
  public void init(TajoConf tajoConf) throws IOException {
    super.init(tajoConf);

    try {
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

      s3Enabled = true;
    } catch (NoClassDefFoundError defFoundError) {
      // If the version of hadoop is less than 2.6.0, hadoop doesn't include aws dependencies because it doesn't provide
      // S3AFileSystem. In this case, tajo never uses aws s3 api directly.
      LOG.warn(defFoundError);
      s3Enabled = false;
    } catch (Exception e) {
      throw new TajoInternalError(e);
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
    long totalBucketSize = 0L;

    if (s3Enabled) {
      String key = pathToKey(path);

      final FileStatus fileStatus =  fs.getFileStatus(path);

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
    } else {
      totalBucketSize = fs.getContentSummary(path).getLength();
    }

    return totalBucketSize;
  }

  private boolean objectRepresentsDirectory(final String name, final long size) {
    return !name.isEmpty() && name.charAt(name.length() - 1) == '/' && size == 0L;
  }

  private Path keyToPath(String key) {
    return new Path("/" + key);
  }

  /* Turns a path (relative or otherwise) into an S3 key
   */
  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(fs.getWorkingDirectory(), path);
    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }

  @VisibleForTesting
  public void setAmazonS3Client(AmazonS3 s3) {
    this.s3 = s3;
  }
}
