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
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
      // Try to get our credentials or just connect anonymously
      String accessKey = conf.get(ACCESS_KEY, null);
      String secretKey = conf.get(SECRET_KEY, null);

      String userInfo = uri.getUserInfo();
      if (userInfo != null) {
        int index = userInfo.indexOf(':');
        if (index != -1) {
          accessKey = userInfo.substring(0, index);
          secretKey = userInfo.substring(index + 1);
        } else {
          accessKey = userInfo;
        }
      }

      AWSCredentialsProviderChain credentials = new AWSCredentialsProviderChain(
        new BasicAWSCredentialsProvider(accessKey, secretKey),
        new InstanceProfileCredentialsProvider(),
        new AnonymousAWSCredentialsProvider()
      );

      ClientConfiguration awsConf = new ClientConfiguration();
      awsConf.setMaxConnections(conf.getInt(MAXIMUM_CONNECTIONS,
        DEFAULT_MAXIMUM_CONNECTIONS));
      boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS,
        DEFAULT_SECURE_CONNECTIONS);
      awsConf.setProtocol(secureConnections ?  Protocol.HTTPS : Protocol.HTTP);
      awsConf.setMaxErrorRetry(conf.getInt(MAX_ERROR_RETRIES,
        DEFAULT_MAX_ERROR_RETRIES));
      awsConf.setConnectionTimeout(conf.getInt(ESTABLISH_TIMEOUT,
        DEFAULT_ESTABLISH_TIMEOUT));
      awsConf.setSocketTimeout(conf.getInt(SOCKET_TIMEOUT,
        DEFAULT_SOCKET_TIMEOUT));

      String proxyHost = conf.getTrimmed(PROXY_HOST,"");
      int proxyPort = conf.getInt(PROXY_PORT, -1);
      if (!proxyHost.isEmpty()) {
        awsConf.setProxyHost(proxyHost);
        if (proxyPort >= 0) {
          awsConf.setProxyPort(proxyPort);
        } else {
          if (secureConnections) {
            LOG.warn("Proxy host set without port. Using HTTPS default 443");
            awsConf.setProxyPort(443);
          } else {
            LOG.warn("Proxy host set without port. Using HTTP default 80");
            awsConf.setProxyPort(80);
          }
        }
        String proxyUsername = conf.getTrimmed(PROXY_USERNAME);
        String proxyPassword = conf.getTrimmed(PROXY_PASSWORD);
        if ((proxyUsername == null) != (proxyPassword == null)) {
          String msg = "Proxy error: " + PROXY_USERNAME + " or " +
            PROXY_PASSWORD + " set without the other.";
          LOG.error(msg);
          throw new IllegalArgumentException(msg);
        }
        awsConf.setProxyUsername(proxyUsername);
        awsConf.setProxyPassword(proxyPassword);
        awsConf.setProxyDomain(conf.getTrimmed(PROXY_DOMAIN));
        awsConf.setProxyWorkstation(conf.getTrimmed(PROXY_WORKSTATION));
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Using proxy server %s:%d as user %s with password %s on domain %s as workstation " +
            "%s", awsConf.getProxyHost(), awsConf.getProxyPort(), awsConf.getProxyUsername(),
            awsConf.getProxyPassword(), awsConf.getProxyDomain(), awsConf.getProxyWorkstation()));
        }
      } else if (proxyPort >= 0) {
        String msg = "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST;
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }

      s3 = new AmazonS3Client(credentials, awsConf);
      String endPoint = conf.getTrimmed(ENDPOINT,"");
      if (!endPoint.isEmpty()) {
        try {
          s3.setEndpoint(endPoint);
        } catch (IllegalArgumentException e) {
          String msg = "Incorrect endpoint: "  + e.getMessage();
          LOG.error(msg);
          throw new IllegalArgumentException(msg, e);
        }
      }

      maxKeys = conf.getInt(MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS);

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

  /**
   * Calculate the total size of all objects in the indicated bucket
   *
   * @param path to use
   * @return calculated size
   * @throws IOException
   */
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
