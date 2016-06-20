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
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.tajo.conf.TajoConf;
import org.weakref.jmx.internal.guava.collect.AbstractSequentialIterator;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.toArray;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class TajoS3FileSystem extends S3AFileSystem {
  private static final DataSize BLOCK_SIZE = new DataSize(32, MEGABYTE);

  private URI uri;
  private AmazonS3 s3;
  private boolean useInstanceCredentials;

  private TajoConf conf;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    requireNonNull(uri, "uri is null");
    requireNonNull(conf, "conf is null");
    super.initialize(uri, conf);
    setConf(conf);

    this.conf = new TajoConf(conf);

    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());

    int maxErrorRetries = this.conf.getIntVar(TajoConf.ConfVars.S3_MAX_ERROR_RETRIES);
    boolean sslEnabled = this.conf.getBoolVar(TajoConf.ConfVars.S3_SSL_ENABLED);

    Duration connectTimeout = Duration.valueOf(this.conf.getVar(TajoConf.ConfVars.S3_CONNECT_TIMEOUT));
    Duration socketTimeout = Duration.valueOf(this.conf.getVar(TajoConf.ConfVars.S3_SOCKET_TIMEOUT));
    int maxConnections = this.conf.getIntVar(TajoConf.ConfVars.S3_MAX_CONNECTIONS);

    this.useInstanceCredentials = this.conf.getBoolVar(TajoConf.ConfVars.S3_USE_INSTANCE_CREDENTIALS);

    ClientConfiguration configuration = new ClientConfiguration()
      .withMaxErrorRetry(maxErrorRetries)
      .withProtocol(sslEnabled ? Protocol.HTTPS : Protocol.HTTP)
      .withConnectionTimeout(Ints.checkedCast(connectTimeout.toMillis()))
      .withSocketTimeout(Ints.checkedCast(socketTimeout.toMillis()))
      .withMaxConnections(maxConnections);

    this.s3 = createAmazonS3Client(uri, conf, configuration);
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

    if (useInstanceCredentials) {
      return new InstanceProfileCredentialsProvider();
    }

    throw new RuntimeException("S3 credentials not configured");
  }

  private static AWSCredentials getAwsCredentials(URI uri, Configuration conf) {
    S3Credentials credentials = new S3Credentials();
    credentials.initialize(uri, conf);
    return new BasicAWSCredentials(credentials.getAccessKey(), credentials.getSecretAccessKey());
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    List<LocatedFileStatus> list = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> iterator = listLocatedStatus(path);
    while (iterator.hasNext()) {
      list.add(iterator.next());
    }
    return toArray(list, LocatedFileStatus.class);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path path) {
    return new RemoteIterator<LocatedFileStatus>() {
      private final Iterator<LocatedFileStatus> iterator = listPrefix(path);

      @Override
      public boolean hasNext() throws IOException {
        try {
          return iterator.hasNext();
        }
        catch (AmazonClientException e) {
          throw new IOException(e);
        }
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        try {
          return iterator.next();
        }
        catch (AmazonClientException e) {
          throw new IOException(e);
        }
      }
    };
  }

  private Iterator<LocatedFileStatus> listPrefix(Path path) {
    String key = keyFromPath(path);
    if (!key.isEmpty()) {
      key += "/";
    }

    ListObjectsRequest request = new ListObjectsRequest()
      .withBucketName(uri.getHost())
      .withPrefix(key)
      .withDelimiter("/");

    Iterator<ObjectListing> listings = new AbstractSequentialIterator<ObjectListing>(s3.listObjects(request)) {
      @Override
      protected ObjectListing computeNext(ObjectListing previous) {
        if (!previous.isTruncated()) {
          return null;
        }
        return s3.listNextBatchOfObjects(previous);
      }
    };

    return Iterators.concat(Iterators.transform(listings, this::statusFromListing));
  }

  private static String keyFromPath(Path path) {
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

  private Iterator<LocatedFileStatus> statusFromListing(ObjectListing listing) {
    return Iterators.concat(
      statusFromPrefixes(listing.getCommonPrefixes()),
      statusFromObjects(listing.getObjectSummaries()));
  }

  private Iterator<LocatedFileStatus> statusFromPrefixes(List<String> prefixes) {
    List<LocatedFileStatus> list = new ArrayList<>();
    for (String prefix : prefixes) {
      Path path = qualifiedPath(new Path("/" + prefix));
      FileStatus status = new FileStatus(0, true, 1, 0, 0, path);
      list.add(createLocatedFileStatus(status));
    }
    return list.iterator();
  }

  private Iterator<LocatedFileStatus> statusFromObjects(List<S3ObjectSummary> objects) {
    return objects.stream()
      .filter(object -> !object.getKey().endsWith("/"))
      .map(object -> new FileStatus(
        object.getSize(),
        false,
        1,
        BLOCK_SIZE.toBytes(),
        object.getLastModified().getTime(),
        qualifiedPath(new Path("/" + object.getKey()))))
      .map(this::createLocatedFileStatus)
      .iterator();
  }

  private Path qualifiedPath(Path path) {
    return path.makeQualified(this.uri, getWorkingDirectory());
  }

  private LocatedFileStatus createLocatedFileStatus(FileStatus status) {
    try {
      BlockLocation[] fakeLocation = getFileBlockLocations(status, 0, status.getLen());
      return new LocatedFileStatus(status, fakeLocation);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @VisibleForTesting
  AmazonS3 getS3Client() {
    return s3;
  }

  @VisibleForTesting
  void setS3Client(AmazonS3 client) {
    s3 = client;
  }
}