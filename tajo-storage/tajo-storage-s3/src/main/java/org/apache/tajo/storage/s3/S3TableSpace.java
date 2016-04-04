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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.catalog.*;
import org.apache.tajo.plan.partition.PartitionPruningHandle;
import org.apache.tajo.storage.FileTablespace;

import net.minidev.json.JSONObject;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.FileUtil;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class S3TableSpace extends FileTablespace {
  private final static Log LOG = LogFactory.getLog(S3TableSpace.class);

  private AmazonS3 s3;
  private boolean useInstanceCredentials;
  //use a custom endpoint?
  public static final String ENDPOINT = "fs.s3a.endpoint";
  private static final DataSize BLOCK_SIZE = new DataSize(32, MEGABYTE);

  public S3TableSpace(String spaceName, URI uri, JSONObject config) {
    super(spaceName, uri, config);
  }

  @Override
  public void init(TajoConf tajoConf) throws IOException {
    super.init(tajoConf);

    this.blocksMetadataEnabled = false;

    int maxErrorRetries = conf.getIntVar(TajoConf.ConfVars.S3_MAX_ERROR_RETRIES);
    boolean sslEnabled = conf.getBoolVar(TajoConf.ConfVars.S3_SSL_ENABLED);

    Duration connectTimeout = Duration.valueOf(conf.getVar(TajoConf.ConfVars.S3_CONNECT_TIMEOUT));
    Duration socketTimeout = Duration.valueOf(conf.getVar(TajoConf.ConfVars.S3_SOCKET_TIMEOUT));
    int maxConnections = conf.getIntVar(TajoConf.ConfVars.S3_MAX_CONNECTIONS);

    this.useInstanceCredentials = conf.getBoolVar(TajoConf.ConfVars.S3_USE_INSTANCE_CREDENTIALS);

    ClientConfiguration configuration = new ClientConfiguration()
      .withMaxErrorRetry(maxErrorRetries)
      .withProtocol(sslEnabled ? Protocol.HTTPS : Protocol.HTTP)
      .withConnectionTimeout(Ints.checkedCast(connectTimeout.toMillis()))
      .withSocketTimeout(Ints.checkedCast(socketTimeout.toMillis()))
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

    if (useInstanceCredentials) {
      return new InstanceProfileCredentialsProvider();
    }

    throw new RuntimeException("S3 credentials not configured");
  }

  private static AWSCredentials getAwsCredentials(URI uri, Configuration conf) {
    TajoS3Credentials credentials = new TajoS3Credentials();
    credentials.initialize(uri, conf);
    return new BasicAWSCredentials(credentials.getAccessKey(), credentials.getSecretAccessKey());
  }

  @Override
  public long calculateSize(Path path) throws IOException {
    Stream<S3ObjectSummary> objectStream = getS3ObjectSummaryStream(path);
    long totalBucketSize = objectStream.mapToLong(object -> object.getSize()).sum();
    objectStream.close();
    return totalBucketSize;
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

  @Override
  public List<Fragment> getPartitionSplits(String tableName, TableMeta meta, Schema schema
    , PartitionPruningHandle pruningHandle) throws IOException {
    long startTime = System.currentTimeMillis();
    List<FileStatus> files = Lists.newArrayList();
    List<String> partitionKeys = Lists.newArrayList();

    // Generate the list of FileStatuses and partition keys
    Path[] paths = pruningHandle.getPartitionPaths();

    // Get common prefix of partition paths
    String commonPrefix = FileUtil.getCommonPrefix(paths);

    // List buckets to generate FileStatuses and partition keys
    if (pruningHandle.hasConjunctiveForms()) {
      listS3ObjectsByMarker(new Path(commonPrefix), files, partitionKeys, pruningHandle);
    } else {
      listAllS3Objects(new Path(commonPrefix), files, partitionKeys, pruningHandle);
    }

    // Generate splits'
    List<Fragment> splits = Lists.newArrayList();
    List<Fragment> volumeSplits = Lists.newArrayList();
    List<BlockLocation> blockLocations = Lists.newArrayList();

    int i = 0;
    for (FileStatus file : files) {
      computePartitionSplits(file, meta, schema, tableName, partitionKeys.get(i), splits, volumeSplits, blockLocations);
      if (LOG.isDebugEnabled()){
        LOG.debug("# of average splits per partition: " + splits.size() / (i+1));
      }
      i++;
    }

    // Combine original fileFragments with new VolumeId information
    setVolumeMeta(volumeSplits, blockLocations);
    splits.addAll(volumeSplits);
    LOG.info("Total # of splits: " + splits.size());

    long finishTime = System.currentTimeMillis();
    long elapsedMills = finishTime - startTime;
    LOG.info(String.format("Split for partition table :%d ms elapsed.", elapsedMills));

    return splits;
  }

  /**
   * Generate the list of FileStatus and partition key using marker parameter in prefix listing API
   *
   * @param path path to be listed
   * @param files the list of FileStatus to be generated
   * @param partitionKeys the list of partition key to be generated
   * @param pruningHandle informs of partition pruning results
   * @throws IOException
   */
  private void listS3ObjectsByMarker(Path path, List<FileStatus> files, List<String> partitionKeys,
    PartitionPruningHandle pruningHandle) throws IOException {
    long startTime = System.currentTimeMillis();

    ObjectListing objectListing;
    String previousPartition = null, nextPartition = null;
    int callCount = 0;
    boolean finished = false, enabled = false;

    String prefix = keyFromPath(path);
    if (!prefix.isEmpty()) {
      prefix += "/";
    }

    ListObjectsRequest request = new ListObjectsRequest()
      .withBucketName(uri.getHost())
      .withPrefix(prefix);

    Map<Integer, String> partitionMap = Maps.newHashMap();
    for (int i = 0; i < pruningHandle.getPartitionKeys().length; i++) {
      partitionMap.put(i, pruningHandle.getPartitionKeys()[i]);
    }

    do {
      enabled = true;

      // Get first chunk of 1000 objects
      objectListing = s3.listObjects(request);

      int objectsCount = objectListing.getObjectSummaries().size();

      // Get partition of last bucket from current objects
      Path lastPath = getPathFromBucket(objectListing.getObjectSummaries().get(objectsCount - 1));
      String lastPartition = lastPath.getParent().getName();

      // Check target partition compare with last partition of current objects
      if (previousPartition == null) {
        if (partitionMap.get(0).compareTo(lastPartition) > 0) {
          enabled = false;
        }
      } else {
        if (previousPartition.compareTo(lastPartition) > 0) {
          enabled = false;
        }
      }

      // Generate FileStatus and partition key
      if (enabled) {
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
          if (summary.getSize() > 0 && !summary.getKey().endsWith("/")) {
            Path bucketPath = getPathFromBucket(summary);

            if (!bucketPath.getName().startsWith("_") && !bucketPath.getName().startsWith(".")) {
              Path partitionPath = bucketPath.getParent();

              // If Tajo can matched partition from partition map, add it to final list.
              if (pruningHandle.getPartitionMap().containsKey(partitionPath)) {
                files.add(getFileStatusFromBucket(summary, bucketPath));
                String partitionKey = pruningHandle.getPartitionMap().get(partitionPath);
                partitionKeys.add(partitionKey);
                previousPartition = partitionKey;
              } else {
                // If Tajo can't matched partition, consider to move next marker.
               int index = -1;
                // If any partition not yet added
                if (previousPartition == null) {
                  nextPartition = partitionMap.get(0);
                } else {
                  // Find index of previous partition
                  for(Map.Entry<Integer, String> entry : partitionMap.entrySet()) {
                    if (entry.getValue().equals(previousPartition)) {
                      index = entry.getKey();
                      break;
                    }
                  }

                  // Find next target partition with the index of previous partition
                  if ((index + 1) < partitionMap.size()) {
                    nextPartition = partitionMap.get(index+1);
                  } else if ((index + 1) == partitionMap.size()) {
                    finished = true;
                    break;
                  }
                }

                if (nextPartition != null  && nextPartition.compareTo(lastPartition) <= 0) {
                  continue;
                } else {
                  break;
                }
              }
            }
          }
        }
      }

      request.setMarker(objectListing.getNextMarker());
      callCount++;
    } while (objectListing.isTruncated() && !finished);
    long finishTime = System.currentTimeMillis();
    long elapsedMills = finishTime - startTime;
    LOG.info(String.format("List S3Objects: %d ms elapsed. API call count: %d", elapsedMills, callCount));
  }

  /**
   * Generate the list of FileStatus and partition key
   *
   * @param path path to be listed
   * @param files the list of FileStatus to be generated
   * @param partitionKeys the list of partition key to be generated
   * @param pruningHandle informs of partition pruning results
   * @throws IOException
   */
  private void listAllS3Objects(Path path, List<FileStatus> files, List<String> partitionKeys, PartitionPruningHandle
    pruningHandle) throws IOException {
    long startTime = System.currentTimeMillis();

    Stream<S3ObjectSummary> objectStream = getS3ObjectSummaryStream(path);

    objectStream
      .filter(summary -> summary.getSize() > 0 && !summary.getKey().endsWith("/"))
      .forEach(summary -> {
          Path bucketPath = getPathFromBucket(summary);
          String fileName = bucketPath.getName();

          if (!fileName.startsWith("_") && !fileName.startsWith(".")) {
            Path partitionPath = bucketPath.getParent();
            if (pruningHandle.getPartitionMap().containsKey(partitionPath)) {
              files.add(getFileStatusFromBucket(summary, bucketPath));
              partitionKeys.add(pruningHandle.getPartitionMap().get(partitionPath));
            }
          }
        }
      );

    long finishTime = System.currentTimeMillis();
    long elapsedMills = finishTime - startTime;
    LOG.info(String.format("List S3Objects: %d ms elapsed", elapsedMills));
  }

  private FileStatus getFileStatusFromBucket(S3ObjectSummary summary, Path path) {
    return new FileStatus(summary.getSize(), false, 1, BLOCK_SIZE.toBytes(),
      summary.getLastModified().getTime(), path);
  }

  private Path getPathFromBucket(S3ObjectSummary summary) {
    String bucketName = summary.getBucketName();
    String pathString = uri.getScheme() + "://" + bucketName + "/" + summary.getKey();
    Path path = new Path(pathString);
    return path;
  }

  private Stream<S3ObjectSummary> getS3ObjectSummaryStream(Path path) throws IOException {
    String prefix = keyFromPath(path);
    if (!prefix.isEmpty()) {
      prefix += "/";
    }

    Iterable<S3ObjectSummary> objectSummaries = S3Objects.withPrefix(s3, uri.getHost(), prefix);
    Stream<S3ObjectSummary> objectStream = StreamSupport.stream(objectSummaries.spliterator(), false);

    return objectStream;
  }

  @VisibleForTesting
  public AmazonS3 getAmazonS3Client() {
    return s3;
  }
}
