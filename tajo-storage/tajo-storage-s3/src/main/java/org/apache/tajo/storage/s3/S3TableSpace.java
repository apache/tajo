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
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
  public static final String ENDPOINT = "fs.s3a.endpoint";
  private static final DataSize BLOCK_SIZE = new DataSize(32, MEGABYTE);
  protected static final double SPLIT_SLOP = 1.1;   // 10% slop

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
        // Check where use a custom endpoint
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
    String key = keyFromPath(path);
    if (!key.isEmpty()) {
      key += "/";
    }

    Iterable<S3ObjectSummary> objectSummaries = S3Objects.withPrefix(s3, uri.getHost(), key);
    Stream<S3ObjectSummary> objectStream = StreamSupport.stream(objectSummaries.spliterator(), false);
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
    List<Fragment> splits = Lists.newArrayList();

    // Generate the list of FileStatuses and partition keys
    Path[] paths = pruningHandle.getPartitionPaths();
    if (paths.length == 0) {
      return splits;
    }

    // Prepare partition map which includes index for each partition path
    Map<Path, Integer> partitionPathMap = Maps.newHashMap();
    for (int i = 0; i < pruningHandle.getPartitionKeys().length; i++) {
      partitionPathMap.put(pruningHandle.getPartitionPaths()[i], i);
      LOG.info("### init partition - i:" + i + ", partition:" + pruningHandle.getPartitionPaths()[i]);
    }

    // Get common prefix of partition paths
    String commonPrefix = FileUtil.getCommonPrefix(paths);

    // Generate splits
    if (pruningHandle.hasConjunctiveForms()) {
      splits.addAll(getFragmentsByMarker(meta, schema, tableName, new Path(commonPrefix), pruningHandle,
        partitionPathMap));
    } else {
      splits.addAll(getFragmentsByListingAllObjects(meta, schema, tableName, new Path(commonPrefix), pruningHandle,
        partitionPathMap));
    }

    LOG.info("Total # of splits: " + splits.size());

    long finishTime = System.currentTimeMillis();
    long elapsedMills = finishTime - startTime;
    LOG.info(String.format("Split for partition table :%d ms elapsed.", elapsedMills));

    return splits;
  }

  /**
   * Generate fragments using marker parameter in prefix listing API
   *
   * @param path path to be listed
   * @param pruningHandle informs of partition pruning results
   * @throws IOException
   */
  private List<Fragment> getFragmentsByMarker(TableMeta meta, Schema schema, String tableName, Path path,
             PartitionPruningHandle pruningHandle, Map<Path, Integer> partitionPathMap) throws IOException {
    List<Fragment> splits = Lists.newArrayList();
    long startTime = System.currentTimeMillis();
    ObjectListing objectListing;
    Path previousPartition = null, nextPartition = null;
    int callCount = 0, i = 0;
    boolean finished = false, enabled = false;

    int partitionCount = pruningHandle.getPartitionPaths().length;
    LOG.info("### 100 ### partitionCount:" + partitionCount);
    // Listing S3 Objects using AWS API
    String prefix = keyFromPath(path);
    if (!prefix.isEmpty()) {
      prefix += "/";
    }
    LOG.info("### 110 ### prefix:" + prefix);

    ListObjectsRequest request = new ListObjectsRequest()
      .withBucketName(uri.getHost())
      .withPrefix(prefix);

    do {
      enabled = true;

      // Get first chunk of 1000 objects
      objectListing = s3.listObjects(request);

      int objectsCount = objectListing.getObjectSummaries().size();
      LOG.info("### 200 ### objectsCount:" + objectsCount);

      // Get partition of last bucket from current objects
      Path lastPath = getPathFromBucket(objectListing.getObjectSummaries().get(objectsCount - 1));
      Path lastPartition = lastPath.getParent();
      LOG.info("### 210 ### lastPath:" + lastPath);

      // Check target partition compare with last partition of current objects
      if (previousPartition == null) {
        if (pruningHandle.getPartitionPaths()[0].compareTo(lastPartition) > 0) {
          enabled = false;
          LOG.info("### 220 ###");
        }
      } else {
        if (previousPartition.compareTo(lastPartition) > 0) {
          enabled = false;
          LOG.info("### 230 ###");
        }
      }
      LOG.info("### 300 ### callCount:" + callCount + ", nextMarker:" + objectListing.getNextMarker());

      // Generate FileStatus and partition key
      if (enabled) {
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
          LOG.info("### 310 ### key:" + summary.getKey());
          if (summary.getSize() > 0 && !summary.getKey().endsWith("/")) {
            Path bucketPath = getPathFromBucket(summary);
            LOG.info("### 310 ### bucketPath:" + bucketPath);

            if (!bucketPath.getName().startsWith("_") && !bucketPath.getName().startsWith(".")) {
              Path partitionPath = bucketPath.getParent();
              LOG.info("### 320 ### partitionPath:" + partitionPath );

              // If Tajo can matched partition from partition map, add it to final list.
              if (partitionPathMap.containsKey(partitionPath)) {
                FileStatus file = getFileStatusFromBucket(summary, bucketPath);
                String partitionKey = getPartitionKey(pruningHandle, partitionPathMap, partitionPath);
                computePartitionSplits(file, meta, schema, tableName, partitionKey, splits);
                previousPartition = partitionPath;
                LOG.info("### 330 ### previousPartition:" + previousPartition );

                if (LOG.isDebugEnabled()){
                  LOG.debug("# of average splits per partition: " + splits.size() / (i+1));
                }
                i++;
              } else {
                nextPartition = null;

                // Get next target partition
                if (previousPartition == null) {
                  nextPartition = pruningHandle.getPartitionPaths()[0];
                  LOG.info("### 400 ### nextPartition:" + nextPartition);
                } else {
                  // Find index of previous partition
                  int index = partitionPathMap.get(previousPartition);

                  // Find next target partition with the index of previous partition
                  if ((index + 1) < partitionCount) {
                    nextPartition = pruningHandle.getPartitionPaths()[index+1];
                    LOG.info("### 410 ### nextPartition:" + nextPartition + ", index:" + index);
                  } else if ((index + 1) == partitionCount) {
                    finished = true;
                    LOG.info("### 420 ### finished");
                    break;
                  }
                }

                LOG.info("### 430 ###");
                // If there is no matched partition, consider to move next marker. Otherwise access next object.
                if (nextPartition != null  && nextPartition.compareTo(lastPartition) <= 0) {
                  LOG.info("### 440 ###");
                  continue;
                } else {
                  LOG.info("### 450 ###");
                  break;
                }
              }
            }
          }
        }
      }
      LOG.info("### 500 ### ");

      request.setMarker(objectListing.getNextMarker());
      callCount++;
    } while (objectListing.isTruncated() && !finished);
    long finishTime = System.currentTimeMillis();
    long elapsedMills = finishTime - startTime;
    LOG.info(String.format("List S3Objects: %d ms elapsed. API call count: %d", elapsedMills, callCount));

    return splits;
  }

  /**
   * Generate fragments using listing all objects without marker
   *
   * @param path path to be listed
   * @param pruningHandle informs of partition pruning results
   * @throws IOException
   */
  private List<Fragment> getFragmentsByListingAllObjects(TableMeta meta, Schema schema, String tableName, Path path,
                        PartitionPruningHandle pruningHandle, Map<Path, Integer> partitionPathMap) throws IOException {
    List<Fragment> splits = Lists.newArrayList();
    long startTime = System.currentTimeMillis();

    String prefix = keyFromPath(path);
    if (!prefix.isEmpty()) {
      prefix += "/";
    }

    int i = 0;
    Iterable<S3ObjectSummary> objectSummaries = S3Objects.withPrefix(s3, uri.getHost(), prefix);
    for (S3ObjectSummary summary : objectSummaries) {
      if (summary.getSize() >0 && !summary.getKey().endsWith("/")) {
        Path bucketPath = getPathFromBucket(summary);
        String fileName = bucketPath.getName();

        if (!fileName.startsWith("_") && !fileName.startsWith(".")) {
          Path partitionPath = bucketPath.getParent();
          if (partitionPathMap.containsKey(partitionPath)) {
            FileStatus file = getFileStatusFromBucket(summary, bucketPath);
            String partitionKey = getPartitionKey(pruningHandle, partitionPathMap, partitionPath);
            computePartitionSplits(file, meta, schema, tableName, partitionKey, splits);

            if (LOG.isDebugEnabled()){
              LOG.debug("# of average splits per partition: " + splits.size() / (i+1));
            }
            i++;
          }
        }
      }
    }

    long finishTime = System.currentTimeMillis();
    long elapsedMills = finishTime - startTime;
    LOG.info(String.format("List S3Objects: %d ms elapsed", elapsedMills));

    return splits;
  }

  private String getPartitionKey(PartitionPruningHandle pruningHandle, Map<Path, Integer> partitionPathMap, Path
    partitionPath) {
    int index = partitionPathMap.get(partitionPath);
    return pruningHandle.getPartitionKeys()[index];
  }

  private void computePartitionSplits(FileStatus file, TableMeta meta, Schema schema, String tableName,
                                      String partitionKey, List<Fragment> splits) throws IOException {
    Path path = file.getPath();
    long length = file.getLen();

    // Get locations of blocks of file
    BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
    boolean splittable = isSplittablePartitionFragment(meta, schema, path, partitionKey, file);
    if (splittable) {

      long minSize = Math.max(getMinSplitSize(), 1);
      long blockSize = file.getBlockSize(); // s3n rest api contained block size but blockLocations is one
      long splitSize = Math.max(minSize, blockSize);
      long bytesRemaining = length;

      // for s3
      while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
        int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
        splits.add(getSplittablePartitionFragment(tableName, path, length - bytesRemaining, splitSize,
          blkLocations[blkIndex].getHosts(), partitionKey));
        bytesRemaining -= splitSize;
      }
      if (bytesRemaining > 0) {
        int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
        splits.add(getSplittablePartitionFragment(tableName, path, length - bytesRemaining, bytesRemaining,
          blkLocations[blkIndex].getHosts(), partitionKey));
      }
    } else { // Non splittable
      splits.add(getNonSplittablePartitionFragment(tableName, path, 0, length, blkLocations, partitionKey));
    }
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

  @VisibleForTesting
  public AmazonS3 getAmazonS3Client() {
    return s3;
  }
}
