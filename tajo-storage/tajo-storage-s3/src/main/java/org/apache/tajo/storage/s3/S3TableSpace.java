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
import java.util.HashSet;
import java.util.List;
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
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
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

    if (pruningHandle.hasConjunctiveForms()) {
      for(Path path : paths) {
        listS3ObjectsOfPartitionTable(path, files, partitionKeys, pruningHandle);
      }
    } else {
      HashSet<Path> parents = getParentPaths(paths);
      for(Path parent : parents) {
        listS3ObjectsOfPartitionTable(parent, files, partitionKeys, pruningHandle);
      }
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
   * Generate the list of FileStatus and partition keys using AWS S3 SDK.
   *
   */
  private void listS3ObjectsOfPartitionTable(Path prefixPath, List<FileStatus> files,
                                      List<String> partitionKeys, PartitionPruningHandle pruningHandle) throws IOException {
    Stream<S3ObjectSummary> objectStream = getS3ObjectSummaryStream(prefixPath);

    objectStream
      .filter(summary -> summary.getSize() > 0 && !summary.getKey().endsWith("/"))
        .forEach(summary -> {
            String bucketName = summary.getBucketName();
            String pathString = uri.getScheme() + "://" + bucketName + "/" + summary.getKey();
            Path path = new Path(pathString);
            String fileName = path.getName();

            if (!fileName.startsWith("_") && !fileName.startsWith(".")) {
              int lastIndex = pathString.lastIndexOf("/");
              String partitionPathString = pathString.substring(0, lastIndex);
              Path partitionPath = new Path(partitionPathString);

              if (pruningHandle.getPartitionMap().containsKey(partitionPath)) {
                String partitionKey = pruningHandle.getPartitionMap().get(partitionPath);
                files.add(new FileStatus(summary.getSize(), false, 1, BLOCK_SIZE.toBytes(),
                  summary.getLastModified().getTime(), path));
                partitionKeys.add(partitionKey);
              }
            }
          }
        );
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


  /**
   * Find parent paths of the specified paths.
   *
   * example #1:
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-01-02
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-01-03
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-02-01
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1995-02-02
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1995-10-02
   *  --> s3://tajo-data-us-east-1/tpch-1g-partition/lineitem
   *
   * example #2:
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-01-02/l_returnflag=A
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-01-02/l_returnflag=R
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-01-03/l_returnflag=R
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-02-02/l_returnflag=A
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1995-02-02/l_returnflag=A
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1995-10-02/l_returnflag=R
   *  --> s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-01-02
   *      s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-01-03
   *      s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-02-02
   *      s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-10-02
   *
   * @param paths
   * @return the collection of parent paths
   */
  private HashSet<Path> getParentPaths(Path[] paths) {
    HashSet<Path> hashSet = Sets.newHashSet();

    for(Path path : paths) {
      hashSet.add(path.getParent());
    }

    return hashSet;
  }



  /**
   * Find prefix paths of the specified paths.
   *
   * example
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1992-01-02
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1993-02-01
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1995-02-02
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=2015-02-01
   *  s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=2016-02-02
   *  --> s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=1
   *      s3://tajo-data-us-east-1/tpch-1g-partition/lineitem/l_shipdate=2
   *
   * @param paths
   * @return the collection of prefix paths
   */
  private HashSet<Path> getFilteredPrefixList(Path[] paths) {
    HashSet<Path> hashSet = Sets.newHashSet();

    for(Path path : paths) {
      String[] partitionKeyValue = path.getName().split("=");
      if (partitionKeyValue != null && partitionKeyValue.length == 2) {
        String name = partitionKeyValue[0] + "=" + partitionKeyValue[1].substring(0, 1);
        Path prefix = new Path(path.getParent(), name);
        hashSet.add(prefix);
      }
    }

    return hashSet;
  }


  @VisibleForTesting
  public AmazonS3 getAmazonS3Client() {
    return s3;
  }
}
