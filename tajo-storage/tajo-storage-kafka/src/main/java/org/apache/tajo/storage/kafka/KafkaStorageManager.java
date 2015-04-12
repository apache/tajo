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

package org.apache.tajo.storage.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.kafka.fragment.KafkaFragment;

// StorageManager for Kafka topic.
public class KafkaStorageManager extends StorageManager {
  private static final Log LOG = LogFactory.getLog(KafkaStorageManager.class);
  static final int FLAG_NOT_SKIP = 0;
  static final int FLAG_UNLIMITED = 0;
  static final String DEFAULT_PARTITION = "all";
  static final String DEFAULT_FRAGMENT_SIZE = "10000";
  // kafka connections pool.
  private Map<String, SimpleConsumerManager> connMap;

  public KafkaStorageManager(StoreType storeType) {
    super(storeType);
  }

  @Override
  public void closeStorageManager() {
    // Close kafka connections of connMap.
    synchronized (connMap) {
      for (SimpleConsumerManager eachConn : connMap.values()) {
        try {
          eachConn.close();
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }

  @Override
  protected void storageInit() throws IOException {
    connMap = new HashMap<String, SimpleConsumerManager>();
  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException {
    TableStats stats = new TableStats();
    stats.setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
    tableDesc.setStats(stats);
    // Actuality kafka topic is not create.
  }

  // is not supported yet.
  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException {
    throw new IOException("Purge is not supported.");
  }

  @Override
  public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc, ScanNode scanNode) throws IOException {
    return getFragmentList(tableDesc, FLAG_NOT_SKIP, FLAG_UNLIMITED);
  }

  @Override
  public List<Fragment> getNonForwardSplit(TableDesc tableDesc, int currentPage, int numFragments) throws IOException {
    int limitNum = numFragments + currentPage;
    List<Fragment> fragments = getFragmentList(tableDesc, currentPage, limitNum);
    if (fragments.size() > 0) {
      return fragments;
    } else { // If any more isn't fragment, End paging.
      return new ArrayList<Fragment>(1);
    }
  }

  @Override
  public StorageProperty getStorageProperty() {
    StorageProperty storageProperty = new StorageProperty();
    storageProperty.setSortedInsert(false);
    storageProperty.setSupportsInsertInto(false);
    return storageProperty;
  }

  // is not supported.
  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc, Schema inputSchema,
      SortSpec[] sortSpecs, TupleRange dataRange) throws IOException {
    return null;
  }

  // is not supported.
  @Override
  public void beforeInsertOrCATS(LogicalNode node) throws IOException {
  }

  // is not supported.
  @Override
  public void rollbackOutputCommit(LogicalNode node) throws IOException {
  }

  // Split scan data into fragments in order to distributed processing. And
  // return fragment list.
  // One partition of kafka topic is one fragment.
  // But, If message count of one partition is so big, One partition is split to
  // several fragments.
  private List<Fragment> getFragmentList(TableDesc tableDesc, int numSkip, int limitNum) throws IOException {
    int fragmentCount = 0;
    String topic = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_TOPIC);
    String brokers = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_BROKER);
    int fragmentSize = Integer.parseInt(tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_FRAGMENT_SIZE,
        DEFAULT_FRAGMENT_SIZE));
    // If isn't specific partitions, scan all partition of topic.
    String partitions = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_TOPIC_PARTITION, DEFAULT_PARTITION);
    Set<Integer> partitionSet = new HashSet<Integer>();
    if (partitions.equals(DEFAULT_PARTITION)) {
      partitionSet = SimpleConsumerManager.getPartitions(brokers, topic);
    } else {
      for (String partitionId : partitions.split(",")) {
        partitionSet.add(Integer.parseInt(partitionId));
      }
    }
    List<Fragment> fragments = new ArrayList<Fragment>();
    boolean isBreak = false; // For paging of nonForwdSplit
    for (Integer partitionId : partitionSet) {
      if (isBreak)
        break;
      SimpleConsumerManager simpleConsumerManager = getConnection(brokers, topic, partitionId);
      long lastOffset = simpleConsumerManager.getReadOffset(kafka.api.OffsetRequest.LatestTime());
      long startOffset = simpleConsumerManager.getReadOffset(kafka.api.OffsetRequest.EarliestTime());
      long messageSize = lastOffset - startOffset;
      if (0 == lastOffset || 0 == messageSize)
        continue;

      // If message count of partition is less than fragmentSize(message count
      // of one fragment),
      if (messageSize <= fragmentSize) {
        fragmentCount++;
        if (FLAG_NOT_SKIP == numSkip || fragmentCount > numSkip) {
          KafkaFragment fragment = new KafkaFragment(tableDesc.getName(), topic, partitionId, brokers, startOffset,
              lastOffset);
          fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
          fragments.add(fragment);
        }
        if (FLAG_UNLIMITED != limitNum && fragmentCount >= limitNum) {
          isBreak = true;
        }
      } else { // If message count of partition is greater than fragmentSize,
        long nextFragmentStartOffset = startOffset;
        while (nextFragmentStartOffset < lastOffset) {
          long nextFragmentlastOffset = nextFragmentStartOffset + fragmentSize;
          fragmentCount++;
          if (FLAG_NOT_SKIP == numSkip || fragmentCount > numSkip) {
            KafkaFragment fragment = new KafkaFragment(tableDesc.getName(), topic, partitionId, brokers,
                nextFragmentStartOffset, nextFragmentlastOffset);
            fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
            fragments.add(fragment);
          }
          nextFragmentStartOffset = nextFragmentlastOffset;
          if (limitNum != FLAG_UNLIMITED && fragmentCount >= limitNum) {
            isBreak = true;
            break;
          }
        }
      }
    }
    return fragments;
  }

  // Manage the kafka connection pool.
  public SimpleConsumerManager getConnection(String seedBrokers, String topic, int partition) throws IOException {
    String conKey = topic + "_" + partition;
    synchronized (connMap) {
      SimpleConsumerManager conn = connMap.get(conKey);
      if (conn == null) {
        conn = SimpleConsumerManager.getSimpleConsumerManager(seedBrokers, topic, partition);
        connMap.put(conKey, conn);
      }
      return conn;
    }

  }
}
