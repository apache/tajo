/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.PartitionInfo;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.FormatProperty;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import net.minidev.json.JSONObject;

/**
 * Tablespace for Kafka table.
 */
public class KafkaTablespace extends Tablespace {
  private static final Log LOG = LogFactory.getLog(KafkaTablespace.class);

  public static final StorageProperty KAFKA_STORAGE_PROPERTIES = new StorageProperty("kafka", false, false, false,
      false);

  public static final FormatProperty KAFKA_FORMAT_PROPERTIES = new FormatProperty(false, false, false);

  public KafkaTablespace(String name, URI uri, JSONObject config) {
    super(name, uri, config);
  }

  @Override
  protected void storageInit() throws IOException {
  }

  @Override
  public long getTableVolume(TableDesc table, Optional<EvalNode> filter) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void close() {
  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws TajoException, IOException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException, TajoException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public URI getTableUri(TableMeta meta, String databaseName, String tableName) {
    return URI.create(uri.toString() + "/" + tableName);
  }

  @Override
  public List<Fragment> getSplits(String inputSourceId,
      TableDesc table,
      boolean requireSorted,
      EvalNode filterCondition)
          throws IOException, TajoException {
    String topic = table.getMeta().getProperty(KafkaStorageConstants.KAFKA_TOPIC);
    int fragmentSize = Integer.parseInt(table.getMeta().getProperty(KafkaStorageConstants.KAFKA_FRAGMENT_SIZE,
        KafkaStorageConstants.DEFAULT_FRAGMENT_SIZE));
    // If isn't specific partitions, scan all partition of topic.
    String partitions = table.getMeta().getProperty(KafkaStorageConstants.KAFKA_TOPIC_PARTITION,
        KafkaStorageConstants.DEFAULT_PARTITION);
    List<PartitionInfo> partitionList;
    if (partitions.equals(KafkaStorageConstants.DEFAULT_PARTITION)) {
      partitionList = SimpleConsumerManager.getPartitions(uri, topic);
    } else {
      partitionList = new LinkedList<>();
      // filter partitions.
      List<PartitionInfo> topicPartitions = SimpleConsumerManager.getPartitions(uri, topic);
      Map<String, PartitionInfo> topicPartitionsMap = new HashMap<>();
      for (PartitionInfo partitionInfo : topicPartitions) {
        topicPartitionsMap.put(Integer.toString(partitionInfo.partition()), partitionInfo);
      }
      for (String partitionId : partitions.split(",")) {
        partitionList.add(topicPartitionsMap.get(partitionId));
      }
    }

    List<Fragment> fragments = new ArrayList<Fragment>();
    for (PartitionInfo partitionInfo : partitionList) {
      int partitionId = partitionInfo.partition();
      String leaderHost = partitionInfo.leader().host();
      long lastOffset;
      long startOffset;
      try (SimpleConsumerManager simpleConsumerManager = new SimpleConsumerManager(uri, topic, partitionId)) {
        simpleConsumerManager.assign();
        lastOffset = simpleConsumerManager.getLatestOffset();
        startOffset = simpleConsumerManager.getEarliestOffset();
      }

      long messageSize = lastOffset - startOffset;
      if (0 == lastOffset || 0 == messageSize)
        continue;

      // If message count of partition is less than fragmentSize(message count of one fragment),
      if (messageSize <= fragmentSize) {
        fragments.add(new KafkaFragment(table.getUri(), table.getName(), topic, startOffset,
            lastOffset, partitionId, leaderHost));
      } else { // If message count of partition is greater than fragmentSize,
        long nextFragmentStartOffset = startOffset;
        while (nextFragmentStartOffset < lastOffset) {
          long nextFragmentLastOffset = nextFragmentStartOffset + fragmentSize;
          // the offset of last part is small than fragmentSize so that system gets the minimum value.
          long fragmentLstOffset = Math.min(nextFragmentLastOffset, lastOffset);
          fragments.add(new KafkaFragment(table.getUri(), table.getName(), topic,
              nextFragmentStartOffset, fragmentLstOffset, partitionId, leaderHost));
          nextFragmentStartOffset = nextFragmentLastOffset;
        }
      }
    }
    return fragments;
  }

  @Override
  public StorageProperty getProperty() {
    return KAFKA_STORAGE_PROPERTIES;
  }

  @Override
  public FormatProperty getFormatProperty(TableMeta meta) {
    return KAFKA_FORMAT_PROPERTIES;
  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc, Schema inputSchema,
      SortSpec[] sortSpecs, TupleRange dataRange) throws IOException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) throws TajoException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public void prepareTable(LogicalNode node) throws IOException, TajoException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public Path commitTable(OverridableConf queryContext, ExecutionBlockId finalEbId, LogicalPlan plan, Schema schema,
      TableDesc tableDesc) throws IOException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public void rollbackTable(LogicalNode node) throws IOException, TajoException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  @Override
  public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
    throw new TajoRuntimeException(new UnsupportedException());
  }
}
