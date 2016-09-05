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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * SimpleConsumerManager is kafka client for KafkaScanner.
 * It's one per partition. Each partition instantiate this class.
 */
public class SimpleConsumerManager implements Closeable {
  private KafkaConsumer<byte[], byte[]> consumer = null;
  private TopicPartition partition;

  /**
   * Create SimpleConsumer instance.
   *
   * @param uri Kafka Tablespace URI. ex) kafka://localhost:9092,localhost:9091
   * @param topic topic name
   * @param partitionId partition id
   */
  public SimpleConsumerManager(URI uri, String topic, int partitionId) {
    this(uri, topic, partitionId, Integer.MAX_VALUE);
  }

  /**
   * Create SimpleConsumer instance.
   *
   * @param uri Kafka Tablespace URI. ex) kafka://localhost:9092,localhost:9091
   * @param topic topic name
   * @param partitionId partition id
   * @param fragmentSize max polling size of kafka
   */
  public SimpleConsumerManager(URI uri, String topic, int partitionId, int fragmentSize) {
    String clientId = SimpleConsumerManager.createIdentifier("TCons");
    Properties props = getDefaultProperties(uri, clientId, fragmentSize);

    partition = new TopicPartition(topic, partitionId);
    consumer = new KafkaConsumer<>(props);
    consumer.assign(Collections.singletonList(partition));
  }

  /**
   * Close consumer.
   */
  @Override
  public void close() {
    if (consumer != null) {
      consumer.close();
    }
    consumer = null;
  }

  /**
   * Get the earliest offset.
   *
   * @return the earliest offset
   */
  public long getEarliestOffset() {
    long currentPosition = consumer.position(partition);
    consumer.seekToBeginning(Collections.singletonList(partition));
    long earliestPosition = consumer.position(partition);
    consumer.seek(partition, currentPosition);
    return earliestPosition;
  }

  /**
   * Get the latest offset.
   *
   * @return the latest offset
   */
  public long getLatestOffset() {
    long currentPosition = consumer.position(partition);
    consumer.seekToEnd(Collections.singletonList(partition));
    long latestPosition = consumer.position(partition);
    consumer.seek(partition, currentPosition);
    return latestPosition;
  }

  /**
   * Poll data from kafka.
   *
   * @param offset position of partition to seek.
   * @param timeout polling timeout.
   * @return records of topic.
   */
  public ConsumerRecords<byte[], byte[]> poll(long offset, long timeout) {
    consumer.seek(partition, offset);

    return consumer.poll(timeout);
  }

  /**
   * Return partition information list of specific topic.
   *
   * @param uri Kafka Tablespace URI
   * @param topic
   * @return
   * @throws IOException
   */
  static List<PartitionInfo> getPartitions(URI uri, String topic) throws IOException {
    String clientId = SimpleConsumerManager.createIdentifier("TPart");
    Properties props = getDefaultProperties(uri, clientId, Integer.MAX_VALUE);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)) {
      return consumer.partitionsFor(topic);
    }
  }

  /**
   * It extracts broker addresses from a kafka Tablespace URI.
   * For example, consider an example URI 'kafka://host1:9092,host2:9092,host3:9092'.
   * <code>extractBroker</code> will extract only 'host1:9092,host2:9092,host3:9092'.
   *
   * @param uri Kafka Tablespace URI
   * @return Broker addresses
   */
  static String extractBroker(URI uri) {
    String uriStr = uri.toString();
    int start = uriStr.indexOf("/") + 2;
    int end = uriStr.indexOf("/", start);
    if (end < 0) {
      return uriStr.substring(start);
    } else {
      return uriStr.substring(start, end);
    }
  }

  /**
   * Gets the default properties.
   *
   * @param uri kafka broker URIs
   * @param clientId
   * @param fragmentSize
   * @return the default properties
   */
  private static Properties getDefaultProperties(URI uri, String clientId, int fragmentSize) {
    Properties props = new Properties();
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, extractBroker(uri));
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, fragmentSize);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }

  /**
   * Create identifier for SimpleConsumer.
   * The SimpleConsumer connects at kafka using this identifier.
   *
   * @param prefix
   * @return
   */
  private static String createIdentifier(String prefix) {
    Random r = new Random();
    return prefix + "_" + r.nextInt(1000000) + "_" + System.currentTimeMillis();
  }
}
