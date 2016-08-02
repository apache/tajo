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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.tajo.storage.kafka.testUtil.EmbeddedKafka;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class TestSimpleConsumerManager {
  static EmbeddedKafka em_kafka;

  // Start up EmbeddedKafka and Generate test data.
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    em_kafka = EmbeddedKafka.createEmbeddedKafka(2181, 9092);
    em_kafka.start();
    em_kafka.createTopic(TestConstants.kafka_partition_num, 1, TestConstants.test_topic);
    genDataForTest();
  }

  // Close EmbeddedKafka.
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    em_kafka.close();
  }

  @Test
  public void testExtractBroker() {
    assertEquals("host1:9092,host2:9092,host3:9092",
        SimpleConsumerManager.extractBroker(URI.create("kafka://host1:9092,host2:9092,host3:9092")));
  }

  // Test for getting topic partitions.
  @Test
  public void testGetPartitions() throws Exception {
    int prtition_num = SimpleConsumerManager
        .getPartitions(new URI("kafka://" + em_kafka.getConnectString()), TestConstants.test_topic)
        .size();
    assertTrue(prtition_num == TestConstants.kafka_partition_num);
  }

  // Test for to fetch data from kafka.
  @Test
  public void testFetchData() throws Exception {
    Set<String> receivedDataSet = new HashSet<>();
    for (PartitionInfo partitionInfo : SimpleConsumerManager.getPartitions(
        new URI("kafka://" + em_kafka.getConnectString()),
        TestConstants.test_topic)) {
      SimpleConsumerManager cm = new SimpleConsumerManager(new URI("kafka://" + em_kafka.getConnectString()),
          TestConstants.test_topic, partitionInfo.partition());
      cm.assign();

      long startOffset = cm.getEarliestOffset();
      long lastOffset = cm.getLatestOffset();
      if (startOffset < lastOffset) {
        for (ConsumerRecord<byte[], byte[]> message : cm.poll(startOffset, Long.MAX_VALUE)) {
          receivedDataSet.add(new String(message.value(), "UTF-8"));
        }
      }
    }
    for (String td : TestConstants.test_data) {
      assertTrue(receivedDataSet.contains(td));
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static void genDataForTest() throws Exception {
    Producer producer = null;
    try {
      producer = em_kafka.createProducer(em_kafka.getConnectString());

      producer.send(new ProducerRecord<String, String>(TestConstants.test_topic, TestConstants.test_data[0]));
      producer.send(new ProducerRecord<String, String>(TestConstants.test_topic, TestConstants.test_data[1]));
      producer.send(new ProducerRecord<String, String>(TestConstants.test_topic, TestConstants.test_data[2]));
      producer.send(new ProducerRecord<String, String>(TestConstants.test_topic, TestConstants.test_data[3]));
      producer.send(new ProducerRecord<String, String>(TestConstants.test_topic, TestConstants.test_data[4]));
    } finally {
      if (null != producer) {
        producer.close();
      }
    }
  }
}
