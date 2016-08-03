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

import static org.apache.tajo.storage.kafka.TestConstants.TOPIC_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.tajo.storage.kafka.server.EmbeddedKafka;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class TestSimpleConsumerManager {

  private static EmbeddedKafka KAFKA;

  private static URI KAFKA_SERVER_URI;

  /**
   * Start up EmbeddedKafka and Generate test data.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    KAFKA = EmbeddedKafka.createEmbeddedKafka(2181, 9092);
    KAFKA.start();
    KAFKA.createTopic(TestConstants.DEFAULT_TEST_PARTITION_NUM, 1, TOPIC_NAME);
    KAFKA_SERVER_URI = URI.create("kafka://" + KAFKA.getConnectString());

    // Load test data.
    try (Producer<String, String> producer = KAFKA.createProducer(KAFKA.getConnectString())) {
      TestConstants.sendTestData(producer, TOPIC_NAME);
    }
  }

  /**
   * Close EmbeddedKafka.
   *
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    KAFKA.close();
  }

  @Test
  public void testExtractBroker() {
    assertEquals("host1:9092,host2:9092,host3:9092",
        SimpleConsumerManager.extractBroker(URI.create("kafka://host1:9092,host2:9092,host3:9092")));
  }

  /**
   * Test for getting topic partitions.
   *
   * @throws Exception
   */
  @Test
  public void testGetPartitions() throws Exception {
    int prtition_num = SimpleConsumerManager.getPartitions(KAFKA_SERVER_URI, TOPIC_NAME).size();
    assertTrue(prtition_num == TestConstants.DEFAULT_TEST_PARTITION_NUM);
  }

  // Test for to fetch data from kafka.
  @Test
  public void testFetchData() throws Exception {
    Set<String> receivedDataSet = new HashSet<>();
    for (PartitionInfo partitionInfo : SimpleConsumerManager.getPartitions(KAFKA_SERVER_URI, TOPIC_NAME)) {
      int partitionId = partitionInfo.partition();
      try (SimpleConsumerManager cm = new SimpleConsumerManager(KAFKA_SERVER_URI, TOPIC_NAME, partitionId)) {
        long startOffset = cm.getEarliestOffset();
        long lastOffset = cm.getLatestOffset();
        if (startOffset < lastOffset) {
          for (ConsumerRecord<byte[], byte[]> message : cm.poll(startOffset, Long.MAX_VALUE)) {
            receivedDataSet.add(new String(message.value(), "UTF-8"));
          }
        }
      }
    }

    TestConstants.equalTestData(receivedDataSet);
  }
}
