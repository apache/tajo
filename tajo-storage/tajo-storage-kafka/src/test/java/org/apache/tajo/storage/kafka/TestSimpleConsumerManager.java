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

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;

import org.apache.tajo.storage.kafka.testUtil.EmbeddedKafka;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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

  // Test for getting topic partitions.
  @Test
  public void testGetPartitions() throws Exception {
    int prtition_num = SimpleConsumerManager.getPartitions(em_kafka.getConnectString(), TestConstants.test_topic)
        .size();
    assertTrue(prtition_num == TestConstants.kafka_partition_num);
  }

  //Test for to fetch data from kafka.
  @Test
  public void testFetchData() throws Exception {
    StringBuilder fatchData = new StringBuilder();
    for (Integer partitionId : SimpleConsumerManager.getPartitions(em_kafka.getConnectString(),
        TestConstants.test_topic)) {
      SimpleConsumerManager cm = SimpleConsumerManager.getSimpleConsumerManager(em_kafka.getConnectString(),
          TestConstants.test_topic, partitionId);
      long startOffset = cm.getReadOffset(kafka.api.OffsetRequest.EarliestTime());
      long lastOffset = cm.getReadOffset(kafka.api.OffsetRequest.LatestTime());
      if (startOffset < lastOffset) {
        for (MessageAndOffset message : cm.fetch(startOffset)) {
          ByteBuffer payload = message.message().payload();
          byte[] data = new byte[payload.limit()];
          payload.get(data);
          fatchData.append(new String(data, "UTF-8"));
        }
      }
    }
    StringBuilder testData = new StringBuilder();
    for(String td : TestConstants.test_data){
      testData.append(td);
    }
    assertTrue(fatchData.toString().equals(testData.toString()));
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static void genDataForTest() throws Exception {
    kafka.javaapi.producer.Producer producer = null;
    try {
      producer = em_kafka.createProducer(em_kafka.getConnectString());
      List<KeyedMessage<Integer, String>> messageList = new ArrayList<KeyedMessage<Integer, String>>();
      messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[0]));
      messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[1]));
      messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[2]));
      messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[3]));
      messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[4]));
      producer.send(messageList);
    } finally {
      if (null != producer) {
        producer.close();
      }
    }
  }
}
