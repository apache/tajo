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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import kafka.producer.KeyedMessage;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.kafka.testUtil.EmbeddedKafka;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestKafkaStorageManager {
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
    em_kafka = null;
  }

  // Test for getNonForwardSplit.
  @Test
  public void testGetNonForwardSplit() throws Exception {
    KafkaStorageManager ksm = new KafkaStorageManager(CatalogProtos.StoreType.CSV);
    ksm.storageInit();
    try {
      TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
      Map<String, String> option = new java.util.HashMap<String, String>();
      option.put(KafkaStorageConstants.KAFKA_BROKER, em_kafka.getConnectString());
      option.put(KafkaStorageConstants.KAFKA_TOPIC, TestConstants.test_topic);
      option.put(KafkaStorageConstants.KAFKA_FRAGMENT_SIZE, "10");
      meta.setOptions(new KeyValueSet(option));
      TableDesc td = new TableDesc("test_table", null, meta, null);
      // for 100 message. 10 message per one fragment. 5 fragments per one page.
      int fragmentCount = 0;
      int pageCount = 0;
      int fragmentPerPage = 5;
      while (true) {
        List<Fragment> fragmentList = ksm.getNonForwardSplit(td, fragmentCount, fragmentPerPage);
        fragmentCount += fragmentList.size();
        if (fragmentList.size() == 0)
          break;
        pageCount += 1;
      }
      // 100 message = 2 page * ( 10 fragment * 5 )
      assertTrue(100 == (pageCount * fragmentCount * fragmentPerPage));
    } finally {
      ksm.closeStorageManager();
    }
  }

  // Test for getSplit.
  @Test
  public void testGetSplit() throws Exception {
    KafkaStorageManager ksm = new KafkaStorageManager(CatalogProtos.StoreType.CSV);
    ksm.storageInit();
    try {
      TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
      Map<String, String> option = new java.util.HashMap<String, String>();
      option.put(KafkaStorageConstants.KAFKA_BROKER, em_kafka.getConnectString());
      option.put(KafkaStorageConstants.KAFKA_TOPIC, TestConstants.test_topic);
      option.put(KafkaStorageConstants.KAFKA_FRAGMENT_SIZE, "10");
      meta.setOptions(new KeyValueSet(option));
      TableDesc td = new TableDesc("test_table", null, meta, null);
      List<Fragment> fragmentList = ksm.getSplits("", td, null);
      // 100 message = 10 fragments * KAFKA_FRAGMENT_SIZE
      assertTrue(100 == (fragmentList.size() * 10));
    } finally {
      ksm.closeStorageManager();
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static void genDataForTest() throws Exception {
    kafka.javaapi.producer.Producer producer = null;
    try {
      producer = em_kafka.createProducer(em_kafka.getConnectString());
      // Generate 100 message.
      for (int i = 0; i < 20; i++) {
        List<KeyedMessage<Integer, String>> messageList = new ArrayList<KeyedMessage<Integer, String>>();
        messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[0]));
        messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[1]));
        messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[2]));
        messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[3]));
        messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[4]));
        producer.send(messageList);
      }
    } finally {
      if (null != producer) {
        producer.close();
      }
    }
  }
}
