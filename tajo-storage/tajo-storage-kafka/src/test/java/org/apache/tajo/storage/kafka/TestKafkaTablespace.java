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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.kafka.testUtil.EmbeddedKafka;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class TestKafkaTablespace {
  static EmbeddedKafka em_kafka;
  private static String tableSpaceUri = "kafka://localhost:9092";

  // Start up EmbeddedKafka and Generate test data.
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    KafkaTablespace hBaseTablespace = new KafkaTablespace("cluster1", URI.create(tableSpaceUri), null);
    hBaseTablespace.init(new TajoConf());
    TablespaceManager.addTableSpaceForTest(hBaseTablespace);

    em_kafka = EmbeddedKafka.createEmbeddedKafka(2181, 9092);
    em_kafka.start();
    em_kafka.createTopic(TestConstants.kafka_partition_num, 1, TestConstants.test_topic);
    genDataForTest();
  }

  @Test
  public void testTablespaceHandler() throws Exception {
    assertTrue((TablespaceManager.getByName("cluster1")) instanceof KafkaTablespace);
    assertTrue((TablespaceManager.get(URI.create(tableSpaceUri))) instanceof KafkaTablespace);
  }

  // Close EmbeddedKafka.
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (em_kafka != null) {
      em_kafka.close();
    }
    em_kafka = null;
  }

  // Test for getSplit.
  @Test
  public void testGetSplit() throws Exception {
    TableMeta meta = CatalogUtil.newTableMeta("KAFKA", new TajoConf());
    Map<String, String> option = new java.util.HashMap<String, String>();
    option.put(KafkaStorageConstants.KAFKA_TOPIC, TestConstants.test_topic);
    option.put(KafkaStorageConstants.KAFKA_FRAGMENT_SIZE, "10");
    meta.setPropertySet(new KeyValueSet(option));
    TableDesc td = new TableDesc("test_table", null, meta, null);
    KafkaTablespace kafkaTablespace = TablespaceManager.getByName("cluster1");
    List<Fragment> fragmentList = kafkaTablespace.getSplits("", td, false, null);
    long totalCount = 0;
    for (int i = 0; i < fragmentList.size(); i++) {
      totalCount += fragmentList.get(i).getLength();
    }
    assertEquals(100, totalCount);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static void genDataForTest() throws Exception {
    Producer producer = null;
    try {
      producer = em_kafka.createProducer(em_kafka.getConnectString());
      // Generate 100 message.
      for (int i = 0; i < 20; i++) {
        producer.send(new ProducerRecord<String, String>(TestConstants.test_topic, TestConstants.test_data[0]));
        producer.send(new ProducerRecord<String, String>(TestConstants.test_topic, TestConstants.test_data[1]));
        producer.send(new ProducerRecord<String, String>(TestConstants.test_topic, TestConstants.test_data[2]));
        producer.send(new ProducerRecord<String, String>(TestConstants.test_topic, TestConstants.test_data[3]));
        producer.send(new ProducerRecord<String, String>(TestConstants.test_topic, TestConstants.test_data[4]));
      }
    } finally {
      if (null != producer) {
        producer.close();
      }
    }
  }
}
