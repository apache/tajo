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

import org.apache.kafka.clients.producer.Producer;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.kafka.server.EmbeddedKafka;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;

import net.minidev.json.JSONObject;

public class TestKafkaTablespace {
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
    KAFKA.createTopic(1, 1, TOPIC_NAME);
    KAFKA_SERVER_URI = URI.create("kafka://" + KAFKA.getConnectString());

    // Load test data.
    try (Producer<String, String> producer = KAFKA.createProducer(KAFKA.getConnectString())) {
      for (int i = 0; i < 20; i++) {
        TestConstants.sendTestData(producer, TOPIC_NAME);
      }
    }

    JSONObject configElements = new JSONObject();
    KafkaTablespace hBaseTablespace = new KafkaTablespace("cluster1", KAFKA_SERVER_URI, configElements);
    hBaseTablespace.init(new TajoConf());
    TablespaceManager.addTableSpaceForTest(hBaseTablespace);
  }

  @Test
  public void testTablespaceHandler() throws Exception {
    assertTrue((TablespaceManager.getByName("cluster1")) instanceof KafkaTablespace);
    assertTrue((TablespaceManager.get(KAFKA_SERVER_URI)) instanceof KafkaTablespace);
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

  /**
   * Test for getSplit.
   *
   * @throws Exception
   */
  @Test
  public void testGetSplit() throws Exception {
    TableMeta meta = CatalogUtil.newTableMeta("KAFKA", new TajoConf());
    Map<String, String> option = new java.util.HashMap<String, String>();
    option.put(KafkaStorageConstants.KAFKA_TOPIC, TOPIC_NAME);
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
}
