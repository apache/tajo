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

import kafka.producer.KeyedMessage;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.kafka.fragment.KafkaFragment;
import org.apache.tajo.storage.kafka.testUtil.EmbeddedKafka;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestKafkaScanner {
  static EmbeddedKafka em_kafka;
  private static Schema schema = new Schema();

  static {
    schema.addColumn("col1", Type.INT4);
    schema.addColumn("col2", Type.TEXT);
    schema.addColumn("col3", Type.FLOAT4);
  }

  // Start up EmbeddedKafka and Generate test data.
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    em_kafka = EmbeddedKafka.createEmbeddedKafka(2181, 9092);
    em_kafka.start();
    em_kafka.createTopic(1, 1, TestConstants.test_topic);
    genDataForTest();
  }

  // Close EmbeddedKafka.
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    em_kafka.close();
    em_kafka = null;
  }

  // Test for readMessage.
  @Test
  public void testScanner() throws Exception {
    KafkaFragment fragment = new KafkaFragment(TestConstants.test_topic, TestConstants.test_topic, 0,
        em_kafka.getConnectString(), 0, 1);
    KafkaScanner scanner = new KafkaScanner(new TajoConf(), schema,
        CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV), fragment);
    scanner.init();
    Tuple tuple = scanner.next();
    assertTrue(tuple.getInt4(0) == 1);
    assertTrue(tuple.getText(1).equals("abc"));
    assertTrue(tuple.getFloat4(2) == 0.2f);
    scanner.close();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void genDataForTest() throws Exception {
    kafka.javaapi.producer.Producer producer = null;
    try {
      producer = em_kafka.createProducer(em_kafka.getConnectString());
      List<KeyedMessage<Integer, String>> messageList = new ArrayList<KeyedMessage<Integer, String>>();
      messageList.add(new KeyedMessage<Integer, String>(TestConstants.test_topic, TestConstants.test_data[0]));
      producer.send(messageList);
    } finally {
      if (null != producer) {
        producer.close();
      }
    }
  }
}
