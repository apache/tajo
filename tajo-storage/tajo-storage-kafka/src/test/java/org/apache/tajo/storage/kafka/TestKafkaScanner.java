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

import static org.apache.tajo.storage.kafka.KafkaTestUtil.TOPIC_NAME;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.producer.Producer;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.kafka.server.EmbeddedKafka;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;

public class TestKafkaScanner {
  private static EmbeddedKafka KAFKA;
  private static Schema TABLE_SCHEMA;
  private static URI KAFKA_SERVER_URI;

  static {
    TABLE_SCHEMA = SchemaBuilder.builder()
        .add("col1", Type.INT4)
        .add("col2", Type.TEXT)
        .add("col3", Type.FLOAT4)
        .build();
  }

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
      KafkaTestUtil.sendTestData(producer, TOPIC_NAME);
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

  /**
   * Test for readMessage.
   *
   * @throws Exception
   */
  @Test
  public void testScanner() throws Exception {
    KafkaFragment fragment = new KafkaFragment(KAFKA_SERVER_URI, TOPIC_NAME, TOPIC_NAME, 0, 1, 0, "localhost");
    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.KAFKA, new TajoConf());
    KafkaScanner scanner = new KafkaScanner(conf, TABLE_SCHEMA, meta, fragment);
    scanner.init();
    Tuple tuple = scanner.next();
    assertTrue(tuple.getInt4(0) == 1);
    assertTrue(tuple.getText(1).equals("abc"));
    assertTrue(tuple.getFloat4(2) == 0.2f);
    scanner.close();
  }
}
