/*
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

import static org.apache.tajo.storage.kafka.TestConstants.DEFAULT_TEST_PARTITION_NUM;

import org.apache.kafka.clients.producer.Producer;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.kafka.server.EmbeddedKafka;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;

import net.minidev.json.JSONObject;

public class TestKafkaQuery extends QueryTestCaseBase {

  private static final String TEST_TOPIC_USER = "TEST_TOPIC_USER";
  private static final String TEST_TOPIC_PROD = "TEST_TOPIC2_PROD";

  private static EmbeddedKafka KAFKA;
  private static URI KAFKA_SERVER_URI;

  @BeforeClass
  public static void setup() throws Exception {
    KAFKA = EmbeddedKafka.createEmbeddedKafka(2181, 9092);
    KAFKA.start();
    KAFKA.createTopic(DEFAULT_TEST_PARTITION_NUM, 1, TEST_TOPIC_USER);
    KAFKA.createTopic(DEFAULT_TEST_PARTITION_NUM, 1, TEST_TOPIC_PROD);
    KAFKA_SERVER_URI = URI.create("kafka://" + KAFKA.getConnectString());

    // Load test data.
    try (Producer<String, String> producer = KAFKA.createProducer(KAFKA.getConnectString())) {
      TestConstants.sendTestData(producer, TEST_TOPIC_USER, "1|user1");
      TestConstants.sendTestData(producer, TEST_TOPIC_USER, "2|user2");
      TestConstants.sendTestData(producer, TEST_TOPIC_USER, "3|user3");
      TestConstants.sendTestData(producer, TEST_TOPIC_USER, "4|user4");
      TestConstants.sendTestData(producer, TEST_TOPIC_USER, "6|user6");
      for (int i = 0; i < 2; i++) {
        TestConstants.sendTestData(producer, TEST_TOPIC_PROD);
      }
    }

    JSONObject configElements = new JSONObject();
    KafkaTablespace hBaseTablespace = new KafkaTablespace("cluster1", KAFKA_SERVER_URI, configElements);
    hBaseTablespace.init(new TajoConf());
    TablespaceManager.addTableSpaceForTest(hBaseTablespace);

    QueryTestCaseBase.testingCluster.getMaster().refresh();
  }

  @AfterClass
  public static void teardown() throws Exception {
    KAFKA.close();
  }

  @Before
  public void prepareTables() throws TajoException {
    String createSql = "create table user (id int4, name text) tablespace cluster1 using kafka with ('kafka.topic'='"
      + TEST_TOPIC_USER + "')";
    executeString(createSql);

    createSql = "create table prod (id int4, prod_name text, point float4) tablespace cluster1 using kafka with ('kafka.topic'='"
      + TEST_TOPIC_PROD + "')";
    executeString(createSql);
  }

  @After
  public void dropTables() throws TajoException {
    executeString("drop table user");
    executeString("drop table prod");
  }

  @SimpleTest
  @Test
  public void testSelect() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testGroupby() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testJoin() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testLeftOuterJoin() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testFullOuterJoin() throws Exception {
    runSimpleTests();
  }
}
