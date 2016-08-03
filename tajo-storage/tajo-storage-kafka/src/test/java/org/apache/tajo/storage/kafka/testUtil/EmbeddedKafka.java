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

package org.apache.tajo.storage.kafka.testUtil;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.testing.FileUtils.deleteRecursively;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.io.Files;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class EmbeddedKafka implements Closeable {
  private final EmbeddedZookeeper zookeeper;
  private final int port;
  private final File kafkaDataDir;
  private final KafkaServerStartable kafka;

  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean stopped = new AtomicBoolean();

  public static EmbeddedKafka createEmbeddedKafka(int zookeeperPort, int kafkaPort) throws IOException {
    return new EmbeddedKafka(new EmbeddedZookeeper(zookeeperPort), kafkaPort);
  }

  EmbeddedKafka(EmbeddedZookeeper zookeeper, int kafkaPort) throws IOException {
    this.zookeeper = checkNotNull(zookeeper, "zookeeper is null");

    this.port = kafkaPort;
    this.kafkaDataDir = Files.createTempDir();

    Properties properties = new Properties();
    properties.setProperty("broker.id", "0");
    properties.setProperty("host.name", "localhost");
    properties.setProperty("num.partitions", "2");
    properties.setProperty("log.flush.interval.messages", "10000");
    properties.setProperty("log.flush.interval.ms", "1000");
    properties.setProperty("log.retention.minutes", "60");
    properties.setProperty("log.segment.bytes", "1048576");
    properties.setProperty("auto.create.topics.enable", "false");
    properties.setProperty("zookeeper.connection.timeout.ms", "1000000");
    properties.setProperty("port", Integer.toString(port));
    properties.setProperty("log.dirs", kafkaDataDir.getAbsolutePath());
    properties.setProperty("zookeeper.connect", zookeeper.getConnectString());

    KafkaConfig config = new KafkaConfig(properties);
    this.kafka = new KafkaServerStartable(config);
  }

  public void start() throws InterruptedException, IOException {
    if (!started.getAndSet(true)) {
      zookeeper.start();
      kafka.startup();
    }
  }

  @Override
  public void close() {
    if (started.get() && !stopped.getAndSet(true)) {
      kafka.shutdown();
      kafka.awaitShutdown();
      zookeeper.close();
      deleteRecursively(kafkaDataDir);
    }
  }

  public int getZookeeperPort() {
    return zookeeper.getPort();
  }

  public int getPort() {
    return port;
  }

  public String getConnectString() {
    return "localhost:" + Integer.toString(port);
  }

  public String getZookeeperConnectString() {
    return zookeeper.getConnectString();
  }

  public void createTopic(int partitions, int replication, String topic) {
    checkState(started.get() && !stopped.get(), "not started!");

    ZkClient zkClient = new ZkClient(getZookeeperConnectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    try {
      AdminUtils.createTopic(ZkUtils.apply(zkClient, false), topic, partitions, replication, new Properties(),
          RackAwareMode.Enforced$.MODULE$);
    } finally {
      zkClient.close();
    }
  }

  public Producer<String, String> createProducer(String connecting) {
    Properties properties = new Properties();
    properties.put("key.serializer", StringSerializer.class);
    properties.put("value.serializer", StringSerializer.class);
    properties.put("bootstrap.servers", connecting);
    Producer<String, String> producer = new KafkaProducer<String, String>(properties);
    return producer;
  }
}
