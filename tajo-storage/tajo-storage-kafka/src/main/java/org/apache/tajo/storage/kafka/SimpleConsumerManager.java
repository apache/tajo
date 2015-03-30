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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.util.NetUtils;

// SimpleConsumerManager is kafka client for KafkaScanner.
// It's one per partition. Each partition instantiate this class.
public class SimpleConsumerManager {
  private static final Log LOG = LogFactory.getLog(SimpleConsumerManager.class);
  // TODO: configurable setting.
  static final int CONSUMER_TIMEOUT = 30000;
  static final int CONSUMER_BUFFER_SIZE = 64 * 1024;
  static final int CONSUMER_FETCH_SIZE = 300 * 1024;
  static final int FETCH_TRY_NUM = 3;

  private SimpleConsumer consumer = null;
  private List<InetSocketAddress> brokers = new ArrayList<InetSocketAddress>();
  private String topic;
  private int partition;
  private String clientId;
  // leader of this partition.
  private Broker leader;

  public SimpleConsumerManager(String seedBrokers, String topic, int partition) throws IOException {
    this.topic = topic;
    this.partition = partition;
    // Identifier of simpleConsumer.
    this.clientId = SimpleConsumerManager.getIdentifier();
    this.brokers = SimpleConsumerManager.getBrokerList(seedBrokers);
    this.leader = findLeader(topic, partition);
    // consumer creation fail.
    if (null == leader) {
      throw new IOException("consumer creation fail");
    } else {
      consumer = new SimpleConsumer(leader.host(), leader.port(), CONSUMER_TIMEOUT, CONSUMER_BUFFER_SIZE, clientId);
    }
  }

  /**
   * Create SimpleConsumer instance. seedBrokers is connection info of kafka
   * brokers. ex) localhost:9092,localhost:9091 topic is topic name. partition
   * is partition id.
   * 
   * @param seedBrokers
   * @param topic
   * @param partition
   * @return
   * @throws IOException
   */
  static public SimpleConsumerManager getSimpleConsumerManager(String seedBrokers, String topic, int partition)
      throws IOException {
    return new SimpleConsumerManager(seedBrokers, topic, partition);
  }

  /**
   * Return partition ID list of specific topic. Check for seedBrokers.
   * seedBrokers is kafka brokers.
   * 
   * @param seedBrokers
   * @param topic
   * @return
   * @throws IOException
   */
  static public Set<Integer> getPartitions(String seedBrokers, String topic) throws IOException {
    Set<Integer> partitions = new HashSet<Integer>();
    for (InetSocketAddress seed : SimpleConsumerManager.getBrokerList(seedBrokers)) {
      SimpleConsumer consumer = null;
      try {
        consumer = new SimpleConsumer(seed.getHostName(), seed.getPort(), CONSUMER_TIMEOUT, CONSUMER_BUFFER_SIZE,
            SimpleConsumerManager.getIdentifier() + "partitionLookup");
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
        // call to topicsMetadata() asks the Broker you are connected to for all
        // the details about the topic we are interested in
        List<TopicMetadata> metaData = resp.topicsMetadata();
        // loop on partitionsMetadata iterates through all the partitions until
        // we find the one we want.
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            partitions.add(part.partitionId());
          }
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new IOException(e);
      } finally {
        if (consumer != null)
          consumer.close();
      }
    }
    return partitions;
  }

  static private List<InetSocketAddress> getBrokerList(String brokers) {
    List<InetSocketAddress> brokerList = new ArrayList<InetSocketAddress>();
    for (String broker : brokers.split(",")) {
      brokerList.add(NetUtils.createUnresolved(broker));
    }
    return brokerList;
  }

  /**
   * Create identifier for SimpleConsumer. The SimpleConsumer connects at kafka
   * using this identifier.
   * 
   * @return
   */
  static private String getIdentifier() {
    Random r = new Random();
    return r.nextLong() + "_" + System.currentTimeMillis();
  }

  synchronized public void close() {
    if (null != consumer) {
      consumer.close();
    }
    consumer = null;
  }

  /**
   * Fetch data from kafka, as much as 'CONSUMER_FETCH_SIZE' size from offset.
   * 
   * @param offset
   * @return
   */
  @SuppressWarnings("unchecked")
  public List<MessageAndOffset> fetch(long offset) {
    List<MessageAndOffset> returnData = null;
    FetchRequest req = new FetchRequestBuilder().clientId(clientId)
        .addFetch(topic, partition, offset, CONSUMER_FETCH_SIZE).build();
    if (null != consumer) {
      FetchResponse fetchResponse = null;
      // If that fails, find new leader of partition and try again.
      for (int i = 0; i < FETCH_TRY_NUM; i++) {
        fetchResponse = consumer.fetch(req);
        if (fetchResponse.hasError()) {
          short code = fetchResponse.errorCode(topic, partition);
          LOG.error("Error fetching data from the Broker:" + leader + " Reason: " + code + " Try: " + i);
          if (ErrorMapping.LeaderNotAvailableCode() == code || ErrorMapping.NotLeaderForPartitionCode() == code
              || ErrorMapping.RequestTimedOutCode() == code || ErrorMapping.BrokerNotAvailableCode() == code
              || ErrorMapping.ReplicaNotAvailableCode() == code) {
            Broker newLeader = findNewLeader();
            if (null != newLeader) {
              synchronized (consumer) {
                this.leader = newLeader;
                consumer.close();
                consumer = new SimpleConsumer(leader.host(), leader.port(), CONSUMER_TIMEOUT, CONSUMER_BUFFER_SIZE,
                    clientId);
              }
            }
          }
        } else {
          break;
        }
        fetchResponse = null;
      }
      if (null != fetchResponse) {
        Iterator<MessageAndOffset> messages = fetchResponse.messageSet(topic, partition).iterator();
        returnData = IteratorUtils.toList(messages);
      }
    }
    return returnData;
  }

  public long getReadOffset(long whichTime) throws IOException {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
    kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
        kafka.api.OffsetRequest.CurrentVersion(), clientId);
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      LOG.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
      throw new IOException("Error fetching data Offset Data the Broker");
    }
    long[] offsets = response.offsets(topic, partition);
    return offsets[0];
  }

  /**
   * find new leader of partition, if old leader fail.
   * 
   * @return
   */
  synchronized private Broker findNewLeader() {
    // retry for 3 time.
    for (int i = 0; i < 3; i++) {
      boolean goToSleep = false;
      Broker newLeader = findLeader(topic, partition);
      if (leader == null) {
        goToSleep = true;
      } else if (leader.host().equalsIgnoreCase(newLeader.host()) && leader.port() == newLeader.port() && i == 0) {
        // first time through if the leader hasn't changed give ZooKeeper a
        // second to recover
        // second time, assume the broker did recover before failover, or it was
        // a non-Broker issue
        goToSleep = true;
      } else {
        return newLeader;
      }
      if (goToSleep) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }
    }
    // Unable to find new leader after Broker failure.
    return null;
  }

  /**
   * Find leader broker of specific topic and partition
   * 
   * @param topic
   * @param partition
   * @return
   */
  synchronized private Broker findLeader(String topic, int partition) {
    PartitionMetadata returnMetaData = null;
    for (InetSocketAddress broker : brokers) {
      SimpleConsumer consumer = null;
      try {
        consumer = new SimpleConsumer(broker.getHostName(), broker.getPort(), CONSUMER_TIMEOUT, CONSUMER_BUFFER_SIZE,
            clientId + "_leaderLookup");
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
        // call to topicsMetadata() asks the Broker you are connected to for all
        // the details about the topic we are interested in
        List<TopicMetadata> metaData = resp.topicsMetadata();
        // loop on partitionsMetadata iterates through all the partitions until
        // we find the one we want.
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            if (part.partitionId() == partition) {
              returnMetaData = part;
              break;
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Error communicating with Broker [" + broker + "] to find Leader for [" + topic + ", " + partition
            + "] Reason: " + e);
      } finally {
        if (consumer != null)
          consumer.close();
      }
    }
    // Can't find metadata for Topic and Partition.
    if (returnMetaData == null) {
      return null;
    } else {
      // add replica broker info to replicaBrokers
      if (returnMetaData != null) {
        brokers.clear();
        for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
          brokers.add(NetUtils.createSocketAddr(replica.host(), replica.port()));
        }
      }
    }
    return returnMetaData.leader();
  }

}
