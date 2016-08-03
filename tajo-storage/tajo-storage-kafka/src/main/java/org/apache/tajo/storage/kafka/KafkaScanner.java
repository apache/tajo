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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.EmptyTuple;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.kafka.KafkaFragment.KafkaFragmentKey;
import org.apache.tajo.storage.text.DelimitedTextFile;
import org.apache.tajo.storage.text.TextLineDeserializer;
import org.apache.tajo.storage.text.TextLineParsingError;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class KafkaScanner implements Scanner {
  private static final Log LOG = LogFactory.getLog(KafkaScanner.class);
  protected boolean inited = false;

  private int numRows = 0;
  private int readRecordIndex = -1;
  private int fragmentSize;

  private long pollTimeout;

  private KafkaFragmentKey startKey;
  private KafkaFragmentKey endKey;
  private long currentOffset;

  private SimpleConsumerManager simpleConsumerManager;

  private List<ConsumerRecord<byte[], byte[]>> records = null;

  private Schema schema;
  private TableMeta meta;
  private TableStats tableStats;
  private KafkaFragment fragment;
  private Column[] targets;
  private TextLineDeserializer deserializer;
  private AtomicBoolean finished = new AtomicBoolean(false);

  private float progress = 0.0f;

  private Tuple outTuple;

  public KafkaScanner(Configuration conf, Schema schema, TableMeta meta, Fragment fragment) throws IOException {
    this.schema = schema;
    this.meta = meta;
    this.fragment = (KafkaFragment) fragment;
    this.tableStats = new TableStats();
  }

  @Override
  public void init() throws IOException {
    inited = true;

    if (targets == null) {
      targets = schema.toArray();
    }

    outTuple = new VTuple(targets.length);

    fragmentSize = Integer.parseInt(meta.getProperty(KafkaStorageConstants.KAFKA_FRAGMENT_SIZE,
        KafkaStorageConstants.DEFAULT_FRAGMENT_SIZE));

    pollTimeout = Long.parseLong(meta.getProperty(KafkaStorageConstants.KAFKA_POLL_TIMEOUT,
        KafkaStorageConstants.DEFAULT_POLL_TIMEOUT));

    // create deserializer. default is DELIMITER('|') text deserializer.
    deserializer = DelimitedTextFile.getLineSerde(meta).createDeserializer(schema, meta, targets);
    deserializer.init();

    simpleConsumerManager = new SimpleConsumerManager(fragment.getUri(), fragment.getTopicName(),
        fragment.getStartKey().getPartitionId(), fragmentSize);
    simpleConsumerManager.assign();

    initOffset();
  }

  private void initOffset() {
    startKey = fragment.getStartKey();
    endKey = fragment.getEndKey();
    currentOffset = startKey.getOffset();
  }

  @Override
  public Tuple next() throws IOException {
    if (finished.get()) {
      return null;
    }

    if (records == null || readRecordIndex >= records.size()) {
      records = readMessage();
      if (records.isEmpty()) {
        finished.set(true);
        progress = 1.0f;
        return null;
      }
      readRecordIndex = 0;
    }

    // A messages is fetched data list.
    // A numRows is current message index in messages.
    ConsumerRecord<byte[], byte[]> record = records.get(readRecordIndex++);
    byte[] value = record.value();
    if (value == null || targets.length == 0) {
      numRows++;
      return EmptyTuple.get();
    }

    ByteBuf buf = Unpooled.wrappedBuffer(value);

    try {
      deserializer.deserialize(buf, outTuple);
    } catch (TextLineParsingError tae) {
      throw new IOException(tae);
    } finally {
      numRows++;
    }
    return outTuple;
  }

  /**
   * Read message from kafka.
   *
   * @param messageSize
   * @return Received records
   * @throws IOException
   */
  private List<ConsumerRecord<byte[], byte[]>> readMessage() throws IOException {
    List<ConsumerRecord<byte[], byte[]>> receivedRecords = new ArrayList<>();
    if (currentOffset == endKey.getOffset()) {
      // If get the last offset, stop to read topic.
      return receivedRecords;
    }
    // Read data until lastOffset of partition of topic.
    // Read from simpleConsumer.
    LOG.info("Read the data of " + fragment + ", current offset: " + currentOffset);
    ConsumerRecords<byte[], byte[]> consumerRecords = simpleConsumerManager.poll(currentOffset, pollTimeout);
    if (consumerRecords.isEmpty()) {
      return receivedRecords;
    }

    long readLastOffset = -1;
    for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
      readLastOffset = consumerRecord.offset();
      if (readLastOffset < endKey.getOffset()) {
        receivedRecords.add(consumerRecord);
        readLastOffset++; // read a next message.
      } else {
        break;
      }
    }
    currentOffset = readLastOffset;
    return receivedRecords;
  }

  @Override
  public void reset() throws IOException {
    progress = 0.0f;
    readRecordIndex = -1;
    records = null;
    finished.set(false);
    tableStats = new TableStats();

    numRows = 0;

    initOffset();
  }

  @Override
  public void close() throws IOException {
    progress = 1.0f;
    finished.set(true);

    if (simpleConsumerManager != null) {
      simpleConsumerManager.close();
    }
    simpleConsumerManager = null;
  }

  @Override
  public void pushOperators(LogicalNode planPart) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public boolean isProjectable() {
    return true;
  }

  @Override
  public void setTarget(Column[] targets) {
    if (inited) {
      throw new IllegalStateException("Should be called before init()");
    }
    this.targets = targets;
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setFilter(EvalNode filter) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public void setLimit(long num) {
  }

  @Override
  public float getProgress() {
    return this.progress;
  }

  @Override
  public TableStats getInputStats() {
    if (tableStats != null) {
      tableStats.setNumRows(numRows);
    }
    return tableStats;
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }
}
