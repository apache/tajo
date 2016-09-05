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

import org.apache.tajo.storage.fragment.FragmentSerde;
import org.apache.tajo.storage.kafka.StorageFragmentProtos.KafkaFragmentProto;

import java.net.URI;

import com.google.protobuf.GeneratedMessage.Builder;

public class KafkaFragmentSerde implements FragmentSerde<KafkaFragment, KafkaFragmentProto> {

  @Override
  public Builder newBuilder() {
    return KafkaFragmentProto.newBuilder();
  }

  @Override
  public KafkaFragmentProto serialize(KafkaFragment fragment) {
    return KafkaFragmentProto.newBuilder()
        .setUri(fragment.getUri().toASCIIString())
        .setTableName(fragment.getInputSourceId())
        .setTopicName(fragment.getTopicName())
        .setStartOffset(fragment.getStartKey().getOffset())
        .setLastOffset(fragment.getEndKey().getOffset())
        .setPartitionId(fragment.getPartitionId())
        .setLast(fragment.isLast())
        .setLength(fragment.getLength())
        .setLeaderHost(fragment.getHostNames().get(0))
        .build();
  }

  @Override
  public KafkaFragment deserialize(KafkaFragmentProto proto) {
    return new KafkaFragment(
        URI.create(proto.getUri()),
        proto.getTableName(),
        proto.getTopicName(),
        proto.getStartOffset(),
        proto.getLastOffset(),
        proto.getPartitionId(),
        proto.getLeaderHost(),
        proto.getLast());
  }
}
