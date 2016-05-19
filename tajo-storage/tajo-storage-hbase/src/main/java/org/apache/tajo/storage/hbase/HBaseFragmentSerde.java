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

package org.apache.tajo.storage.hbase;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage.Builder;
import org.apache.tajo.storage.fragment.FragmentSerde;
import org.apache.tajo.storage.hbase.StorageFragmentProtos.HBaseFragmentProto;

import java.net.URI;

public class HBaseFragmentSerde implements FragmentSerde<HBaseFragment, HBaseFragmentProto> {

  @Override
  public Builder newBuilder() {
    return HBaseFragmentProto.newBuilder();
  }

  @Override
  public HBaseFragmentProto serialize(HBaseFragment fragment) {
    return HBaseFragmentProto.newBuilder()
        .setUri(fragment.getUri().toASCIIString())
        .setTableName(fragment.getInputSourceId())
        .setHbaseTableName(fragment.getHbaseTableName())
        .setStartRow(ByteString.copyFrom(fragment.getStartKey().getBytes()))
        .setStopRow(ByteString.copyFrom(fragment.getEndKey().getBytes()))
        .setLast(fragment.isLast())
        .setLength(fragment.getLength())
        .setRegionLocation(fragment.getHostNames().get(0))
        .build();
  }

  @Override
  public HBaseFragment deserialize(HBaseFragmentProto proto) {
    return new HBaseFragment(
        URI.create(proto.getUri()),
        proto.getTableName(),
        proto.getHbaseTableName(),
        proto.getStartRow().toByteArray(),
        proto.getStopRow().toByteArray(),
        proto.getRegionLocation(),
        proto.getLast());
  }
}
