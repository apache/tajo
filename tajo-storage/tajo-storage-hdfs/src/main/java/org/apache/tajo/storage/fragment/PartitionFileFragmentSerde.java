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

package org.apache.tajo.storage.fragment;

import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.storage.StorageFragmentProtos;

import java.util.ArrayList;
import java.util.List;

public class PartitionFileFragmentSerde implements FragmentSerde<PartitionFileFragment,
  StorageFragmentProtos.PartitionFileFragmentProto> {

  @Override
  public GeneratedMessage.Builder newBuilder() {
    return StorageFragmentProtos.PartitionFileFragmentProto.newBuilder();
  }

  @Override
  public StorageFragmentProtos.PartitionFileFragmentProto serialize(PartitionFileFragment fragment) {
    StorageFragmentProtos.PartitionFileFragmentProto.Builder builder =
      StorageFragmentProtos.PartitionFileFragmentProto.newBuilder();
    builder.setId(fragment.inputSourceId);
    builder.setStartOffset(fragment.startKey);
    builder.setLength(fragment.length);
    builder.setPath(fragment.getPath().toString());
    if(fragment.getDiskIds() != null) {
      List<Integer> idList = new ArrayList<>();
      for(int eachId: fragment.getDiskIds()) {
        idList.add(eachId);
      }
      builder.addAllDiskIds(idList);
    }

    if(fragment.hostNames != null) {
      builder.addAllHosts(fragment.hostNames);
    }

    if(fragment.getPartitionKeys() != null) {
      builder.setPartitionKeys(fragment.getPartitionKeys());
    }
    return builder.build();
  }

  @Override
  public PartitionFileFragment deserialize(StorageFragmentProtos.PartitionFileFragmentProto proto) {
    return new PartitionFileFragment(
      proto.getId(),
      new Path(proto.getPath()),
      proto.getStartOffset(),
      proto.getLength(),
      proto.getHostsList().toArray(new String[proto.getHostsCount()]),
      proto.getDiskIdsList().toArray(new Integer[proto.getDiskIdsCount()]),
      proto.getPartitionKeys());
  }

}
