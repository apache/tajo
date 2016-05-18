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

package org.apache.tajo.storage.fragment;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.storage.StorageFragmentProtos.PartitionFileFragmentProto;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class PartitionFileFragment extends FileFragment {

  @Expose private String partitionKeys; // required

  public PartitionFileFragment(String tableName, Path uri, BlockLocation blockLocation,
    String partitionKeys) throws IOException {
    super(tableName, uri, blockLocation);
    this.partitionKeys = partitionKeys;
  }
  public PartitionFileFragment(String tableName, Path uri, long start, long length, String[] hosts,
    String partitionKeys) {
    super(tableName, uri, start, length, hosts);
    this.partitionKeys = partitionKeys;
  }

  public PartitionFileFragment(String fragmentId, Path path, long start, long length, String partitionKeys) {
    super(fragmentId, path, start, length);
    this.partitionKeys = partitionKeys;
  }

  public String getPartitionKeys() {
    return partitionKeys;
  }

  public void setPartitionKeys(String partitionKeys) {
    this.partitionKeys = partitionKeys;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PartitionFileFragment) {
      PartitionFileFragment t = (PartitionFileFragment) o;
      if (getPath().equals(t.getPath())
        && TUtil.checkEquals(t.getStartKey(), this.getStartKey())
        && TUtil.checkEquals(t.getLength(), this.getLength())
        && getPartitionKeys().equals(t.getPartitionKeys())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(inputSourceId, uri, startKey, endKey, length, getDiskIds(), hostNames, partitionKeys);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    PartitionFileFragment frag = (PartitionFileFragment) super.clone();
    frag.setPartitionKeys(getPartitionKeys());
    return frag;
  }

  @Override
  public String toString() {
    return "\"fragment\": {\"id\": \""+ inputSourceId +"\", \"path\": "
      +getPath() + "\", \"start\": " + this.getStartKey() + ",\"length\": "
      + getLength() + "\", \"partitionKeys\":" + getPartitionKeys() + "}" ;
  }

}
