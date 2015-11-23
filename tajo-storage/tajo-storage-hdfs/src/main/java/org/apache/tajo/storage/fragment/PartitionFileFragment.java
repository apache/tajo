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

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class PartitionFileFragment implements Fragment, Comparable<PartitionFileFragment>, Cloneable {
  @Expose private String tableName; // required
  @Expose private Path uri; // required
  @Expose public Long startOffset; // required
  @Expose public Long length; // required

  private String[] hosts; // Datanode hostnames

  @Expose private String partitionKeys; // required

  public PartitionFileFragment(ByteString raw) throws InvalidProtocolBufferException {
    PartitionFileFragmentProto.Builder builder = PartitionFileFragmentProto.newBuilder();
    builder.mergeFrom(raw);
    builder.build();
    init(builder.build());
  }

  public PartitionFileFragment(String tableName, Path uri, BlockLocation blockLocation, String partitionKeys)
      throws IOException {
    this.set(tableName, uri, blockLocation.getOffset(), blockLocation.getLength(), blockLocation.getHosts(), null,
      partitionKeys);
  }

  public PartitionFileFragment(String tableName, Path uri, long start, long length, String[] hosts, int[] diskIds,
                               String partitionKeys) {
    this.set(tableName, uri, start, length, hosts, diskIds, partitionKeys);
  }

  // Non splittable
  public PartitionFileFragment(String tableName, Path uri, long start, long length, String[] hosts,
                               String partitionKeys) {
    this.set(tableName, uri, start, length, hosts, null, partitionKeys);
  }

  public PartitionFileFragment(String fragmentId, Path path, long start, long length, String partitionKeys) {
    this.set(fragmentId, path, start, length, null, null, partitionKeys);
  }

  public PartitionFileFragment(PartitionFileFragmentProto proto) {
    init(proto);
  }

  private void init(PartitionFileFragmentProto proto) {
    int[] diskIds = new int[proto.getDiskIdsList().size()];
    int i = 0;
    for(Integer eachValue: proto.getDiskIdsList()) {
      diskIds[i++] = eachValue;
    }
    this.set(proto.getId(), new Path(proto.getPath()),
        proto.getStartOffset(), proto.getLength(),
        proto.getHostsList().toArray(new String[]{}),
        diskIds,
        proto.getPartitionKeys());
  }

  private void set(String tableName, Path path, long start,
      long length, String[] hosts, int[] diskIds, String partitionKeys) {
    this.tableName = tableName;
    this.uri = path;
    this.startOffset = start;
    this.length = length;
    this.hosts = hosts;
    this.partitionKeys = partitionKeys;
  }


  /**
   * Get the list of hosts (hostname) hosting this block
   */
  public String[] getHosts() {
    if (hosts == null) {
      this.hosts = new String[0];
    }
    return this.hosts;
  }

  @Override
  public String getTableName() {
    return this.tableName;
  }

  public Path getPath() {
    return this.uri;
  }

  public void setPath(Path path) {
    this.uri = path;
  }

  public Long getStartKey() {
    return this.startOffset;
  }

  public String getPartitionKeys() {
    return partitionKeys;
  }

  public void setPartitionKeys(String partitionKeys) {
    this.partitionKeys = partitionKeys;
  }

  @Override
  public String getKey() {
    return this.uri.toString();
  }

  @Override
  public long getLength() {
    return this.length;
  }

  @Override
  public boolean isEmpty() {
    return this.length <= 0;
  }
  /**
   * 
   * The offset range of tablets <b>MUST NOT</b> be overlapped.
   * 
   * @param t
   * @return If the table paths are not same, return -1.
   */
  @Override
  public int compareTo(PartitionFileFragment t) {
    if (getPath().equals(t.getPath())) {
      long diff = this.getStartKey() - t.getStartKey();
      if (diff < 0) {
        return -1;
      } else if (diff > 0) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return getPath().compareTo(t.getPath());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PartitionFileFragment) {
      PartitionFileFragment t = (PartitionFileFragment) o;
      if (getPath().equals(t.getPath())
          && TUtil.checkEquals(t.getStartKey(), this.getStartKey())
          && TUtil.checkEquals(t.getLength(), this.getLength())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, uri, startOffset, length);
  }
  
  public Object clone() throws CloneNotSupportedException {
    PartitionFileFragment frag = (PartitionFileFragment) super.clone();
    frag.tableName = tableName;
    frag.uri = uri;
    frag.hosts = hosts;

    return frag;
  }

  @Override
  public String toString() {
    return "\"fragment\": {\"id\": \""+ tableName +"\", \"path\": "
    		+getPath() + "\", \"start\": " + this.getStartKey() + ",\"length\": "
        + getLength() + "\", \"partitionKeys\":" + getPartitionKeys() + "}" ;
  }

  public FragmentProto getProto() {
    PartitionFileFragmentProto.Builder builder = PartitionFileFragmentProto.newBuilder();
    builder.setId(this.tableName);
    builder.setStartOffset(this.startOffset);
    builder.setLength(this.length);
    builder.setPath(this.uri.toString());

    if(hosts != null) {
      builder.addAllHosts(TUtil.newList(hosts));
    }
    builder.setPartitionKeys(this.partitionKeys);

    FragmentProto.Builder fragmentBuilder = FragmentProto.newBuilder();
    fragmentBuilder.setId(this.tableName);
    fragmentBuilder.setDataFormat(BuiltinStorages.TEXT);
    fragmentBuilder.setContents(builder.buildPartial().toByteString());
    return fragmentBuilder.build();
  }
}
