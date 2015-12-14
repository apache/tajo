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
import org.apache.tajo.storage.StorageFragmentProtos.FileFragmentProto;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class FileFragment implements Fragment, Comparable<FileFragment>, Cloneable {
  @Expose private String tableName; // required
  @Expose private Path uri; // required
  @Expose public Long startOffset; // required
  @Expose public Long length; // required

  private String[] hosts; // Datanode hostnames
  @Expose private int[] diskIds;

  public FileFragment(ByteString raw) throws InvalidProtocolBufferException {
    FileFragmentProto.Builder builder = FileFragmentProto.newBuilder();
    builder.mergeFrom(raw);
    builder.build();
    init(builder.build());
  }

  public FileFragment(String tableName, Path uri, BlockLocation blockLocation)
      throws IOException {
    this.set(tableName, uri, blockLocation.getOffset(), blockLocation.getLength(), blockLocation.getHosts(), null);
  }

  public FileFragment(String tableName, Path uri, long start, long length, String[] hosts, int[] diskIds) {
    this.set(tableName, uri, start, length, hosts, diskIds);
  }
  // Non splittable
  public FileFragment(String tableName, Path uri, long start, long length, String[] hosts) {
    this.set(tableName, uri, start, length, hosts, null);
  }

  public FileFragment(String fragmentId, Path path, long start, long length) {
    this.set(fragmentId, path, start, length, null, null);
  }

  public FileFragment(FileFragmentProto proto) {
    init(proto);
  }

  private void init(FileFragmentProto proto) {
    int[] diskIds = new int[proto.getDiskIdsList().size()];
    int i = 0;
    for(Integer eachValue: proto.getDiskIdsList()) {
      diskIds[i++] = eachValue;
    }
    List<String> var = proto.getHostsList();
    this.set(proto.getId(), new Path(proto.getPath()),
        proto.getStartOffset(), proto.getLength(),
            var.toArray(new String[var.size()]),
        diskIds);
  }

  private void set(String tableName, Path path, long start,
      long length, String[] hosts, int[] diskIds) {
    this.tableName = tableName;
    this.uri = path;
    this.startOffset = start;
    this.length = length;
    this.hosts = hosts;
    this.diskIds = diskIds;
  }


  /**
   * Get the list of hosts (hostname) hosting this block
   */
  public String[] getHosts() {
    if (hosts == null) {
      this.hosts = new String[0];
    }
    return hosts;
  }

  /**
   * Get the list of Disk Ids
   * Unknown disk is -1. Others 0 ~ N
   */
  public int[] getDiskIds() {
    if (diskIds == null) {
      this.diskIds = new int[getHosts().length];
      Arrays.fill(this.diskIds, -1);
    }
    return diskIds;
  }

  public void setDiskIds(int[] diskIds){
    this.diskIds = diskIds;
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
  public int compareTo(FileFragment t) {
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
      return -1;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof FileFragment) {
      FileFragment t = (FileFragment) o;
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
    FileFragment frag = (FileFragment) super.clone();
    frag.tableName = tableName;
    frag.uri = uri;
    frag.diskIds = diskIds;
    frag.hosts = hosts;

    return frag;
  }

  @Override
  public String toString() {
    return "\"fragment\": {\"id\": \""+ tableName +"\", \"path\": "
    		+getPath() + "\", \"start\": " + this.getStartKey() + ",\"length\": "
        + getLength() + "}" ;
  }

  public FragmentProto getProto() {
    FileFragmentProto.Builder builder = FileFragmentProto.newBuilder();
    builder.setId(this.tableName);
    builder.setStartOffset(this.startOffset);
    builder.setLength(this.length);
    builder.setPath(this.uri.toString());
    if(diskIds != null) {
      List<Integer> idList = new ArrayList<>();
      for(int eachId: diskIds) {
        idList.add(eachId);
      }
      builder.addAllDiskIds(idList);
    }

    if(hosts != null) {
      builder.addAllHosts(Arrays.asList(hosts));
    }

    FragmentProto.Builder fragmentBuilder = FragmentProto.newBuilder();
    fragmentBuilder.setId(this.tableName);
    fragmentBuilder.setDataFormat(BuiltinStorages.TEXT);
    fragmentBuilder.setContents(builder.buildPartial().toByteString());
    return fragmentBuilder.build();
  }
}
