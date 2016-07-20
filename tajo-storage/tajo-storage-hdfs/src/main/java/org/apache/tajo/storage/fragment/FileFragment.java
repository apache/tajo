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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.storage.DataLocation;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * Fragment for file systems.
 */
public class FileFragment extends AbstractFileFragment {
  private Integer[] diskIds; // disk volume ids
  private String partitionKeys;

  public FileFragment(String tableName, Path uri, BlockLocation blockLocation)
      throws IOException {
    this(tableName, uri, blockLocation.getOffset(), blockLocation.getLength(), blockLocation.getHosts(), null, null);
  }

  public FileFragment(String tableName, Path uri, BlockLocation blockLocation, String partitionKeys)
    throws IOException {
    this(tableName, uri, blockLocation.getOffset(), blockLocation.getLength(), blockLocation.getHosts(), null,
      partitionKeys);
  }

  public FileFragment(String tableName, Path uri, long start, long length, String[] hosts, Integer[] diskIds,
                      String partitionKeys) {
    super(BuiltinFragmentKinds.FILE, uri.toUri(), tableName, start, start + length, length, hosts);
    this.diskIds = diskIds;
    this.partitionKeys = partitionKeys;
  }

  // Non splittable
  public FileFragment(String tableName, Path uri, long start, long length, String[] hosts) {
    this(tableName, uri, start, length, hosts, null, null);
  }

  public FileFragment(String tableName, Path uri, long start, long length, String[] hosts, String partitionKeys) {
    this(tableName, uri, start, length, hosts, null, partitionKeys);
  }

  public FileFragment(String fragmentId, Path path, long start, long length) {
    this(fragmentId, path, start, length, null, null, null);
  }

  public FileFragment(String fragmentId, Path path, long start, long length, String partitionKeys) {
    this(fragmentId, path, start, length, null, null, partitionKeys);
  }

  /**
   * Get the list of Disk Ids
   * Unknown disk is -1. Others 0 ~ N
   */
  public Integer[] getDiskIds() {
    if (diskIds == null) {
      this.diskIds = new Integer[getHostNames().size()];
      Arrays.fill(this.diskIds, DataLocation.UNKNOWN_VOLUME_ID);
    }
    return diskIds;
  }

  public void setDiskIds(Integer[] diskIds){
    this.diskIds = diskIds;
  }

  public String getPartitionKeys() {
    return this.partitionKeys;
  }

  public void setPartitionKeys(String partitionKeys) {
    this.partitionKeys = partitionKeys;
  }

  public Path getPath() {
    return new Path(uri);
  }

  public void setPath(Path path) {
    this.uri = path.toUri();
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
    return Objects.hashCode(inputSourceId, uri, startKey, endKey, length, diskIds, hostNames, partitionKeys);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    FileFragment frag = (FileFragment) super.clone();
    frag.diskIds = diskIds;
    frag.partitionKeys = partitionKeys;
    return frag;
  }

  @Override
  public String toString() {
    return "\"fragment\": {\"id\": \""+ inputSourceId +"\", \"path\": " + getPath()
      + "\", \"start\": " + this.getStartKey() + ",\"length\": " + getLength()
      + ",\"partitionKeys\": " + getPartitionKeys() + "}" ;
  }
}
